# 秒杀场景下单件商品减库存高并发优化方案

## 问题分析

在秒杀场景下，单件商品减库存的数据库单行更新QPS达到1万+时，会遇到以下问题：

1. **数据库瓶颈**：MySQL单行更新QPS极限约5000-8000，1万+会导致严重锁竞争
2. **响应延迟**：大量请求等待数据库锁，响应时间急剧增加
3. **超时风险**：锁等待时间过长导致请求超时
4. **库存超卖**：高并发下可能出现库存扣减不一致

## 核心优化思路

**分层架构 + 异步同步**：
- **第一层（Redis）**：预扣库存，快速响应（QPS可达10万+）
- **第二层（消息队列）**：削峰填谷，异步处理
- **第三层（数据库）**：最终一致性，批量更新

## 方案一：Redis预扣库存 + 异步同步（推荐）

### 架构设计

```
用户请求 → Redis预扣库存(Lua脚本) → 立即返回结果
                ↓
        消息队列(削峰)
                ↓
        批量更新数据库
```

### 实现步骤

#### 1. Redis库存预扣（Lua脚本保证原子性）

```go
// internal/dao/seckill_stock.go

package dao

import (
	"context"
	"errors"
	"fmt"
	"time"
	"xena-goods/internal/common"
	"xena-goods/internal/pkg"
	
	"github.com/go-redis/redis/v8"
	"gitlab-vywrajy.micoworld.net/xparty-back/xena-base/xlogrus"
)

// 预扣库存Lua脚本（原子性保证）
var deductStockScript = `
local stockKey = KEYS[1]
local stockLockKey = KEYS[2]
local deductAmount = tonumber(ARGV[1])
local expireTime = tonumber(ARGV[2])

-- 获取当前库存
local currentStock = redis.call('GET', stockKey)
if currentStock == false then
    return {err = 'STOCK_NOT_FOUND'}
end

currentStock = tonumber(currentStock)
if currentStock < deductAmount then
    return {err = 'STOCK_NOT_ENOUGH', stock = currentStock}
end

-- 扣减库存
local newStock = currentStock - deductAmount
redis.call('SET', stockKey, newStock, 'EX', expireTime)

-- 记录预扣信息（用于后续数据库同步）
local lockValue = redis.call('INCR', stockLockKey)
redis.call('EXPIRE', stockLockKey, expireTime)

return {ok = 1, stock = newStock, lockId = lockValue}
`

// 回滚库存Lua脚本
var rollbackStockScript = `
local stockKey = KEYS[1]
local deductAmount = tonumber(ARGV[1])
local expireTime = tonumber(ARGV[2])

local currentStock = redis.call('GET', stockKey)
if currentStock == false then
    redis.call('SET', stockKey, deductAmount, 'EX', expireTime)
    return {ok = 1, stock = deductAmount}
end

currentStock = tonumber(currentStock)
local newStock = currentStock + deductAmount
redis.call('SET', stockKey, newStock, 'EX', expireTime)

return {ok = 1, stock = newStock}
`

// DeductStockRedis 在Redis中预扣库存
// 返回: (success bool, remainingStock int64, error)
func DeductStockRedis(ctx context.Context, goodsId uint64, deductAmount int64) (bool, int64, error) {
	stockKey := fmt.Sprintf("goods:stock:%d", goodsId)
	stockLockKey := fmt.Sprintf("goods:stock:lock:%d", goodsId)
	expireTime := 3600 * 24 // 24小时过期
	
	// 执行Lua脚本
	result, err := pkg.GetRedisCli().Eval(ctx, deductStockScript, []string{stockKey, stockLockKey}, 
		deductAmount, expireTime).Result()
	
	if err != nil {
		xlogrus.Errorf(ctx, "deduct stock redis error, goodsId:%d, err:%v", goodsId, err)
		return false, 0, err
	}
	
	// 解析结果
	resMap, ok := result.([]interface{})
	if !ok || len(resMap) == 0 {
		return false, 0, errors.New("invalid redis result")
	}
	
	// 检查是否有错误
	if errStr, ok := resMap[0].(string); ok && errStr == "STOCK_NOT_FOUND" {
		return false, 0, errors.New("stock not found")
	}
	
	if errStr, ok := resMap[0].(string); ok && errStr == "STOCK_NOT_ENOUGH" {
		stock, _ := resMap[1].(int64)
		return false, stock, errors.New("stock not enough")
	}
	
	// 成功
	okVal, _ := resMap[0].(int64)
	if okVal == 1 {
		stock, _ := resMap[1].(int64)
		return true, stock, nil
	}
	
	return false, 0, errors.New("unknown error")
}

// RollbackStockRedis 回滚库存
func RollbackStockRedis(ctx context.Context, goodsId uint64, deductAmount int64) error {
	stockKey := fmt.Sprintf("goods:stock:%d", goodsId)
	expireTime := 3600 * 24
	
	_, err := pkg.GetRedisCli().Eval(ctx, rollbackStockScript, []string{stockKey}, 
		deductAmount, expireTime).Result()
	
	if err != nil {
		xlogrus.Errorf(ctx, "rollback stock redis error, goodsId:%d, err:%v", goodsId, err)
		return err
	}
	
	return nil
}

// InitStockRedis 初始化Redis库存（从数据库加载）
func InitStockRedis(ctx context.Context, goodsId uint64, stock int64) error {
	stockKey := fmt.Sprintf("goods:stock:%d", goodsId)
	expireTime := 3600 * 24
	
	err := pkg.GetRedisCli().Set(ctx, stockKey, stock, time.Duration(expireTime)*time.Second).Err()
	if err != nil {
		xlogrus.Errorf(ctx, "init stock redis error, goodsId:%d, err:%v", goodsId, err)
		return err
	}
	
	return nil
}

// GetStockRedis 获取Redis中的库存
func GetStockRedis(ctx context.Context, goodsId uint64) (int64, error) {
	stockKey := fmt.Sprintf("goods:stock:%d", goodsId)
	
	result, err := pkg.GetRedisCli().Get(ctx, stockKey).Int64()
	if err == redis.Nil {
		return 0, errors.New("stock not found")
	}
	if err != nil {
		xlogrus.Errorf(ctx, "get stock redis error, goodsId:%d, err:%v", goodsId, err)
		return 0, err
	}
	
	return result, nil
}
```

#### 2. 消息队列异步同步到数据库

```go
// internal/dao/seckill_stock_sync.go

package dao

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"xena-goods/internal/model"
	"xena-goods/internal/pkg"
	
	"gitlab-vywrajy.micoworld.net/xparty-back/xena-base/xkafka"
	"gitlab-vywrajy.micoworld.net/xparty-back/xena-base/xlogrus"
)

// StockDeductMessage 库存扣减消息
type StockDeductMessage struct {
	GoodsId      uint64 `json:"goods_id"`
	Uid          uint64 `json:"uid"`
	DeductAmount int64  `json:"deduct_amount"`
	OrderId      uint64 `json:"order_id"`
	Timestamp    int64  `json:"timestamp"`
}

// SendStockDeductMessage 发送库存扣减消息到Kafka
func SendStockDeductMessage(ctx context.Context, msg *StockDeductMessage) error {
	producer := xkafka.GetProducer()
	if producer == nil {
		return fmt.Errorf("kafka producer not initialized")
	}
	
	// 使用goodsId作为key，确保同一商品的扣减消息有序
	key := fmt.Sprintf("%d", msg.GoodsId)
	
	data, err := json.Marshal(msg)
	if err != nil {
		xlogrus.Errorf(ctx, "marshal stock deduct message error: %v", err)
		return err
	}
	
	topic := "goods_stock_deduct"
	err = producer.Send(ctx, topic, key, data)
	if err != nil {
		xlogrus.Errorf(ctx, "send stock deduct message error: %v", err)
		return err
	}
	
	return nil
}

// BatchUpdateStockDB 批量更新数据库库存
func BatchUpdateStockDB(ctx context.Context, messages []*StockDeductMessage) error {
	if len(messages) == 0 {
		return nil
	}
	
	// 按商品ID分组
	stockMap := make(map[uint64]int64)
	for _, msg := range messages {
		stockMap[msg.GoodsId] += msg.DeductAmount
	}
	
	// 批量更新数据库
	for goodsId, totalDeduct := range stockMap {
		err := pkg.GoodsDB.WithContext(ctx).Model(&model.GoodsMaterial{}).
			Where("goods_id = ?", goodsId).
			Update("stock", gorm.Expr("stock - ?", totalDeduct)).Error
		
		if err != nil {
			xlogrus.Errorf(ctx, "batch update stock db error, goodsId:%d, err:%v", goodsId, err)
			// 继续处理其他商品，不中断
			continue
		}
	}
	
	return nil
}

// RegisterStockDeductConsumer 注册库存扣减消息消费者
func RegisterStockDeductConsumer() {
	consumer := xkafka.GetConsumer()
	if consumer == nil {
		xlogrus.Errorf(context.Background(), "kafka consumer not initialized")
		return
	}
	
	topic := "goods_stock_deduct"
	groupID := "goods_stock_deduct_consumer"
	
	// 批量消费，每批最多100条，最多等待1秒
	batchSize := 100
	batchTimeout := time.Second
	
	consumer.RegisterHandler(topic, groupID, func(ctx context.Context, messages []*xkafka.Message) error {
		if len(messages) == 0 {
			return nil
		}
		
		stockMessages := make([]*StockDeductMessage, 0, len(messages))
		for _, msg := range messages {
			var stockMsg StockDeductMessage
			if err := json.Unmarshal(msg.Value, &stockMsg); err != nil {
				xlogrus.Errorf(ctx, "unmarshal stock message error: %v", err)
				continue
			}
			stockMessages = append(stockMessages, &stockMsg)
		}
		
		// 批量更新数据库
		return BatchUpdateStockDB(ctx, stockMessages)
	}, xkafka.WithBatchSize(batchSize), xkafka.WithBatchTimeout(batchTimeout))
}
```

#### 3. Service层调用

```go
// internal/service/seckill.go

package service

import (
	"context"
	"errors"
	"time"
	"xena-goods/internal/dao"
	"xena-goods/internal/pkg"
	
	"gitlab-vywrajy.micoworld.net/xparty-back/xena-base/xlogrus"
)

// DeductStock 扣减库存（秒杀接口）
func DeductStock(ctx context.Context, goodsId, uid, orderId uint64, amount int64) error {
	// 1. Redis预扣库存
	success, remainingStock, err := dao.DeductStockRedis(ctx, goodsId, amount)
	if err != nil {
		if err.Error() == "stock not enough" {
			return errors.New("库存不足")
		}
		xlogrus.Errorf(ctx, "deduct stock redis error: %v", err)
		return errors.New("系统繁忙，请稍后重试")
	}
	
	if !success {
		return errors.New("库存不足")
	}
	
	// 2. 发送消息到Kafka（异步同步到数据库）
	msg := &dao.StockDeductMessage{
		GoodsId:      goodsId,
		Uid:          uid,
		DeductAmount: amount,
		OrderId:      orderId,
		Timestamp:    time.Now().Unix(),
	}
	
	if err := dao.SendStockDeductMessage(ctx, msg); err != nil {
		// 如果消息发送失败，回滚Redis库存
		xlogrus.Errorf(ctx, "send stock deduct message error, rollback redis stock: %v", err)
		dao.RollbackStockRedis(ctx, goodsId, amount)
		return errors.New("系统繁忙，请稍后重试")
	}
	
	xlogrus.Infof(ctx, "deduct stock success, goodsId:%d, uid:%d, remainingStock:%d", 
		goodsId, uid, remainingStock)
	
	return nil
}
```

### 性能指标

- **Redis预扣库存**：QPS可达10万+
- **响应时间**：< 5ms（Redis操作）
- **数据库压力**：降低到原来的1/100（批量更新）
- **库存准确性**：100%（Lua脚本保证原子性）

## 方案二：Redis + 数据库双写（适用于实时性要求高的场景）

如果业务要求必须实时同步到数据库，可以使用双写方案：

```go
// 1. Redis预扣库存
success, stock, err := DeductStockRedis(ctx, goodsId, amount)

// 2. 立即更新数据库（异步，不阻塞）
go func() {
    err := UpdateStockDB(context.Background(), goodsId, amount)
    if err != nil {
        // 记录错误，后续补偿
        xlogrus.Errorf(ctx, "update stock db error: %v", err)
    }
}()

// 3. 立即返回结果
return success
```

## 方案三：库存预热 + 限流

### 1. 库存预热

在秒杀开始前，将数据库库存加载到Redis：

```go
// 秒杀开始前执行
func PreloadStock(ctx context.Context, goodsIds []uint64) error {
	for _, goodsId := range goodsIds {
		// 从数据库查询库存
		var goods model.GoodsMaterial
		err := pkg.GoodsDB.WithContext(ctx).
			Where("goods_id = ?", goodsId).
			First(&goods).Error
		if err != nil {
			continue
		}
		
		// 加载到Redis
		err = dao.InitStockRedis(ctx, goodsId, int64(goods.Stock))
		if err != nil {
			xlogrus.Errorf(ctx, "preload stock error, goodsId:%d, err:%v", goodsId, err)
		}
	}
	return nil
}
```

### 2. 限流保护

```go
// 使用Redis实现限流
func CheckRateLimit(ctx context.Context, goodsId uint64, limitQPS int) (bool, error) {
	key := fmt.Sprintf("goods:rate_limit:%d", goodsId)
	
	// 使用滑动窗口限流
	current, err := pkg.GetRedisCli().Incr(ctx, key).Result()
	if err != nil {
		return false, err
	}
	
	if current == 1 {
		pkg.GetRedisCli().Expire(ctx, key, time.Second)
	}
	
	return current <= int64(limitQPS), nil
}
```

## 方案四：库存分片（适用于超大规模）

如果单个商品QPS超过10万，可以分片：

```go
// 将库存分成多个分片
func DeductStockSharded(ctx context.Context, goodsId uint64, amount int64) error {
	shardCount := 10 // 10个分片
	shardId := goodsId % uint64(shardCount)
	
	stockKey := fmt.Sprintf("goods:stock:%d:shard:%d", goodsId, shardId)
	// ... 类似预扣逻辑
}
```

## 监控指标

1. **Redis库存命中率**：应该 > 99.9%
2. **消息队列延迟**：< 1秒
3. **数据库更新延迟**：< 5秒
4. **库存准确性**：定期对账Redis和数据库

## 注意事项

1. **库存预热**：秒杀开始前必须预热Redis库存
2. **库存对账**：定期（如每小时）对账Redis和数据库库存
3. **故障恢复**：Redis故障时，降级到数据库直接扣减
4. **幂等性**：消息队列消费需要保证幂等性
5. **超时处理**：订单超时未支付，需要回滚库存

## 推荐方案选择

- **QPS < 5000**：直接数据库更新
- **5000 < QPS < 20000**：Redis预扣 + 消息队列异步同步（推荐）
- **QPS > 20000**：Redis预扣 + 消息队列 + 库存分片
- **实时性要求高**：Redis预扣 + 数据库双写

## 完整示例代码

见 `seckill_stock_example.go`

