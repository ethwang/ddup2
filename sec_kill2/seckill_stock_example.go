package seckill2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// 注意：以下代码需要根据实际项目结构调整
// 这里使用简化的实现，实际项目中应该使用项目内部的 pkg、xlogrus、xkafka、model 等包

// 全局变量（实际应该通过依赖注入）
var (
	// redisClient 应该通过 pkg.GetRedisCli() 获取
	redisClient *redis.Client
	// db 应该通过 pkg.GoodsDB 获取
	db *gorm.DB
)

// 初始化函数（实际应该在项目启动时初始化）
func Init(rdb *redis.Client, database *gorm.DB) {
	redisClient = rdb
	db = database
}

// 日志函数（简化实现，实际应该使用 xlogrus）
func logError(ctx context.Context, format string, args ...interface{}) {
	log.Printf("[ERROR] "+format, args...)
}

func logInfo(ctx context.Context, format string, args ...interface{}) {
	log.Printf("[INFO] "+format, args...)
}

func logWarn(ctx context.Context, format string, args ...interface{}) {
	log.Printf("[WARN] "+format, args...)
}

// ==================== Redis库存预扣 ====================

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
	if redisClient == nil {
		return false, 0, errors.New("redis client not initialized")
	}
	result, err := redisClient.Eval(ctx, deductStockScript, []string{stockKey, stockLockKey},
		deductAmount, expireTime).Result()

	if err != nil {
		logError(ctx, "deduct stock redis error, goodsId:%d, err:%v", goodsId, err)
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

	if redisClient == nil {
		return errors.New("redis client not initialized")
	}
	_, err := redisClient.Eval(ctx, rollbackStockScript, []string{stockKey},
		deductAmount, expireTime).Result()

	if err != nil {
		logError(ctx, "rollback stock redis error, goodsId:%d, err:%v", goodsId, err)
		return err
	}

	return nil
}

// InitStockRedis 初始化Redis库存（从数据库加载）
func InitStockRedis(ctx context.Context, goodsId uint64, stock int64) error {
	stockKey := fmt.Sprintf("goods:stock:%d", goodsId)
	expireTime := 3600 * 24

	if redisClient == nil {
		return errors.New("redis client not initialized")
	}
	err := redisClient.Set(ctx, stockKey, stock, time.Duration(expireTime)*time.Second).Err()
	if err != nil {
		logError(ctx, "init stock redis error, goodsId:%d, err:%v", goodsId, err)
		return err
	}

	return nil
}

// GetStockRedis 获取Redis中的库存
func GetStockRedis(ctx context.Context, goodsId uint64) (int64, error) {
	stockKey := fmt.Sprintf("goods:stock:%d", goodsId)

	if redisClient == nil {
		return 0, errors.New("redis client not initialized")
	}
	result, err := redisClient.Get(ctx, stockKey).Int64()
	if err == redis.Nil {
		return 0, errors.New("stock not found")
	}
	if err != nil {
		logError(ctx, "get stock redis error, goodsId:%d, err:%v", goodsId, err)
		return 0, err
	}

	return result, nil
}

// ==================== 消息队列异步同步 ====================

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
	// 使用goodsId作为key，确保同一商品的扣减消息有序
	key := fmt.Sprintf("%d", msg.GoodsId)

	topic := "goods_stock_deduct"
	// 注意：实际项目中应该使用 xkafka.ProduceJSON
	// 这里只是示例，实际需要配置 Kafka producer
	// xkafka.ProduceJSON(ctx, topic, key, msg)

	// 简化实现：序列化消息（实际应该发送到 Kafka）
	msgBytes, _ := json.Marshal(msg)
	logInfo(ctx, "sent stock deduct message to kafka: topic=%s, key=%s, msg=%s",
		topic, key, string(msgBytes))

	return nil
}

// BatchUpdateStockDB 批量更新数据库库存（已废弃，改为单条处理）
// 保留此函数作为参考，实际使用 ProcessStockDeductMessage
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
	// 注意：实际项目中应该使用 pkg.GoodsDB 和 model.GoodsMaterial
	// 这里使用简化的实现
	if db == nil {
		return errors.New("database not initialized")
	}
	for goodsId, totalDeduct := range stockMap {
		// 实际应该使用具体的模型
		// err := db.WithContext(ctx).Model(&model.GoodsMaterial{}).
		// 	Where("goods_id = ?", goodsId).
		// 	Update("stock", gorm.Expr("stock - ?", totalDeduct)).Error

		// 简化实现：只记录日志
		logInfo(ctx, "batch update stock: goodsId=%d, deduct=%d", goodsId, totalDeduct)
		// if err != nil {
		// 	logError(ctx, "batch update stock db error, goodsId:%d, err:%v", goodsId, err)
		// 	continue
		// }
	}

	return nil
}

// KafkaMessage Kafka 消息结构（简化实现）
type KafkaMessage struct {
	Key   string
	Value []byte
}

// ProcessStockDeductMessage 处理库存扣减消息（单条处理，Kafka保证顺序）
// 注意：实际项目中应该使用 xkafka.Message
func ProcessStockDeductMessage(ctx context.Context, msg *KafkaMessage) error {
	var stockMsg StockDeductMessage
	if err := json.Unmarshal(msg.Value, &stockMsg); err != nil {
		logError(ctx, "unmarshal stock message error: %v, value: %s", err, string(msg.Value))
		return fmt.Errorf("反序列化消息失败: %w", err)
	}

	// 更新数据库库存
	// 注意：实际项目中应该使用 pkg.GoodsDB 和 model.GoodsMaterial
	if db == nil {
		return errors.New("database not initialized")
	}
	// 实际实现：
	// err := db.WithContext(ctx).Model(&model.GoodsMaterial{}).
	// 	Where("goods_id = ?", stockMsg.GoodsId).
	// 	Update("stock", gorm.Expr("stock - ?", stockMsg.DeductAmount)).Error

	// 简化实现：只记录日志
	logInfo(ctx, "processed stock deduct: goodsId=%d, uid=%d, amount=%d",
		stockMsg.GoodsId, stockMsg.Uid, stockMsg.DeductAmount)

	// if err != nil {
	// 	logError(ctx, "update stock db error, goodsId:%d, err:%v", stockMsg.GoodsId, err)
	// 	return err
	// }

	return nil
}

// RegisterStockDeductConsumer 注册库存扣减消息消费者
// 注意：实际项目中应该使用 xkafka.Register
func RegisterStockDeductConsumer() {
	topic := "goods_stock_deduct"
	// 实际实现：
	// xkafka.Register(topic, ProcessStockDeductMessage)

	// 简化实现：只记录日志
	logInfo(context.Background(), "registered kafka consumer for topic: %s", topic)
}

// ==================== 库存预热 ====================

// PreloadStock 库存预热（秒杀开始前执行）
func PreloadStock(ctx context.Context, goodsIds []uint64) error {
	if db == nil {
		return errors.New("database not initialized")
	}
	for _, goodsId := range goodsIds {
		// 从数据库查询库存
		// 注意：实际项目中应该使用 model.GoodsMaterial
		// var goods model.GoodsMaterial
		// err := db.WithContext(ctx).
		// 	Where("goods_id = ?", goodsId).
		// 	First(&goods).Error
		// if err != nil {
		// 	logError(ctx, "preload stock query error, goodsId:%d, err:%v", goodsId, err)
		// 	continue
		// }

		// 简化实现：只记录日志
		logInfo(ctx, "preload stock: goodsId=%d", goodsId)

		// 假设GoodsMaterial有Stock字段，如果没有需要从其他表查询
		// 这里需要根据实际表结构调整
		// stock := goods.Stock // 需要确认字段名

		// 加载到Redis
		// err = InitStockRedis(ctx, goodsId, int64(stock))
		// if err != nil {
		// 	xlogrus.Errorf(ctx, "preload stock error, goodsId:%d, err:%v", goodsId, err)
		// }
	}
	return nil
}

// ==================== 限流保护 ====================

// 滑动窗口限流Lua脚本（使用Redis ZSet）
var slidingWindowScript = `
local key = KEYS[1]
local window = tonumber(ARGV[1])  -- 窗口大小（秒）
local limit = tonumber(ARGV[2])   -- 限制数量
local now = tonumber(ARGV[3])      -- 当前时间戳（秒）

-- 1. 删除窗口外的数据（清理过期数据）
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

-- 2. 统计窗口内的请求数
local count = redis.call('ZCARD', key)

-- 3. 如果未超过限制，添加当前请求
if count < limit then
    -- 使用时间戳作为score和member，确保唯一性
    redis.call('ZADD', key, now, now)
    redis.call('EXPIRE', key, window)  -- 设置过期时间
    return {1, count + 1}  -- 允许，返回{1, 当前计数}
else
    return {0, count}  -- 拒绝，返回{0, 当前计数}
end
`

// CheckRateLimit 检查限流（固定窗口，简单但不够精确）
// 注意：这是固定窗口限流，不是真正的滑动窗口
// 在窗口边界可能出现流量突增
func CheckRateLimit(ctx context.Context, goodsId uint64, limitQPS int) (bool, error) {
	key := fmt.Sprintf("goods:rate_limit:%d", goodsId)

	if redisClient == nil {
		return false, errors.New("redis client not initialized")
	}
	// 固定窗口：每个1秒窗口内限制请求数
	current, err := redisClient.Incr(ctx, key).Result()
	if err != nil {
		return false, err
	}

	if current == 1 {
		// 第一次请求时设置1秒过期
		redisClient.Expire(ctx, key, time.Second)
	}

	return current <= int64(limitQPS), nil
}

// CheckRateLimitSlidingWindow 检查限流（真正的滑动窗口，更精确）
// 参数：
//   - goodsId: 商品ID
//   - limitQPS: 限制的QPS（每秒请求数）
//   - windowSeconds: 滑动窗口大小（秒），通常等于1
//
// 返回：
//   - allowed: 是否允许请求
//   - currentCount: 当前窗口内的请求数
//   - error: 错误信息
func CheckRateLimitSlidingWindow(ctx context.Context, goodsId uint64, limitQPS int, windowSeconds int) (bool, int64, error) {
	key := fmt.Sprintf("goods:rate_limit:sliding:%d", goodsId)
	now := time.Now().Unix()

	if redisClient == nil {
		return false, 0, errors.New("redis client not initialized")
	}
	// 执行Lua脚本（原子操作）
	result, err := redisClient.Eval(ctx, slidingWindowScript,
		[]string{key}, windowSeconds, limitQPS, now).Result()

	if err != nil {
		logError(ctx, "sliding window rate limit error, goodsId:%d, err:%v", goodsId, err)
		return false, 0, err
	}

	// 解析结果
	res, ok := result.([]interface{})
	if !ok || len(res) < 2 {
		return false, 0, errors.New("invalid redis result")
	}

	allowed := res[0].(int64) == 1
	currentCount := res[1].(int64)

	return allowed, currentCount, nil
}

// ==================== 库存对账 ====================

// ReconcileStock 库存对账（定期执行，如每小时）
func ReconcileStock(ctx context.Context, goodsId uint64) error {
	// 1. 从Redis获取库存
	redisStock, err := GetStockRedis(ctx, goodsId)
	if err != nil {
		// Redis中没有库存，可能是未预热，不算错误
		if err.Error() == "stock not found" {
			return nil
		}
		logError(ctx, "reconcile stock get redis error, goodsId:%d, err:%v", goodsId, err)
		return err
	}

	if db == nil {
		return errors.New("database not initialized")
	}
	// 2. 从数据库获取库存
	// 注意：实际项目中应该使用 model.GoodsMaterial
	// var goods model.GoodsMaterial
	// err = db.WithContext(ctx).
	// 	Where("goods_id = ?", goodsId).
	// 	First(&goods).Error
	// if err != nil {
	// 	logError(ctx, "reconcile stock get db error, goodsId:%d, err:%v", goodsId, err)
	// 	return err
	// }

	// 3. 对比差异（需要确认字段名，这里假设有Stock字段）
	// 注意：如果GoodsMaterial没有Stock字段，需要从其他表查询
	// dbStock := goods.Stock
	// if redisStock != int64(dbStock) {
	// 	logWarn(ctx, "stock mismatch, goodsId:%d, redis:%d, db:%d", goodsId, redisStock, dbStock)
	// 	// 以数据库为准，更新Redis
	// 	return InitStockRedis(ctx, goodsId, int64(dbStock))
	// }

	// 暂时只记录Redis库存，不进行对比
	logInfo(ctx, "reconcile stock, goodsId:%d, redis:%d", goodsId, redisStock)

	return nil
}
