package main

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// ============================================
// 高并发库存扣减优化策略
// ============================================

// 策略1: Redis Lua 脚本原子扣减（已在 main.go 实现）
// 优点：保证原子性，性能高
// 缺点：需要维护 Redis 和数据库的一致性

// 策略2: Redis 分布式锁 + 数据库更新
// 适用于：需要强一致性的场景
func (s *StockService) DeductStockWithLock(ctx context.Context, productID int64, quantity int64) (bool, error) {
	lockKey := fmt.Sprintf("lock:product:%d", productID)
	stockKey := fmt.Sprintf("stock:product:%d", productID)

	// 获取分布式锁
	lockValue := fmt.Sprintf("%d", time.Now().UnixNano())
	locked, err := s.redisClient.SetNX(ctx, lockKey, lockValue, 10*time.Second).Result()
	if err != nil {
		return false, err
	}
	if !locked {
		return false, fmt.Errorf("获取锁失败")
	}

	// 释放锁
	defer func() {
		// 使用 Lua 脚本确保只释放自己的锁
		unlockScript := `
			if redis.call("get", KEYS[1]) == ARGV[1] then
				return redis.call("del", KEYS[1])
			else
				return 0
			end
		`
		s.redisClient.Eval(ctx, unlockScript, []string{lockKey}, lockValue)
	}()

	// 检查库存
	stock, err := s.redisClient.Get(ctx, stockKey).Int64()
	if err != nil {
		return false, err
	}

	if stock < quantity {
		return false, fmt.Errorf("库存不足")
	}

	// 扣减 Redis 库存
	if err := s.redisClient.DecrBy(ctx, stockKey, quantity).Err(); err != nil {
		return false, err
	}

	// 异步更新数据库
	select {
	case s.updateChan <- &StockUpdate{
		ProductID: productID,
		Quantity:  -quantity,
	}:
	default:
		// 通道满时的处理
	}

	return true, nil
}

// 策略3: 数据库乐观锁（已在 main.go 实现）
// 使用版本号字段，更新时检查版本号是否变化

// 策略4: 数据库悲观锁（SELECT FOR UPDATE）
// 适用于：并发量不是特别高的场景
func (s *StockService) DeductStockWithPessimisticLock(ctx context.Context, productID int64, quantity int64) (bool, error) {
	var product Product

	// 使用事务 + 悲观锁
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// SELECT FOR UPDATE 锁定行
		if err := tx.Set("gorm:query_option", "FOR UPDATE").
			Where("id = ?", productID).
			First(&product).Error; err != nil {
			return err
		}

		if product.Stock < quantity {
			return fmt.Errorf("库存不足")
		}

		// 更新库存
		return tx.Model(&product).
			Update("stock", product.Stock-quantity).Error
	})

	if err != nil {
		return false, err
	}

	return true, nil
}

// 策略5: 分库分表
// 将商品ID进行哈希分片，分散到不同的数据库或表中
func (s *StockService) GetShardKey(productID int64, shardCount int) int {
	return int(productID) % shardCount
}

// 策略6: 库存预热 + 本地缓存
// 在秒杀开始前，将热门商品库存加载到 Redis
// 使用本地缓存（如 freecache）进一步减少 Redis 访问

// 策略7: 限流 + 削峰
// 使用令牌桶或漏桶算法限制请求速率
// 使用消息队列（如 Kafka、RabbitMQ）削峰填谷

// 策略8: 库存预扣 + 定时回滚
// 预扣库存后，如果用户在规定时间内未完成支付，自动回滚库存
func (s *StockService) ReserveStockWithTTL(ctx context.Context, productID int64, quantity int64, ttl time.Duration) (string, error) {
	stockKey := fmt.Sprintf("stock:product:%d", productID)
	reserveKey := fmt.Sprintf("reserve:%s:%d", fmt.Sprintf("%d", time.Now().UnixNano()), productID)

	// 扣减库存
	luaScript := `
		local stock = redis.call('get', KEYS[1])
		if stock == false then
			return -1
		end
		stock = tonumber(stock)
		local quantity = tonumber(ARGV[1])
		if stock < quantity then
			return 0
		end
		redis.call('decrby', KEYS[1], quantity)
		redis.call('setex', KEYS[2], ARGV[2], quantity)
		return 1
	`

	result, err := s.redisClient.Eval(ctx, luaScript,
		[]string{stockKey, reserveKey},
		quantity, int(ttl.Seconds())).Int64()

	if err != nil {
		return "", err
	}

	if result != 1 {
		return "", fmt.Errorf("预扣失败")
	}

	return reserveKey, nil
}

// 确认预扣（支付成功）
func (s *StockService) ConfirmReserve(ctx context.Context, reserveKey string) error {
	// 删除预扣记录，库存已经扣减，不需要回滚
	return s.redisClient.Del(ctx, reserveKey).Err()
}

// 取消预扣（超时或支付失败）
func (s *StockService) CancelReserve(ctx context.Context, reserveKey string) error {
	// 获取预扣数量并回滚库存
	luaScript := `
		local quantity = redis.call('get', KEYS[1])
		if quantity == false then
			return 0  -- 预扣记录不存在或已过期
		end
		quantity = tonumber(quantity)
		redis.call('del', KEYS[1])
		redis.call('incrby', KEYS[2], quantity)
		return 1
	`

	productID := extractProductIDFromReserveKey(reserveKey)
	stockKey := fmt.Sprintf("stock:product:%d", productID)

	_, err := s.redisClient.Eval(ctx, luaScript,
		[]string{reserveKey, stockKey}).Int64()

	return err
}

func extractProductIDFromReserveKey(key string) int64 {
	// 从 reserve:timestamp:productID 格式中提取 productID
	// 实际实现应该解析 key 格式
	var productID int64
	fmt.Sscanf(key, "reserve:%*d:%d", &productID)
	return productID
}
