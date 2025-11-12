package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// 商品库存服务
type StockService struct {
	redisClient *redis.Client
	db          *gorm.DB
	// 批量更新通道
	updateChan chan *StockUpdate
	// 批量更新协程控制
	wg sync.WaitGroup
}

// 库存更新消息
type StockUpdate struct {
	ProductID int64
	Quantity  int64 // 扣减数量（负数表示扣减）
}

// 商品表结构
type Product struct {
	ID        int64 `gorm:"primaryKey"`
	Stock     int64 `gorm:"column:stock"`
	Version   int64 `gorm:"column:version"` // 乐观锁版本号
	UpdatedAt time.Time
}

// 初始化库存服务
func NewStockService(redisAddr string, dsn string) (*StockService, error) {
	// 初始化 Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	// 初始化数据库
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	// 自动迁移（实际生产环境应该手动管理）
	db.AutoMigrate(&Product{})

	service := &StockService{
		redisClient: rdb,
		db:          db,
		updateChan:  make(chan *StockUpdate, 10000), // 缓冲通道
	}

	// 启动批量更新协程
	service.startBatchUpdater()

	return service, nil
}

// 初始化商品库存到 Redis
func (s *StockService) InitProductStock(ctx context.Context, productID int64, stock int64) error {
	key := fmt.Sprintf("stock:product:%d", productID)
	return s.redisClient.Set(ctx, key, stock, 0).Err()
}

// 秒杀扣减库存（Redis 预扣 + 异步同步数据库）
func (s *StockService) SeckillDeductStock(ctx context.Context, productID int64, quantity int64) (bool, error) {
	key := fmt.Sprintf("stock:product:%d", productID)

	// 方案1: 使用 Lua 脚本保证原子性
	luaScript := `
		local stock = redis.call('get', KEYS[1])
		if stock == false then
			return -1  -- 商品不存在
		end
		stock = tonumber(stock)
		local quantity = tonumber(ARGV[1])
		if stock < quantity then
			return 0  -- 库存不足
		end
		redis.call('decrby', KEYS[1], quantity)
		return 1  -- 扣减成功
	`

	result, err := s.redisClient.Eval(ctx, luaScript, []string{key}, quantity).Int64()
	if err != nil {
		return false, err
	}

	if result == -1 {
		return false, fmt.Errorf("商品不存在")
	}

	if result == 0 {
		return false, fmt.Errorf("库存不足")
	}

	// 扣减成功，异步同步到数据库
	select {
	case s.updateChan <- &StockUpdate{
		ProductID: productID,
		Quantity:  -quantity, // 负数表示扣减
	}:
	default:
		// 通道满了，记录日志或使用备用方案
		log.Printf("警告: 更新通道已满，商品ID: %d", productID)
	}

	return true, nil
}

// 批量更新数据库库存
func (s *StockService) startBatchUpdater() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		ticker := time.NewTicker(100 * time.Millisecond) // 每100ms批量更新一次
		defer ticker.Stop()

		updates := make(map[int64]int64) // productID -> 累计扣减数量

		for {
			select {
			case update, ok := <-s.updateChan:
				if !ok {
					// 通道关闭，执行最后一次批量更新
					s.flushUpdates(updates)
					return
				}
				updates[update.ProductID] += update.Quantity

			case <-ticker.C:
				// 定时批量更新
				if len(updates) > 0 {
					s.flushUpdates(updates)
					updates = make(map[int64]int64) // 重置
				}
			}
		}
	}()
}

// 批量刷新更新到数据库
func (s *StockService) flushUpdates(updates map[int64]int64) {
	if len(updates) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 使用事务批量更新
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for productID, quantity := range updates {
			// 使用乐观锁更新
			result := tx.Model(&Product{}).
				Where("id = ? AND version = ?", productID, gorm.Expr("version")).
				Updates(map[string]interface{}{
					"stock":      gorm.Expr("stock + ?", quantity),
					"version":    gorm.Expr("version + 1"),
					"updated_at": time.Now(),
				})

			if result.Error != nil {
				return result.Error
			}

			// 如果更新行数为0，说明版本号冲突（乐观锁失败）
			if result.RowsAffected == 0 {
				log.Printf("警告: 乐观锁冲突，商品ID: %d", productID)
				// 可以重试或记录到死信队列
			}
		}
		return nil
	})

	if err != nil {
		log.Printf("批量更新库存失败: %v", err)
	} else {
		log.Printf("批量更新库存成功，更新了 %d 个商品", len(updates))
	}
}

// 关闭服务
func (s *StockService) Close() {
	close(s.updateChan)
	s.wg.Wait()
	s.redisClient.Close()
}

func main() {
	// 初始化库存服务
	// 注意：实际使用时需要配置正确的 Redis 和 MySQL 连接信息
	service, err := NewStockService("localhost:6379", "user:password@tcp(localhost:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local")
	if err != nil {
		log.Fatalf("初始化库存服务失败: %v", err)
	}
	defer service.Close()

	// 初始化示例商品库存（实际应该从数据库加载）
	ctx := context.Background()
	productID := int64(1001)
	if err := service.InitProductStock(ctx, productID, 10000); err != nil {
		log.Printf("初始化商品库存失败: %v", err)
	}

	r := gin.Default()

	// 秒杀接口
	r.POST("/seckill/:product_id", func(c *gin.Context) {
		var productID int64
		if _, err := fmt.Sscanf(c.Param("product_id"), "%d", &productID); err != nil {
			c.JSON(400, gin.H{"error": "无效的商品ID"})
			return
		}

		quantity := int64(1) // 默认扣减1件

		success, err := service.SeckillDeductStock(c.Request.Context(), productID, quantity)
		if err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		if !success {
			c.JSON(400, gin.H{"error": "扣减失败"})
			return
		}

		c.JSON(200, gin.H{
			"success": true,
			"message": "扣减成功",
		})
	})

	// 查询库存接口
	r.GET("/stock/:product_id", func(c *gin.Context) {
		var productID int64
		if _, err := fmt.Sscanf(c.Param("product_id"), "%d", &productID); err != nil {
			c.JSON(400, gin.H{"error": "无效的商品ID"})
			return
		}

		key := fmt.Sprintf("stock:product:%d", productID)
		stock, err := service.redisClient.Get(c.Request.Context(), key).Int64()
		if err != nil {
			if err == redis.Nil {
				c.JSON(404, gin.H{"error": "商品不存在"})
				return
			}
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		c.JSON(200, gin.H{
			"product_id": productID,
			"stock":      stock,
		})
	})

	r.Run(":8080")
}
