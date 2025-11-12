# 滑动窗口限流详解

## 什么是滑动窗口限流？

滑动窗口限流是一种更精确的限流算法，相比固定窗口限流，它能更平滑地控制流量，避免在窗口边界处出现流量突增的问题。

## 三种限流算法对比

### 1. 固定窗口限流（Fixed Window）

**原理**：将时间分成固定大小的窗口（如1秒），每个窗口内限制请求数量。

**示例**：限制每秒100个请求

```
时间轴：  |----窗口1----|----窗口2----|----窗口3----|
请求：    100个请求     100个请求     100个请求
```

**问题**：
- 在窗口边界处可能出现流量突增
- 例如：在0.9秒时来了50个请求，在1.1秒时又来了50个请求，虽然都在各自窗口内，但实际在0.1秒内来了100个请求

```
时间：  0.9秒         1.0秒         1.1秒
       |----窗口1----|----窗口2----|
请求：  50个请求      50个请求
        ↑
    实际在0.2秒内来了100个请求！
```

### 2. 滑动窗口限流（Sliding Window）

**原理**：维护一个滑动的时间窗口，统计窗口内所有请求，窗口会随着时间滑动。

**示例**：限制每秒100个请求

```
时间轴：  |----滑动窗口----|
          ↑              ↑
        窗口开始       窗口结束（当前时间）
```

**优点**：
- 更精确，不会出现边界突增
- 流量控制更平滑

**实现方式**：
1. **时间片滑动窗口**：将窗口分成多个时间片，每个时间片记录请求数
2. **Redis ZSet滑动窗口**：使用Redis有序集合，记录每个请求的时间戳

### 3. 令牌桶限流（Token Bucket）

**原理**：以固定速率生成令牌，请求需要消耗令牌。

**优点**：
- 允许突发流量（桶内有令牌时）
- 长期平均速率可控

## 滑动窗口限流实现

### 方案一：Redis ZSet实现（推荐）

使用Redis的有序集合（ZSet）存储请求时间戳，自动清理过期数据。

```go
// 滑动窗口限流Lua脚本
var slidingWindowScript = `
local key = KEYS[1]
local window = tonumber(ARGV[1])  -- 窗口大小（秒）
local limit = tonumber(ARGV[2])   -- 限制数量
local now = tonumber(ARGV[3])      -- 当前时间戳

-- 1. 删除窗口外的数据（清理过期数据）
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

-- 2. 统计窗口内的请求数
local count = redis.call('ZCARD', key)

-- 3. 如果未超过限制，添加当前请求
if count < limit then
    redis.call('ZADD', key, now, now)  -- score和member都使用时间戳
    redis.call('EXPIRE', key, window)  -- 设置过期时间
    return {1, count + 1}  -- 允许，返回当前计数
else
    return {0, count}  -- 拒绝，返回当前计数
end
`
```

**工作原理**：
1. 使用ZSet存储请求时间戳（score = 时间戳，member = 时间戳）
2. 每次请求时：
   - 删除窗口外的数据（`ZREMRANGEBYSCORE`）
   - 统计窗口内的请求数（`ZCARD`）
   - 如果未超限，添加当前请求（`ZADD`）

**示例**：
```
限制：每秒100个请求
窗口：1秒

时间轴：
0.0秒：请求1 → ZSet: [0.0]
0.1秒：请求2 → ZSet: [0.0, 0.1]
0.5秒：请求3 → ZSet: [0.0, 0.1, 0.5]
1.0秒：请求4 → ZSet: [0.0, 0.1, 0.5, 1.0]  (删除0.0，添加1.0)
1.1秒：请求5 → ZSet: [0.1, 0.5, 1.0, 1.1]  (删除0.1，添加1.1)
```

### 方案二：时间片滑动窗口

将窗口分成多个时间片，每个时间片记录请求数。

```go
// 时间片滑动窗口
// 将1秒分成10个时间片，每个时间片0.1秒
var timeSliceScript = `
local key = KEYS[1]
local window = tonumber(ARGV[1])  -- 窗口大小（秒）
local limit = tonumber(ARGV[2])   -- 限制数量
local now = tonumber(ARGV[3])     -- 当前时间戳
local sliceSize = tonumber(ARGV[4]) -- 时间片大小（秒）

-- 计算当前时间片
local currentSlice = math.floor(now / sliceSize)

-- 计算窗口内的所有时间片
local total = 0
for i = 0, window / sliceSize - 1 do
    local sliceKey = key .. ':' .. (currentSlice - i)
    local count = redis.call('GET', sliceKey) or 0
    total = total + tonumber(count)
end

-- 如果未超过限制，增加当前时间片的计数
if total < limit then
    local currentSliceKey = key .. ':' .. currentSlice
    redis.call('INCR', currentSliceKey)
    redis.call('EXPIRE', currentSliceKey, window)
    return {1, total + 1}
else
    return {0, total}
end
`
```

## 代码实现对比

### 当前代码（固定窗口）

```go
func CheckRateLimit(ctx context.Context, goodsId uint64, limitQPS int) (bool, error) {
    key := fmt.Sprintf("goods:rate_limit:%d", goodsId)
    
    // 固定窗口：每个1秒窗口内限制请求数
    current, err := pkg.GetRedisCli().Incr(ctx, key).Result()
    if err != nil {
        return false, err
    }
    
    if current == 1 {
        // 第一次请求时设置1秒过期
        pkg.GetRedisCli().Expire(ctx, key, time.Second)
    }
    
    return current <= int64(limitQPS), nil
}
```

**问题**：
- 在0.9秒时来了50个请求（窗口1）
- 在1.1秒时又来了50个请求（窗口2）
- 虽然都在各自窗口内，但实际在0.2秒内来了100个请求

### 滑动窗口实现

```go
func CheckRateLimitSlidingWindow(ctx context.Context, goodsId uint64, limitQPS int, windowSeconds int) (bool, int64, error) {
    key := fmt.Sprintf("goods:rate_limit:sliding:%d", goodsId)
    now := time.Now().Unix()
    
    // 执行Lua脚本
    result, err := pkg.GetRedisCli().Eval(ctx, slidingWindowScript, 
        []string{key}, windowSeconds, limitQPS, now).Result()
    
    if err != nil {
        return false, 0, err
    }
    
    res := result.([]interface{})
    allowed := res[0].(int64) == 1
    count := res[1].(int64)
    
    return allowed, count, nil
}
```

## 性能对比

| 方案 | 精确度 | 内存占用 | Redis操作 | 适用场景 |
|------|--------|----------|-----------|----------|
| 固定窗口 | 低 | 低 | 1次INCR | 对精确度要求不高 |
| 滑动窗口(ZSet) | 高 | 中 | 1次Lua脚本 | 需要精确控制 |
| 时间片窗口 | 中 | 中 | 多次GET/INCR | 中等精确度需求 |

## 使用建议

1. **秒杀场景**：使用滑动窗口，精确控制QPS
2. **API限流**：固定窗口即可，简单高效
3. **用户行为限流**：滑动窗口，防止恶意刷接口

## 完整示例

见 `rate_limit_sliding_window_example.go`
