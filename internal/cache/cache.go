package cache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
)

type Cache struct {
	rdb *redis.Client
}

func Connect(addr, password string) (*Cache, error) {
	rdb := redis.NewClient(&redis.Options{Addr: addr, Password: password, DB: 0})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("connect redis: %w", err)
	}
	return &Cache{rdb: rdb}, nil
}

func markPriceKey(symbol string) string { return "perpgo:mark:" + symbol }

// 标记价格由MarkPriceService算好后写进来，其它地方不要直接写
func (c *Cache) SetMarkPrice(ctx context.Context, symbol, price string) error {
	return c.rdb.Set(ctx, markPriceKey(symbol), price, 0).Err()
}

// 返回("", nil)表示这个symbol还没有任何标记价格——调用方要把"没有标记价格"当一个合法状态处理，不是错误
func (c *Cache) GetMarkPrice(ctx context.Context, symbol string) (string, error) {
	v, err := c.rdb.Get(ctx, markPriceKey(symbol)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return v, err
}

func indexPriceKey(symbol string) string   { return "perpgo:index:" + symbol }
func indexPriceTsKey(symbol string) string { return "perpgo:index_ts:" + symbol }

// 指数价格——反映外部真实市场(币安等)的参考价，是标记价格的锚：标记价格由指数价、我们盘口相对
// 指数价的基差、最新成交价三者取中位数得出，见MarkPriceService。价格和它的更新时间(毫秒)用MSET
// 原子地一起写，读的一方才不会读到"新价格配旧时间"的组合——时间是判断喂价有没有断的依据
func (c *Cache) SetIndexPrice(ctx context.Context, symbol, price string, tsMs int64) error {
	return c.rdb.MSet(ctx, indexPriceKey(symbol), price, indexPriceTsKey(symbol), tsMs).Err()
}

// 返回("", 0, nil)表示这个symbol还没有任何外部行情源喂过指数价格。价格存在但没有时间戳(旧版本
// 写入的数据)时时间戳返回0，调用方会当成陈旧处理，等下一次喂价就恢复
func (c *Cache) GetIndexPrice(ctx context.Context, symbol string) (price string, tsMs int64, err error) {
	vals, err := c.rdb.MGet(ctx, indexPriceKey(symbol), indexPriceTsKey(symbol)).Result()
	if err != nil {
		return "", 0, err
	}
	if v, ok := vals[0].(string); ok {
		price = v
	}
	if v, ok := vals[1].(string); ok {
		tsMs, _ = strconv.ParseInt(v, 10, 64)
	}
	return price, tsMs, nil
}

func lastTradeKey(symbol string) string { return "perpgo:last:" + symbol }

// 最新成交价单独存一份：标记价格不再等于它，但重算标记价格(引擎重启后、喂价变化时)还要用它
func (c *Cache) SetLastTradePrice(ctx context.Context, symbol, price string) error {
	return c.rdb.Set(ctx, lastTradeKey(symbol), price, 0).Err()
}

// 返回("", nil)表示这个symbol还没有成交过
func (c *Cache) GetLastTradePrice(ctx context.Context, symbol string) (string, error) {
	v, err := c.rdb.Get(ctx, lastTradeKey(symbol)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return v, err
}

func fundingAccumSumKey(symbol string) string   { return "perpgo:funding:accum:sum:" + symbol }
func fundingAccumCountKey(symbol string) string { return "perpgo:funding:accum:count:" + symbol }

// accumulateFundingSampleScript 原子地把sum和count一起累加——不能拆成两条独立的Redis命令
// 分别执行(哪怕各自都是原子的INCRBYFLOAT/INCR)，那样两条命令之间有一个窗口，某个读取方
// (FundingService结算读累加器算TWAP均值)可能读到"sum已经加了这次采样、count还没加"的
// 中间状态，多算出一次样本不存在的贡献。Lua脚本在Redis里整体原子执行，两条命令要么都生效
// 要么都还没生效，读取方不会看到半更新的中间态
var accumulateFundingSampleScript = redis.NewScript(`
redis.call("INCRBYFLOAT", KEYS[1], ARGV[1])
redis.call("INCR", KEYS[2])
return 1
`)

// 把这一次采样的溢价率累加进这个symbol当前资金费率周期的累加器。
// sum/count分别存成两个独立key，用Redis原生的INCRBYFLOAT/INCR(包在一个Lua脚本里原子执行，
// 见上面)做累加——不是"GET当前值→在Go里算新值→SET回去"这种读改写。engine分片部署下
// (docs/engine-sharding.md)多个实例各自独立跑自己的采样ticker，GET-改-SET之间的窗口会
// 让后写的实例把先写的实例那次采样静默覆盖掉，不是"重复采样只是提高密度、不影响均值"那么
// 无害——那个结论只在累加操作本身是原子的前提下才成立，早期实现这里的注释就是被这个
// 想当然的假设误导的，实际上会丢样本。premium先转成float64存进Redis的浮点累加器——
// 溢价率是统计意义上的近似量(结算时还要clamp到FundingRateCap)，不是像账户余额那样必须
// 保持decimal库的精确精度，float64/long double的精度对这个量级的值绰绰有余
func (c *Cache) AccumulateFundingSample(ctx context.Context, symbol string, premium decimal.Decimal) error {
	f, _ := premium.Float64()
	return accumulateFundingSampleScript.Run(ctx, c.rdb,
		[]string{fundingAccumSumKey(symbol), fundingAccumCountKey(symbol)}, f).Err()
}

// 返回这个symbol当前周期已经累计的溢价率之和与采样次数，从没采样过
// 返回(0, 0)——FundingService结算时用sum/count算TWAP均值
func (c *Cache) GetFundingAccumulator(ctx context.Context, symbol string) (decimal.Decimal, int64, error) {
	sumStr, err := c.rdb.Get(ctx, fundingAccumSumKey(symbol)).Result()
	if errors.Is(err, redis.Nil) {
		return decimal.Zero, 0, nil
	}
	if err != nil {
		return decimal.Zero, 0, err
	}
	sum, err := decimal.NewFromString(sumStr)
	if err != nil {
		return decimal.Zero, 0, err
	}
	countStr, err := c.rdb.Get(ctx, fundingAccumCountKey(symbol)).Result()
	if errors.Is(err, redis.Nil) {
		return sum, 0, nil
	}
	if err != nil {
		return decimal.Zero, 0, err
	}
	count, err := strconv.ParseInt(countStr, 10, 64)
	if err != nil {
		return decimal.Zero, 0, err
	}
	return sum, count, nil
}

// 一个周期结算完之后清空累加器，开始下一周期的采样
func (c *Cache) ResetFundingAccumulator(ctx context.Context, symbol string) error {
	return c.rdb.Del(ctx, fundingAccumSumKey(symbol), fundingAccumCountKey(symbol)).Err()
}

// /Subscribe：contract-engine往外发布实时事件(深度/成交/K线/标记价格/账户快照)，
// contract-api的WS网关订阅转发给客户端——两个进程用Redis Pub/Sub解耦，contract-engine
// 只管发布，不知道、也不需要知道有没有人在订阅，见docs/websocket.md
func (c *Cache) Publish(ctx context.Context, channel, payload string) error {
	return c.rdb.Publish(ctx, channel, payload).Err()
}

// 返回的*redis.PubSub由调用方负责关闭(defer Close())，不在这里做懒订阅/
// 引用计数管理——那是internal/ws.Hub的职责，这一层只是对go-redis客户端的薄封装
func (c *Cache) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return c.rdb.Subscribe(ctx, channels...)
}

// 记录一个一次性的随机串，返回true表示这是第一次出现，false表示已经用过。鉴权用它防重放：
// 同一个请求在时间窗内被截获后重放，nonce重复就会被拒绝。ttl必须覆盖时间戳的整个有效期，
// 过期太早会让已经用过的nonce又能被重放
func (c *Cache) ClaimNonce(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return c.rdb.SetNX(ctx, key, "1", ttl).Result()
}

// 基于SETNX的简单分布式锁原语：key不存在才能设置成功(ok=true)，同时给一个
// TTL防止持锁方崩溃/异常导致永久死锁。token是调用方生成的随机值，配合ReleaseLock按token
// 校验一致才删——避免"锁已经过期自动释放、被别人抢到，自己却把别人的锁误删"。上层封装见
// internal/service.LockService
func (c *Cache) AcquireLock(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	return c.rdb.SetNX(ctx, key, token, ttl).Result()
}

var releaseLockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
else
	return 0
end
`)

// 只有key当前的值还等于token(还是自己持有的那把锁，没有过期后被别人抢走)才会
// 真的删除——用Lua脚本保证"比较+删除"这两步原子完成，不是先GET再判断再DEL(那样中间有
// 竞态窗口)
func (c *Cache) ReleaseLock(ctx context.Context, key, token string) error {
	return releaseLockScript.Run(ctx, c.rdb, []string{key}, token).Err()
}
