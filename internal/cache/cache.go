package cache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

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

func (c *Cache) SetMarkPrice(ctx context.Context, symbol, price string) error {
	return c.rdb.Set(ctx, markPriceKey(symbol), price, 0).Err()
}

// GetMarkPrice 返回("", nil)表示这个symbol还没有任何标记价格(从没成交过)——调用方要把
// "没有标记价格"当一个合法状态处理，不是错误
func (c *Cache) GetMarkPrice(ctx context.Context, symbol string) (string, error) {
	v, err := c.rdb.Get(ctx, markPriceKey(symbol)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return v, err
}

func indexPriceKey(symbol string) string { return "perpgo:index:" + symbol }

// SetIndexPrice 指数价格——反映外部真实市场(未来接入币安行情)的参考价，跟"标记价格"(反映
// 我们自己盘口的最新成交)是两个独立概念，资金费率就是两者的溢价，见FundingService
func (c *Cache) SetIndexPrice(ctx context.Context, symbol, price string) error {
	return c.rdb.Set(ctx, indexPriceKey(symbol), price, 0).Err()
}

// GetIndexPrice 返回("", nil)表示这个symbol还没有任何外部行情源喂过指数价格
func (c *Cache) GetIndexPrice(ctx context.Context, symbol string) (string, error) {
	v, err := c.rdb.Get(ctx, indexPriceKey(symbol)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return v, err
}

func fundingAccumKey(symbol string) string { return "perpgo:funding:accum:" + symbol }

// AccumulateFundingSample 把这一次采样的溢价率累加进这个symbol当前资金费率周期的累加器——
// sum/count压缩存成一个"sum|count"字符串，省一次round trip。只有FundingService.SampleOnce
// 单个goroutine会写这个key，不存在并发覆盖问题，不需要用Lua脚本做原子读改写
func (c *Cache) AccumulateFundingSample(ctx context.Context, symbol string, premium decimal.Decimal) error {
	sum, count, err := c.GetFundingAccumulator(ctx, symbol)
	if err != nil {
		return err
	}
	newValue := sum.Add(premium).String() + "|" + strconv.FormatInt(count+1, 10)
	return c.rdb.Set(ctx, fundingAccumKey(symbol), newValue, 0).Err()
}

// GetFundingAccumulator 返回这个symbol当前周期已经累计的溢价率之和与采样次数，从没采样过
// 返回(0, 0)——FundingService结算时用sum/count算TWAP均值
func (c *Cache) GetFundingAccumulator(ctx context.Context, symbol string) (decimal.Decimal, int64, error) {
	v, err := c.rdb.Get(ctx, fundingAccumKey(symbol)).Result()
	if errors.Is(err, redis.Nil) {
		return decimal.Zero, 0, nil
	}
	if err != nil {
		return decimal.Zero, 0, err
	}
	sumStr, countStr, ok := strings.Cut(v, "|")
	if !ok {
		return decimal.Zero, 0, nil
	}
	sum, err := decimal.NewFromString(sumStr)
	if err != nil {
		return decimal.Zero, 0, err
	}
	count, err := strconv.ParseInt(countStr, 10, 64)
	if err != nil {
		return decimal.Zero, 0, err
	}
	return sum, count, nil
}

// ResetFundingAccumulator 一个周期结算完之后清空累加器，开始下一周期的采样
func (c *Cache) ResetFundingAccumulator(ctx context.Context, symbol string) error {
	return c.rdb.Del(ctx, fundingAccumKey(symbol)).Err()
}
