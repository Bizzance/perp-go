// Package cache 封装Redis客户端——MVP阶段只用来存标记价格(跟Java版MarkPriceService
// 用Redis存contract:mark:<symbol>同一个用途)，后续阶段(幂等去重/短期token)会复用同一个连接。
package cache

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
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

// GetMarkPrice 返回("", nil)表示这个symbol还没有任何标记价格(从没成交过)——调用方要按
// Java版MarkPriceService同样的语义处理："没有标记价格"是一个合法状态，不是错误
func (c *Cache) GetMarkPrice(ctx context.Context, symbol string) (string, error) {
	v, err := c.rdb.Get(ctx, markPriceKey(symbol)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return v, err
}
