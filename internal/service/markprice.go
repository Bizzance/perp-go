package service

import (
	"context"

	"github.com/shopspring/decimal"

	"perp-go/internal/cache"
)

type MarkPriceService struct {
	cache *cache.Cache
}

func NewMarkPriceService(c *cache.Cache) *MarkPriceService {
	return &MarkPriceService{cache: c}
}

// 返回(zero, false)表示这个symbol还没有任何标记价格(从没成交过)，调用方要按"没有价格就跳过这一轮判断"处理，不能当0价格用
func (s *MarkPriceService) Get(ctx context.Context, symbol string) (decimal.Decimal, bool) {
	v, err := s.cache.GetMarkPrice(ctx, symbol)
	if err != nil || v == "" {
		return decimal.Zero, false
	}
	d, err := decimal.NewFromString(v)
	if err != nil {
		return decimal.Zero, false
	}
	return d, true
}

func (s *MarkPriceService) UpdateFromTrade(ctx context.Context, symbol string, price decimal.Decimal) error {
	return s.cache.SetMarkPrice(ctx, symbol, price.String())
}

// 外部行情源推送这个symbol的指数价格
func (s *MarkPriceService) SetIndexPrice(ctx context.Context, symbol string, price decimal.Decimal) error {
	return s.cache.SetIndexPrice(ctx, symbol, price.String())
}

// 返回(zero, false)表示这个symbol还没有任何外部行情源喂过指数价格
func (s *MarkPriceService) GetIndexPrice(ctx context.Context, symbol string) (decimal.Decimal, bool) {
	v, err := s.cache.GetIndexPrice(ctx, symbol)
	if err != nil || v == "" {
		return decimal.Zero, false
	}
	d, err := decimal.NewFromString(v)
	if err != nil {
		return decimal.Zero, false
	}
	return d, true
}
