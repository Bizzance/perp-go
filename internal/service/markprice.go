// 标记价格：MVP阶段用"最新成交价"本身当标记价格(EMA alpha=1，不平滑)。指数价格是另一个独立
// 概念，由外部行情源推送(MVP先靠脚本/运营手动喂，以后换成接入币安行情的适配器)，反映真实市场
// 价格，资金费率就是标记价格相对指数价格的溢价，见FundingService。
package service

import (
	"context"

	"github.com/shopspring/decimal"

	"perp-go/internal/cache"
)

type MarkPriceService struct {
	cache *cache.Cache
}

func NewMarkPriceService(c *cache.Cache) *MarkPriceService { return &MarkPriceService{cache: c} }

// Get 返回(zero, false)表示这个symbol还没有任何标记价格(从没成交过)，调用方要按"没有价格
// 就跳过这一轮判断"处理，不能当0价格用
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

// SetIndexPrice 外部行情源推送这个symbol的指数价格
func (s *MarkPriceService) SetIndexPrice(ctx context.Context, symbol string, price decimal.Decimal) error {
	return s.cache.SetIndexPrice(ctx, symbol, price.String())
}

// GetIndexPrice 返回(zero, false)表示这个symbol还没有任何外部行情源喂过指数价格
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
