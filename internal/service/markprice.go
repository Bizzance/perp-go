// 标记价格：MVP阶段用"最新成交价"本身当标记价格(EMA alpha=1，不平滑)——照抄Java版本地开发
// profile的简化(`contract.mark-price.source=trade`)，不接外部指数价格。
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
