package service

import (
	"context"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
	"perp-go/internal/repo"
)

type PositionService struct {
	positions *repo.PositionRepo
	coins     *repo.CoinRepo
	markPrice *MarkPriceService
}

func NewPositionService(positions *repo.PositionRepo, coins *repo.CoinRepo, markPrice *MarkPriceService) *PositionService {
	return &PositionService{positions: positions, coins: coins, markPrice: markPrice}
}

func (s *PositionService) FindByUID(ctx context.Context, uid uint64) ([]model.Position, error) {
	return s.positions.FindByUID(ctx, uid)
}

// TotalUnrealizedPnl 这个uid名下全部持仓当前未实现盈亏之和——freezeMargin的浮盈买力判断、
// 账户权益展示、强平风控扫描三处共用同一份计算。任何一个持仓缺标记价格就把它的浮盈当0(不计入)，
// 这是保守方向：算少了买力/权益顶多让强平判断更容易触发、开仓更容易被拒绝，不会让账户透支或
// 让强平被延误，照抄这次会话给Java版ContractAccountService.totalUnrealizedPnl加的同名方法
func (s *PositionService) TotalUnrealizedPnl(ctx context.Context, uid uint64) (decimal.Decimal, error) {
	positions, err := s.positions.FindByUID(ctx, uid)
	if err != nil {
		return decimal.Zero, err
	}
	total := decimal.Zero
	for _, p := range positions {
		if p.Volume.Sign() <= 0 {
			continue
		}
		mark, ok := s.markPrice.Get(ctx, p.Symbol)
		if !ok {
			continue
		}
		total = total.Add(p.UnrealizedPnl(mark))
	}
	return total, nil
}

// MaintenanceMarginTotal 这个uid名下全部持仓的维持保证金要求之和，风控强平判断用
func (s *PositionService) MaintenanceMarginTotal(ctx context.Context, uid uint64) (decimal.Decimal, []model.Position, bool, error) {
	positions, err := s.positions.FindByUID(ctx, uid)
	if err != nil {
		return decimal.Zero, nil, false, err
	}
	total := decimal.Zero
	for _, p := range positions {
		if p.Volume.Sign() <= 0 {
			continue
		}
		coin, err := s.coins.FindBySymbol(ctx, p.Symbol)
		if err != nil {
			return decimal.Zero, nil, false, err
		}
		mark, ok := s.markPrice.Get(ctx, p.Symbol)
		if coin == nil || !ok {
			// 缺标记价格/合约配置：整体跳过这一轮判断，不能把这个仓位当0处理(会让维持保证金
			// 要求算少，误判为安全)——照抄Java版LiquidationService.liquidateCrossAccountIfNeeded的取舍
			return decimal.Zero, nil, false, nil
		}
		notional := p.Volume.Mul(mark)
		total = total.Add(notional.Mul(coin.MaintenanceMarginRate))
	}
	return total, positions, true, nil
}
