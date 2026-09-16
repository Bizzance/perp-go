package service

import (
	"context"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
	"perp-go/internal/repo"
)

type PositionService struct {
	positions  *repo.PositionRepo
	riskLimits *repo.RiskLimitRepo
	markPrice  *MarkPriceService
}

func NewPositionService(positions *repo.PositionRepo, riskLimits *repo.RiskLimitRepo, markPrice *MarkPriceService) *PositionService {
	return &PositionService{positions: positions, riskLimits: riskLimits, markPrice: markPrice}
}

// TierFor 给定symbol和名义价值，找到适用的保证金分档——按tier升序找第一个MaxNotional
// 覆盖到这个名义价值的档位；MaxNotional=0代表这一档不限(约定放在最后一档)。这个symbol
// 一档都没配返回(nil, nil)，调用方要按"没有风控依据、不允许交易"处理，不能当0档处理。
// 依赖一个配置时的约定：tier序号升序 == MaxNotional升序，这里不做单调性校验——风控参数
// 由运营配置，跟price_tick/volume_step这些字段一样属于"信任配置"，不在查询路径上重复校验
func (s *PositionService) TierFor(ctx context.Context, symbol string, notional decimal.Decimal) (*model.RiskLimitTier, error) {
	tiers, err := s.riskLimits.FindBySymbol(ctx, symbol)
	if err != nil || len(tiers) == 0 {
		return nil, err
	}
	for i := range tiers {
		if tiers[i].MaxNotional.Sign() <= 0 || notional.LessThanOrEqual(tiers[i].MaxNotional) {
			return &tiers[i], nil
		}
	}
	last := tiers[len(tiers)-1]
	return &last, nil
}

func (s *PositionService) FindByUID(ctx context.Context, uid uint64) ([]model.Position, error) {
	return s.positions.FindByUID(ctx, uid)
}

func (s *PositionService) Find(ctx context.Context, uid uint64, symbol string, side model.Side) (*model.Position, error) {
	return s.positions.Find(ctx, uid, symbol, side)
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

// MaintenanceMarginTotal 这个uid名下全部持仓的维持保证金要求之和，风控强平判断用——
// 维持保证金按分档公式notional*mmr-maintenanceAmount算，档位由这个仓位当前的名义价值决定
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
		mark, ok := s.markPrice.Get(ctx, p.Symbol)
		if !ok {
			// 缺标记价格：整体跳过这一轮判断，不能把这个仓位当0处理(会让维持保证金要求算少，
			// 误判为安全)
			return decimal.Zero, nil, false, nil
		}
		notional := p.Volume.Mul(mark)
		tier, err := s.TierFor(ctx, p.Symbol, notional)
		if err != nil {
			return decimal.Zero, nil, false, err
		}
		if tier == nil {
			// 没配分档同样不能当0处理，理由跟缺标记价格一样：算少了维持保证金会误判为安全
			return decimal.Zero, nil, false, nil
		}
		maint := decimal.Max(decimal.Zero, notional.Mul(tier.MaintenanceMarginRate).Sub(tier.MaintenanceAmount))
		total = total.Add(maint)
	}
	return total, positions, true, nil
}
