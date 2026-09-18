package service

import (
	"context"
	"log"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
	"perp-go/internal/repo"
)

type PositionService struct {
	positions  *repo.PositionRepo
	riskLimits *repo.RiskLimitRepo
	markPrice  *MarkPriceService
}

func NewPositionService(
	positions *repo.PositionRepo,
	riskLimits *repo.RiskLimitRepo,
	markPrice *MarkPriceService,
) *PositionService {
	return &PositionService{
		positions:  positions,
		riskLimits: riskLimits,
		markPrice:  markPrice,
	}
}

// 给定symbol和名义价值，找到适用的保证金分档——按tier升序找第一个MaxNotional覆盖到这个名义价值的档位；MaxNotional=0代表这一档不限(约定放在最后一档)。
// 这个symbol一档都没配返回(nil, nil)，调用方要按"没有风控依据、不允许交易"处理，不能当0档处理。
// 依赖一个配置时的约定：tier序号升序 == MaxNotional升序，配置出错(比如两档的MaxNotional填反了)会导致按tier顺序找到的档位跟名义价值大小顺序对不上、静默命中错误的档位——风控
// 失效但没有任何报错，后果比"没配置"更隐蔽，所以这里做一次单调性校验，一旦不满足就按照"没配置"处理(调用方已经有现成的安全兜底)，而不是假装没看见继续算下去
func (s *PositionService) TierFor(ctx context.Context, symbol string, notional decimal.Decimal) (*model.RiskLimitTier, error) {
	tiers, err := s.riskLimits.FindBySymbol(ctx, symbol)
	if err != nil || len(tiers) == 0 {
		return nil, err
	}
	for i := 1; i < len(tiers); i++ {
		prev, cur := tiers[i-1], tiers[i]
		if prev.MaxNotional.Sign() <= 0 || (cur.MaxNotional.Sign() > 0 && cur.MaxNotional.LessThanOrEqual(prev.MaxNotional)) {
			log.Printf("[ERROR] 保证金分档配置不满足名义价值单调递增，按未配置处理, symbol=%s, tier=%d, tier=%d",
				symbol, prev.Tier, cur.Tier)
			return nil, nil
		}
	}
	for i := range tiers {
		if tiers[i].MaxNotional.Sign() <= 0 || notional.LessThanOrEqual(tiers[i].MaxNotional) {
			return &tiers[i], nil
		}
	}
	// 走到这里说明名义价值超过了最后一档的MaxNotional，而按约定最后一档本该是MaxNotional=0
	// (不限)——上面的单调性校验已经保证前面各档递增，能走到这里只可能是配置漏掉了那档"不限"，
	// 不是名义价值真的超出了业务上限。不能像最初那样直接套用最后一档的参数蒙混过关(等于让
	// 超出配置覆盖范围的仓位也被当成"已知风险"处理)，按未配置处理，跟单调性校验失败同一个
	// 安全兜底
	log.Printf("[ERROR] 保证金分档配置缺最后一档(MaxNotional=0/不限)，名义价值超出覆盖范围，按未配置处理, symbol=%s, notional=%s",
		symbol, notional)
	return nil, nil
}

func (s *PositionService) FindByUID(ctx context.Context, uid uint64) ([]model.Position, error) {
	return s.positions.FindByUID(ctx, uid)
}

// PositionView 查询接口/WS账户快照共用的展示视图：持仓原始字段+现算的标记价/未实现盈亏/
// 回报率/名义价值/预估强平价。之前REST的GET /position/current和WS的私有账户快照
// (PushService.PublishUserSnapshot)各自独立算了一遍这些计算字段，WS那份漏掉了全部计算
// 字段，只推裸的model.Position——两处对同一个资源的"完整视图"定义不一致，容易让依赖WS
// 推送做风控展示的客户端拿到的字段跟REST查询不对等。现在统一收进这一个方法，两边共用
type PositionView struct {
	model.Position
	MarkPrice        decimal.Decimal `json:"markPrice"`
	UnrealizedPnl    decimal.Decimal `json:"unrealizedPnl"`
	Roe              decimal.Decimal `json:"roe"`
	NotionalValue    decimal.Decimal `json:"notionalValue"`
	LiquidationPrice decimal.Decimal `json:"liquidationPrice"`
}

// Views 这个uid名下全部持仓的展示视图
func (s *PositionService) Views(ctx context.Context, uid uint64) ([]PositionView, error) {
	positions, err := s.positions.FindByUID(ctx, uid)
	if err != nil {
		return nil, err
	}
	views := make([]PositionView, 0, len(positions))
	for _, p := range positions {
		v := PositionView{Position: p}
		mark, hasMark := s.markPrice.Get(ctx, p.Symbol)
		if hasMark {
			v.MarkPrice = mark
			v.UnrealizedPnl = p.UnrealizedPnl(mark)
			v.NotionalValue = p.Volume.Mul(mark)
			if p.PositionMargin.Sign() > 0 {
				v.Roe = v.UnrealizedPnl.Div(p.PositionMargin)
			}
			if tier, err := s.TierFor(ctx, p.Symbol, v.NotionalValue); err == nil && tier != nil {
				v.LiquidationPrice = p.LiquidationPrice(tier.MaintenanceMarginRate, tier.MaintenanceAmount)
			}
		}
		views = append(views, v)
	}
	return views, nil
}

func (s *PositionService) Find(ctx context.Context, uid uint64, symbol string, side model.Side) (*model.Position, error) {
	return s.positions.Find(ctx, uid, symbol, side)
}

// 这个uid名下全部持仓当前未实现盈亏之和——freezeMargin的浮盈买力判断、
// 账户权益展示、强平风控扫描三处共用同一份计算。任何一个持仓缺标记价格就把它的浮盈当0(不计入)，
// 这是保守方向：算少了买力/权益顶多让强平判断更容易触发、开仓更容易被拒绝，不会让账户透支或
// 让强平被延误
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

// 这个uid名下全部持仓的维持保证金要求之和，风控强平判断用——
// 维持保证金按分档公式notional*mmr-maintenanceAmount算，档位由这个仓位当前的名义价值决定。
// 缺标记价格/分档配置的仓位只跳过它自己这一份贡献(计入0，打ERROR日志)，不能因为一个symbol
// 缺数据就让调用方跳过这个uid的整轮风控扫描——那样会连累这个用户名下其它数据齐全、可能
// 已经跌破维持保证金的仓位也被一起放过，比"漏算这一个仓位"更危险。
//
// 注意这里跳过=计0，跟TotalUnrealizedPnl把缺数据的仓位浮盈算0不是同一个安全方向：
// TotalUnrealizedPnl算0会拉低equity，让强平判断更容易触发，是保守方向；这里给
// maintainTotal算0是相反方向——会让这个仓位自己永远不足以成为触发强平的原因，即使它在
// 持续亏损(那笔亏损依然会通过TotalUnrealizedPnl正常拉低equity，不会完全没有感知，
// 只是没有为这个仓位单独贡献维持保证金要求)。这是分档配置在仓位已开仓后被删除/改坏
// 这个操作失误场景下的已知残留风险，见docs/known-limitations.md，MVP阶段选择打日志
// 告警而不是引入"缺配置就假设最大风险、强制触发强平"这类会带来其它副作用的兜底逻辑
func (s *PositionService) MaintenanceMarginTotal(ctx context.Context, uid uint64) (decimal.Decimal, []model.Position, error) {
	positions, err := s.positions.FindByUID(ctx, uid)
	if err != nil {
		return decimal.Zero, nil, err
	}
	total := decimal.Zero
	for _, p := range positions {
		if p.Volume.Sign() <= 0 {
			continue
		}
		mark, ok := s.markPrice.Get(ctx, p.Symbol)
		if !ok {
			log.Printf("[WARN] 维持保证金计算缺标记价格，跳过这个仓位, uid=%d, symbol=%s", uid, p.Symbol)
			continue
		}
		notional := p.Volume.Mul(mark)
		tier, err := s.TierFor(ctx, p.Symbol, notional)
		if err != nil {
			return decimal.Zero, nil, err
		}
		if tier == nil {
			// ERROR不是WARN：这个仓位的维持保证金贡献被计成0，会削弱这个uid的强平判断，
			// 属于需要运营立刻介入排查配置的问题，不是可以自愈的瞬时状态
			log.Printf("[ERROR] 维持保证金计算缺分档配置，该仓位维持保证金按0计入，跳过, uid=%d, symbol=%s", uid, p.Symbol)
			continue
		}
		maint := decimal.Max(decimal.Zero, notional.Mul(tier.MaintenanceMarginRate).Sub(tier.MaintenanceAmount))
		total = total.Add(maint)
	}
	return total, positions, nil
}
