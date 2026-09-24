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

// 这个symbol配置的全部保证金分档(按tier升序)，合约信息查询接口用
func (s *PositionService) Tiers(ctx context.Context, symbol string) ([]model.RiskLimitTier, error) {
	return s.riskLimits.FindBySymbol(ctx, symbol)
}

func (s *PositionService) FindByUID(ctx context.Context, uid uint64) ([]model.Position, error) {
	return s.positions.FindByUID(ctx, uid)
}

// 查询接口/WS账户快照共用的展示视图：持仓原始字段+现算的标记价/未实现盈亏/
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

// 这个uid名下全部持仓的展示视图。insured是账户当前的投保状态，equityBase是账户当前的
// balance+credit——两者一起决定预估强平价用的lossRatio*equityBase(见
// LiquidationService.checkAndLiquidate，投保保的是整个账户，不是单笔仓位)
func (s *PositionService) Views(ctx context.Context, uid uint64, insured bool, equityBase decimal.Decimal) ([]PositionView, error) {
	positions, err := s.positions.FindByUID(ctx, uid)
	if err != nil {
		return nil, err
	}
	lossRatio := lossRatioFor(insured)
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
			v.LiquidationPrice = p.LiquidationPrice(lossRatio, equityBase)
		}
		views = append(views, v)
	}
	return views, nil
}

func (s *PositionService) Find(ctx context.Context, uid uint64, symbol string, side model.Side) (*model.Position, error) {
	return s.positions.Find(ctx, uid, symbol, side)
}

// 这个symbol下全部还有仓位的记录，不分uid——ADL(见adl.go)挑选反向最
// 赚钱的仓位强制减仓时用来找候选池
func (s *PositionService) FindOpenBySymbol(ctx context.Context, symbol string) ([]model.Position, error) {
	return s.positions.FindOpenBySymbol(ctx, symbol)
}

// 见repo.PositionRepo.UpdateLeverage——独立杠杆设置接口(docs/leverage.md)
// 修改完保证金冻结之后，用这个把仓位自己的记账字段(保证金/杠杆)同步成新值
func (s *PositionService) UpdateLeverage(ctx context.Context, id uint64, newMargin, newCreditMargin decimal.Decimal, newLeverage uint32, expectedVolume decimal.Decimal, updateTime int64) (bool, error) {
	return s.positions.UpdateLeverage(ctx, id, newMargin, newCreditMargin, newLeverage, expectedVolume, updateTime)
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
	return s.TotalUnrealizedPnlOf(ctx, positions), nil
}

// 跟TotalUnrealizedPnl算的是同一个口径，但接收调用方已经查好的持仓列表，不再重新查一遍
// FindByUID——LiquidationService.checkAndLiquidate风控扫描时前面已经为了查标记价新鲜度
// 查过一次持仓，这里不需要为了算浮盈亏再查第二次，风控扫描对每个uid的查询次数直接减半
func (s *PositionService) TotalUnrealizedPnlOf(ctx context.Context, positions []model.Position) decimal.Decimal {
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
	return total
}

// 全部持仓占用的保证金之和(含来自信用额度的部分)，查询接口用于展示——这笔钱一直锁在
// accounts.frozen_margin/frozen_credit里(见settlement.go)，账户权益(Equity)不需要
// 再单独加一遍，这里只是按仓位维度把同一份锁定重新汇总一次
func sumPositionMargin(positions []model.Position) decimal.Decimal {
	total := decimal.Zero
	for _, p := range positions {
		if p.Volume.Sign() > 0 {
			total = total.Add(p.PositionMargin)
		}
	}
	return total
}

// 这个uid名下全部持仓占用的保证金之和，见sumPositionMargin
func (s *PositionService) TotalPositionMargin(ctx context.Context, uid uint64) (decimal.Decimal, error) {
	positions, err := s.positions.FindByUID(ctx, uid)
	if err != nil {
		return decimal.Zero, err
	}
	return sumPositionMargin(positions), nil
}

// LossRatioUninsured/LossRatioInsured：维持保证金要求现在按"亏损达到账户总资产(balance+
// credit)的这个比例"算，不再用risk_limit_tiers的分档mmr公式(那张表继续用于开仓/改杠杆的
// 最大杠杆校验、以及强平单保护价缓冲，见liquidation.go)。未投保的账户亏光全部余额(100%)
// 才强平；已投保的账户风控收紧到亏损80%就强平——投保就是保整个账户，不是只保某一笔仓位的
// 保证金，这是产品口径的简化决定(先不支持"只投保一部分")。用户随时可以投保，这里每次都读
// 账户当前的投保状态，不是仓位开仓时锁定的快照，见LiquidationService.checkAndLiquidate
var (
	LossRatioUninsured = decimal.NewFromInt(1)            // 100%
	LossRatioInsured   = decimal.RequireFromString("0.8") // 80%
)

func lossRatioFor(insured bool) decimal.Decimal {
	if insured {
		return LossRatioInsured
	}
	return LossRatioUninsured
}
