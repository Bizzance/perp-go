package service

import (
	"context"
	"log"
	"sync"

	"github.com/shopspring/decimal"

	"perp-go/internal/cache"
	"perp-go/internal/model"
	"perp-go/internal/repo"
)

// 按名义金额(USDT)吃盘口，返回冲击买价和冲击卖价，盘口不够深时ok=false。生产里是matching.Book.ImpactPrices
type ImpactPriceFunc func(symbol string, notional decimal.Decimal) (bid, ask decimal.Decimal, ok bool)

type FundingService struct {
	cache     *cache.Cache
	coins     *repo.CoinRepo
	positions *repo.PositionRepo
	funding   *repo.FundingRepo
	accounts  *AccountService
	tx        *repo.TxRepo
	markPrice *MarkPriceService

	// 采样要读订单簿，所以只有拥有这个symbol订单簿的engine实例才能采(分片部署下别的实例的本地订单簿是空的)，
	// 用WithBook接上。没接的话SampleOnce什么都采不了
	owns   func(symbol string) bool
	impact ImpactPriceFunc

	mu         sync.Mutex
	skipReason map[string]string // symbol -> 上一次采样被跳过的原因，只在变化时打日志
}

func NewFundingService(
	c *cache.Cache,
	coins *repo.CoinRepo,
	positions *repo.PositionRepo,
	funding *repo.FundingRepo,
	accounts *AccountService,
	tx *repo.TxRepo,
	markPrice *MarkPriceService,
) *FundingService {
	return &FundingService{
		cache:     c,
		coins:     coins,
		positions: positions,
		funding:   funding,
		accounts:  accounts,
		tx:        tx,
		markPrice: markPrice,

		skipReason: make(map[string]string),
	}
}

// 接上订单簿：owns判断这个engine实例负责不负责某个symbol，impact按名义金额算冲击买卖价。
// 只在contract-engine里调用，SampleOnce要用
func (s *FundingService) WithBook(owns func(symbol string) bool, impact ImpactPriceFunc) *FundingService {
	s.owns, s.impact = owns, impact
	return s
}

// 给这个engine实例负责的每个启用合约采一次样：算这一刻的溢价指数，累加进这个symbol当前资金费率周期的
// 累加器，结算时取累加器的均值当TWAP。溢价指数是币安的口径，用冲击价格而不是标记价：
//
//	溢价 = [max(0, 冲击买价 - 指数价) - max(0, 指数价 - 冲击卖价)] / 指数价
//
// 冲击买价/卖价是按合约的FundingImpactNotional(USDT)去吃订单簿两侧、吃完的平均成交价。盘口的买卖价都高于
// 指数价说明合约比现货贵、溢价为正；都低于指数价溢价为负；指数价夹在中间(买价<=指数价<=卖价)溢价为0。
// 以前用(标记价-指数价)/指数价，但标记价被夹在指数价和"指数价加基差"之间，溢价会被压缩到不超过盘口基差。
//
// 这些情况跳过这一个合约、不采样(不能当0处理，那样会把溢价算成有偏向性的错误值)：
//   - 不归这个实例负责：本地订单簿是空的。分片部署下每个symbol只有owner采样
//   - 指数价缺失或断供
//   - 没配冲击名义金额，或者任何一侧盘口的总名义价值不够：盘口太薄时没有可靠的冲击价格，
//     否则可以靠撤掉一侧的挂单来控制溢价
func (s *FundingService) SampleOnce(ctx context.Context) {
	if s.impact == nil {
		log.Printf("[ERROR] funding sample: 没有接订单簿(WithBook)，不能采样")
		return
	}
	coins, err := s.coins.FindAllEnabled(ctx)
	if err != nil {
		log.Printf("[ERROR] funding sample: list coins failed: %v", err)
		return
	}
	for _, coin := range coins {
		if s.owns != nil && !s.owns(coin.Symbol) {
			continue
		}
		index, hasIndex := s.markPrice.GetFreshIndex(ctx, coin.Symbol)
		if !hasIndex {
			continue // 喂价断了，标记价冻结、强平也暂停，见mark-price.md，这里不用另外告警
		}
		if coin.FundingImpactNotional.Sign() <= 0 {
			s.noteSkip(coin.Symbol, "没有配置冲击名义金额(funding_impact_notional)")
			continue
		}
		bid, ask, ok := s.impact(coin.Symbol, coin.FundingImpactNotional)
		if !ok {
			s.noteSkip(coin.Symbol, "盘口不够深，凑不够冲击名义金额"+coin.FundingImpactNotional.String())
			continue
		}
		s.noteSkip(coin.Symbol, "")
		premium := premiumFromImpact(index, bid, ask)
		if err := s.cache.AccumulateFundingSample(ctx, coin.Symbol, premium); err != nil {
			log.Printf("[ERROR] funding sample accumulate failed, symbol=%s: %v", coin.Symbol, err)
		}
	}
}

// 溢价指数：[max(0, 冲击买价-指数价) - max(0, 指数价-冲击卖价)] / 指数价。index必须大于0
func premiumFromImpact(index, impactBid, impactAsk decimal.Decimal) decimal.Decimal {
	up := decimal.Max(decimal.Zero, impactBid.Sub(index))
	down := decimal.Max(decimal.Zero, index.Sub(impactAsk))
	return up.Sub(down).Div(index)
}

// 记录这个symbol这一轮采样被跳过的原因(""=采样成功)，只在原因变化时打一条日志：采样每分钟一次，
// 一个长期没有深度的合约(比如测试环境的做市停了)不能每分钟刷一条
func (s *FundingService) noteSkip(symbol, reason string) {
	s.mu.Lock()
	prev := s.skipReason[symbol]
	s.skipReason[symbol] = reason
	s.mu.Unlock()
	switch {
	case reason != "" && reason != prev:
		log.Printf("[WARN] 资金费率采样被跳过, symbol=%s: %s。这个周期的资金费率只按已采到的样本算，一个样本都没有就是0", symbol, reason)
	case reason == "" && prev != "":
		log.Printf("[INFO] 资金费率采样恢复, symbol=%s", symbol)
	}
}

// 给每个启用的合约判断是否跨过了下一个结算时间点，跨过了就结算这一周期
func (s *FundingService) SettleIfDue(ctx context.Context, now int64) {
	coins, err := s.coins.FindAllEnabled(ctx)
	if err != nil {
		log.Printf("[ERROR] funding settle: list coins failed: %v", err)
		return
	}
	for _, coin := range coins {
		if err := s.settleSymbolIfDue(ctx, coin, now); err != nil {
			log.Printf("[ERROR] funding settle failed, symbol=%s: %v", coin.Symbol, err)
		}
	}
}

// 结算周期边界不单独存"下次结算时间"，直接从当前时间和结算周期长度算出来——
// 对齐到从Unix纪元(0点)起的整点边界，interval=8小时时天然落在UTC 00:00/08:00/16:00
func fundingBoundary(now, intervalMs int64) int64 { return (now / intervalMs) * intervalMs }

func (s *FundingService) settleSymbolIfDue(ctx context.Context, coin model.Coin, now int64) error {
	intervalMs := int64(coin.FundingIntervalHours) * 3600_000
	if intervalMs <= 0 {
		return nil
	}
	boundary := fundingBoundary(now, intervalMs)
	lastSettled, err := s.funding.LastFundingTime(ctx, coin.Symbol)
	if err != nil {
		return err
	}
	if boundary <= lastSettled {
		return nil // 还没跨过下一个结算点
	}
	mark, hasMark := s.markPrice.GetFresh(ctx, coin.Symbol)
	if !hasMark {
		return nil // 没有标记价格、或者喂价断了标记价已经过期，没法结算，跳过、等下一次tick重试
	}
	index, hasIndex := s.markPrice.GetIndexPrice(ctx, coin.Symbol)
	if !hasIndex {
		index = decimal.Zero // 只是审计记录里的展示字段，指数价格缺失不影响rate本身(已经是历史采样算出来的)
	}
	rate := s.EstimateRate(ctx, coin)

	// 先落一条结算记录占住这个周期(symbol,funding_time)的UNIQUE KEY，再做真正的资金划转——
	// 顺序不能反过来：如果先转账、划到一半失败才发现落库失败，下一次tick会认为这个周期还没
	// 结算过，重新把已经转过账的仓位再转一遍，造成重复扣款/派发。现在这个顺序下，最坏情况是
	// "这个周期有仓位划转失败、没收到/没扣到资金费"，不会出现同一笔钱被重复结算
	if err := s.funding.Insert(ctx, &model.FundingRateRecord{
		Symbol: coin.Symbol, FundingTime: boundary, Rate: rate, MarkPrice: mark, IndexPrice: index, CreateTime: now,
	}); err != nil {
		return err
	}
	if !rate.IsZero() {
		s.settlePositions(ctx, coin.Symbol, rate, mark, boundary)
	}
	log.Printf("[INFO] 资金费率结算完成, symbol=%s, fundingTime=%d, rate=%s, markPrice=%s", coin.Symbol, boundary, rate, mark)
	return s.cache.ResetFundingAccumulator(ctx, coin.Symbol)
}

// 按费率给这个symbol下每个仓位划转资金费：多头视角的资金费=名义价值*费率，
// 正数=多头要付出去的钱；空头是多头的镜像，符号相反，跟真实的多空力量对比无关，统一走这一个公式。
// 结算记录已经落库、这个周期不会重试，单个仓位划转失败只记日志、不中断其它仓位的结算——
// 中断整批的话，排在后面的仓位会白白错过这一期资金费，比只错过这一个仓位的影响更大
func (s *FundingService) settlePositions(ctx context.Context, symbol string, rate, markPrice decimal.Decimal, now int64) {
	positions, err := s.positions.FindOpenBySymbol(ctx, symbol)
	if err != nil {
		log.Printf("[ERROR] funding settle: list positions failed, symbol=%s: %v", symbol, err)
		return
	}
	for _, p := range positions {
		longFundingFee := p.Volume.Mul(markPrice).Mul(rate)
		fundingFee := longFundingFee
		if p.Side == model.SideShort {
			fundingFee = longFundingFee.Neg()
		}
		if fundingFee.IsZero() {
			continue
		}
		if err := s.accounts.SettleToAvailable(ctx, p.UID, fundingFee.Neg()); err != nil {
			log.Printf("[ERROR] funding settle: uid=%d symbol=%s: %v", p.UID, symbol, err)
			continue
		}
		if err := s.tx.Insert(ctx, p.UID, symbol, model.TxFundingFee, fundingFee.Neg(), now); err != nil {
			log.Printf("[ERROR] funding settle: write ledger failed, uid=%d symbol=%s: %v", p.UID, symbol, err)
		}
	}
}

// 查询接口/结算共用：当前周期到目前为止的TWAP均值，clamp到±FundingRateCap——
// 主流交易所结算前展示的"预测资金费率"也是同样的实时估算值，不是等结算那一刻才有数
func (s *FundingService) EstimateRate(ctx context.Context, coin model.Coin) decimal.Decimal {
	sum, count, err := s.cache.GetFundingAccumulator(ctx, coin.Symbol)
	if err != nil || count == 0 {
		return decimal.Zero
	}
	rate := sum.Div(decimal.NewFromInt(count))
	rateCap := coin.FundingRateCap
	if rateCap.Sign() <= 0 {
		return rate
	}
	return decimal.Max(rateCap.Neg(), decimal.Min(rate, rateCap))
}

// 查询接口用：下一个结算时间点
func (s *FundingService) NextFundingTime(coin model.Coin, now int64) int64 {
	intervalMs := int64(coin.FundingIntervalHours) * 3600_000
	if intervalMs <= 0 {
		return 0
	}
	return fundingBoundary(now, intervalMs) + intervalMs
}

func (s *FundingService) History(ctx context.Context, symbol string, limit int, before int64) ([]model.FundingRateRecord, error) {
	return s.funding.FindHistory(ctx, symbol, limit, before)
}
