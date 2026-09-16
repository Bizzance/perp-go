// 资金费率：永续合约锚定现货价格的核心机制——标记价格相对指数价格的溢价，按周期(默认8小时，
// 对齐到从0点起的整点边界，跟主流交易所UTC 00:00/08:00/16:00同样的对齐方式)结算给多空双方。
// 溢价为正(标记价格比指数价格贵)时多头付给空头——多头是把合约价格推高、造成溢价的一方；溢价
// 为负则反过来。指数价格由外部行情源推送(见MarkPriceService.SetIndexPrice)，MVP阶段先靠
// 脚本/运营手动喂，以后换成接入币安行情的适配器，这条链路(含下面的采样/结算逻辑)不用改。
package service

import (
	"context"
	"log"

	"github.com/shopspring/decimal"

	"perp-go/internal/cache"
	"perp-go/internal/model"
	"perp-go/internal/repo"
)

type FundingService struct {
	cache     *cache.Cache
	coins     *repo.CoinRepo
	positions *repo.PositionRepo
	funding   *repo.FundingRepo
	accounts  *AccountService
	tx        *repo.TxRepo
	markPrice *MarkPriceService
}

func NewFundingService(c *cache.Cache, coins *repo.CoinRepo, positions *repo.PositionRepo, funding *repo.FundingRepo,
	accounts *AccountService, tx *repo.TxRepo, markPrice *MarkPriceService) *FundingService {
	return &FundingService{
		cache: c, coins: coins, positions: positions, funding: funding,
		accounts: accounts, tx: tx, markPrice: markPrice,
	}
}

// SampleOnce 给每个启用的合约采一次样：溢价率=(标记价格-指数价格)/指数价格，累加进这个symbol
// 当前资金费率周期的累加器，结算时取累加器的均值当TWAP。标记价/指数价任一缺失就跳过——不能当0
// 处理，那样会把溢价算成一个错误的、有偏向性的值
func (s *FundingService) SampleOnce(ctx context.Context) {
	coins, err := s.coins.FindAllEnabled(ctx)
	if err != nil {
		log.Printf("[ERROR] funding sample: list coins failed: %v", err)
		return
	}
	for _, coin := range coins {
		mark, hasMark := s.markPrice.Get(ctx, coin.Symbol)
		index, hasIndex := s.markPrice.GetIndexPrice(ctx, coin.Symbol)
		if !hasMark || !hasIndex || index.Sign() <= 0 {
			continue
		}
		premium := mark.Sub(index).Div(index)
		if err := s.cache.AccumulateFundingSample(ctx, coin.Symbol, premium); err != nil {
			log.Printf("[ERROR] funding sample accumulate failed, symbol=%s: %v", coin.Symbol, err)
		}
	}
}

// SettleIfDue 给每个启用的合约判断是否跨过了下一个结算时间点，跨过了就结算这一周期
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

// fundingBoundary 结算周期边界不单独存"下次结算时间"，直接从当前时间和结算周期长度算出来——
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
	mark, hasMark := s.markPrice.Get(ctx, coin.Symbol)
	if !hasMark {
		return nil // 没有标记价格没法结算，跳过、等下一次tick重试
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

// settlePositions 按费率给这个symbol下每个仓位划转资金费：多头视角的资金费=名义价值*费率，
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

// EstimateRate 查询接口/结算共用：当前周期到目前为止的TWAP均值，clamp到±FundingRateCap——
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

// NextFundingTime 查询接口用：下一个结算时间点
func (s *FundingService) NextFundingTime(coin model.Coin, now int64) int64 {
	intervalMs := int64(coin.FundingIntervalHours) * 3600_000
	if intervalMs <= 0 {
		return 0
	}
	return fundingBoundary(now, intervalMs) + intervalMs
}

func (s *FundingService) History(ctx context.Context, symbol string, limit int) ([]model.FundingRateRecord, error) {
	return s.funding.FindHistory(ctx, symbol, limit)
}
