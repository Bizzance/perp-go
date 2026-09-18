package service

import (
	"context"
	"log"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
	"perp-go/internal/repo"
)

const protectPriceBufferMultiplier = 2 // 保护价滑点缓冲=维持保证金率的这个倍数，思路借鉴真实交易所"破产价附近留保护价"

type LiquidationService struct {
	engine        *EngineService
	orders        *repo.OrderRepo
	positions     *repo.PositionRepo
	positionSvc   *PositionService
	markPrice     *MarkPriceService
	accounts      *AccountService
	fund          *InsuranceFundService
	coins         *repo.CoinRepo
	timeoutMillis int64
}

func NewLiquidationService(
	engine *EngineService,
	orders *repo.OrderRepo,
	positions *repo.PositionRepo,
	positionSvc *PositionService,
	markPrice *MarkPriceService,
	accounts *AccountService,
	fund *InsuranceFundService,
	coins *repo.CoinRepo,
	timeoutMillis int64,
) *LiquidationService {
	return &LiquidationService{
		engine:        engine,
		orders:        orders,
		positions:     positions,
		positionSvc:   positionSvc,
		markPrice:     markPrice,
		accounts:      accounts,
		fund:          fund,
		coins:         coins,
		timeoutMillis: timeoutMillis,
	}
}

// 全部有仓位的账户扫一遍
func (s *LiquidationService) RiskScanOnce(ctx context.Context) {
	uids, err := s.positions.FindAllOpenUIDs(ctx)
	if err != nil {
		log.Printf("[ERROR] risk scan: list uids failed: %v", err)
		return
	}
	for _, uid := range uids {
		if err := s.checkAndLiquidate(ctx, uid); err != nil {
			log.Printf("[ERROR] risk scan uid=%d failed: %v", uid, err)
		}
	}
}

func (s *LiquidationService) checkAndLiquidate(ctx context.Context, uid uint64) error {
	maintainTotal, positions, err := s.positionSvc.MaintenanceMarginTotal(ctx, uid)
	if err != nil || len(positions) == 0 {
		return err
	}
	available, err := s.accounts.FindFreshAvailable(ctx, uid)
	if err != nil {
		return err
	}
	credit, err := s.accounts.FindFreshCredit(ctx, uid)
	if err != nil {
		return err
	}
	totalUnrealized, err := s.positionSvc.TotalUnrealizedPnl(ctx, uid)
	if err != nil {
		return err
	}
	// 账户权益把credit算进去，信用额度才能真正起到"扛住浮亏、推迟强平"的作用——不这样算的话
	// 信用额度就只是个能开仓的额度，对避免强平没有意义
	equity := available.Add(credit).Add(totalUnrealized)
	if equity.GreaterThan(maintainTotal) {
		return nil
	}
	// 过滤掉已经在LIQUIDATING的仓位(上一轮扫描已经挂出强平单、还在排队/等超时兜底)——不然
	// 这条告警日志会在整个强平窗口期(最多到LiquidationOrderTimeoutMs)里每个扫描周期重复刷屏，
	// 同一批仓位实际只会真正挂一次单(queueLiquidation内部的MarkLiquidating原子guard保证)，
	// 这里只是让日志跟真实发生的动作对得上
	pending := make([]model.Position, 0, len(positions))
	for _, p := range positions {
		if p.Volume.Sign() > 0 && p.Status == model.PositionStatusNormal {
			pending = append(pending, p)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	log.Printf("[WARN] 触发全仓联合强平, uid=%d, 账户权益=%s, 维持保证金要求=%s, 仓位数=%d", uid, equity, maintainTotal, len(pending))
	for _, p := range pending {
		if !s.engine.OwnsSymbol(p.Symbol) {
			// 分片部署下(docs/engine-sharding.md)风控扫描在每个实例上都独立跑一遍(全仓强平
			// 判断本来就需要看这个uid名下全部symbol的仓位，没法只让owning实例扫)，但真正
			// 挂强平单这个动作只能由拥有这个symbol订单簿的实例来做——不然queueLiquidation
			// 会把MarkLiquidating这个一次性的原子状态转换在一个从来没有真实订单簿的实例上
			// 用掉，真正拥有这个symbol的实例的扫描会看到MarkLiquidating已经失败(不是
			// normal状态了)而放弃，这个仓位就再也没人真正挂强平单
			continue
		}
		s.queueLiquidation(ctx, p)
	}
	return nil
}

// queueLiquidation 强平入口：原子标记这个仓位进入LIQUIDATING，成功之后把真正的分批挂单
// 循环丢给一个独立goroutine异步跑(liquidateInClips)——不在风控扫描这个调用路径上等，
// 一个大仓位分批下来可能要跨越好几个LiquidationOrderTimeoutMs，不能让RiskScanOnce的
// 一次tick被卡住
func (s *LiquidationService) queueLiquidation(ctx context.Context, p model.Position) {
	ok, err := s.positions.MarkLiquidating(ctx, p.ID)
	if err != nil || !ok {
		return // 上一轮已经在强平中，跳过
	}
	// 仓位状态变成liquidating这件事本身就该立刻让客户端知道(这是用户能看到的风险提示)，
	// 不能只等到强平单真的成交才推送——挂出去的保护价单可能要等一段时间才被吃到
	s.engine.PublishUserSnapshot(ctx, p.UID)
	go s.liquidateInClips(context.Background(), p.UID, p.Symbol, p.Side)
}

// liquidateInClips 大仓位分批强平：单批强平挂单量不超过coin.MaxVolume——这是普通下单本来
// 就有的单笔最大量限制(router.go的addOrder同样校验)，强平单没理由例外，一次性把一个远超
// 正常单笔上限的大仓位甩给盘口，对价格冲击太大，也超出这个symbol正常交易时的流动性预期，
// 见docs/liquidation.md。依次挂出每一批，各自走"挂保护价单→(立刻全部成交/取消就马上进
// 入下一批，不用傻等超时；否则等LiquidationOrderTimeoutMs超时兜底直接结算这一批未成交的
// 剩余部分)"，直到这个仓位volume归零。中途任何一步没法继续(缺标记价格/缺分档配置/落库
// 失败)都会把状态撤回normal并退出整个循环，交给下一轮风控扫描重新判断要不要继续强平——
// 不重试同一批，那些失败原因不是"这一批运气不好"，重试大概率还是失败
func (s *LiquidationService) liquidateInClips(ctx context.Context, uid uint64, symbol string, side model.Side) {
	for {
		p, err := s.positions.Find(ctx, uid, symbol, side)
		if err != nil {
			log.Printf("[ERROR] 分批强平查询仓位失败, uid=%d, symbol=%s: %v", uid, symbol, err)
			return
		}
		if p == nil || p.Volume.Sign() <= 0 {
			return // 已经清仓完毕，正常结束
		}

		orderID, ok := s.submitLiquidationClip(ctx, *p)
		if !ok {
			return // submitLiquidationClip内部已经处理了ClearLiquidating+推送快照
		}

		// SubmitOrder是同步撮合的，返回时如果这一批已经全部成交/取消，直接检查下一批，
		// 不用傻等超时——正常流动性下的强平单大概率能立刻成交，没必要每一批都死等
		// LiquidationOrderTimeoutMs
		o, err := s.orders.FindByOrderID(ctx, orderID)
		if err == nil && o != nil && !model.ActiveOrderStatuses[o.Status] {
			continue
		}
		time.Sleep(time.Duration(s.timeoutMillis) * time.Millisecond)
		s.settleTimeoutFallback(ctx, orderID)
	}
}

// submitLiquidationClip 挂出一批强平单，数量按coin.MaxVolume截断。返回ok=false表示这一批
// 没能挂出去，调用方(liquidateInClips)应该直接退出整个强平循环
func (s *LiquidationService) submitLiquidationClip(ctx context.Context, p model.Position) (uint64, bool) {
	mark, hasMark := s.markPrice.Get(ctx, p.Symbol)
	if !hasMark {
		s.positions.ClearLiquidating(ctx, p.ID)
		// 上面(queueLiquidation)已经推过一次"liquidating"快照，这里状态被撤销回normal了，
		// 必须补一次快照，不然客户端会一直卡在"liquidating"这个已经不成立的状态上——这两次
		// 推送之间的短暂窗口期，客户端看到的risk状态跟服务端不一致是可以接受的(风控扫描
		// 本来就是定期轮询，不是绝对实时)，但状态回退之后不推送、永远不同步是不能接受的
		s.engine.PublishUserSnapshot(ctx, p.UID)
		return 0, false
	}
	tier, err := s.positionSvc.TierFor(ctx, p.Symbol, p.Volume.Mul(mark))
	if err != nil || tier == nil {
		s.positions.ClearLiquidating(ctx, p.ID)
		s.engine.PublishUserSnapshot(ctx, p.UID)
		return 0, false
	}
	buffer := mark.Mul(tier.MaintenanceMarginRate).Mul(decimal.NewFromInt(protectPriceBufferMultiplier))
	protectPrice := mark.Sub(buffer)
	if p.Side == model.SideShort {
		protectPrice = mark.Add(buffer)
	}

	clipVolume := p.Volume
	if coin, err := s.coins.FindBySymbol(ctx, p.Symbol); err == nil && coin != nil &&
		coin.MaxVolume.Sign() > 0 && clipVolume.GreaterThan(coin.MaxVolume) {
		clipVolume = coin.MaxVolume
	}

	orderID := NextID()
	now := NowMillis()
	o := &model.Order{
		OrderID:     orderID,
		UID:         p.UID,
		Symbol:      p.Symbol,
		Side:        p.Side,
		Action:      model.ActionClose,
		Type:        model.OrderTypeLimit,
		Price:       protectPrice,
		Amount:      clipVolume,
		Leverage:    p.Leverage,
		ReduceOnly:  true,
		Liquidation: true,
		Status:      model.OrderStatusOpen,
		CreateTime:  now,
		UpdateTime:  now,
	}
	log.Printf("[WARN] 触发强平-挂单排队, uid=%d, symbol=%s, side=%s, 本批量=%s, 仓位剩余量=%s, markPrice=%s, 保护价=%s",
		p.UID, p.Symbol, p.Side, clipVolume, p.Volume, mark, protectPrice)
	if err := s.orders.Insert(ctx, o); err != nil {
		log.Printf("[ERROR] insert liquidation order failed: %v", err)
		s.positions.ClearLiquidating(ctx, p.ID)
		s.engine.PublishUserSnapshot(ctx, p.UID)
		return 0, false
	}
	if err := s.engine.SubmitOrder(ctx, o, time.Now().UnixNano()); err != nil {
		log.Printf("[ERROR] submit liquidation order failed: %v", err)
	}
	return orderID, true
}

// settleTimeoutFallback 一批强平单挂出去LiquidationOrderTimeoutMs还没成交完，撤掉这一批
// 剩余的部分，按当前标记价直接结算——注意是"这一批"未成交的量(o.RemainingAmount())，
// 不是整个仓位剩余的量，分批强平下这两者可能不相等(仓位可能还有别的批次没开始处理)
func (s *LiquidationService) settleTimeoutFallback(ctx context.Context, orderID uint64) {
	o, err := s.orders.FindByOrderID(ctx, orderID)
	if err != nil || o == nil || !model.ActiveOrderStatuses[o.Status] {
		return // 已经成交完/取消了，没什么可兜底的
	}
	book := s.engine.matchingEngine.BookFor(o.Symbol)
	if _, ok := book.Cancel(o.OrderID); ok { // 摘掉簿子上剩余的部分，避免超时兜底之后又意外撮合成交
		s.engine.PublishDepth(ctx, o.Symbol)
	}

	p, err := s.positions.Find(ctx, o.UID, o.Symbol, o.Side)
	if err != nil || p == nil || p.Volume.Sign() <= 0 {
		s.orders.MarkCanceled(ctx, orderID, NowMillis())
		return
	}
	mark, ok := s.markPrice.Get(ctx, o.Symbol)
	if !ok {
		return
	}
	closeVolume := decimal.Min(p.Volume, o.RemainingAmount())
	log.Printf("[WARN] 触发强平超时兜底直接结算, uid=%d, symbol=%s, side=%s, volume=%s, markPrice=%s",
		o.UID, o.Symbol, o.Side, closeVolume, mark)

	now := NowMillis()
	if err := s.orders.ApplyFill(ctx, orderID, closeVolume, mark, model.OrderStatusFilled, now); err != nil {
		log.Printf("[ERROR] apply fallback fill failed: %v", err)
		return
	}
	if _, err := s.engine.settlement.SettleFill(ctx, o, closeVolume, mark, false, now); err != nil {
		log.Printf("[ERROR] settle fallback fill failed: %v", err)
		return
	}
	if err := s.engine.HandleLiquidationSettleAftermath(ctx, o.Symbol, o.UID); err != nil {
		log.Printf("[ERROR] liquidation aftermath (fallback) failed: %v", err)
	}
	// 这条路径直接调settlement.SettleFill，不经过settleOneFill，所以snapshot推送要在这里
	// 单独补一次——跟settleOneFill里"结算完毕后推送"是同一个时机，只是走的是不同代码路径
	s.engine.PublishUserSnapshot(ctx, o.UID)
}
