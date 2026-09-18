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
		s.queueLiquidation(ctx, p)
	}
	return nil
}

func (s *LiquidationService) queueLiquidation(ctx context.Context, p model.Position) {
	ok, err := s.positions.MarkLiquidating(ctx, p.ID)
	if err != nil || !ok {
		return // 上一轮已经在强平中，跳过
	}
	// 仓位状态变成liquidating这件事本身就该立刻让客户端知道(这是用户能看到的风险提示)，
	// 不能只等到强平单真的成交才推送——挂出去的保护价单可能要等一段时间才被吃到
	s.engine.PublishUserSnapshot(ctx, p.UID)
	mark, hasMark := s.markPrice.Get(ctx, p.Symbol)
	if !hasMark {
		s.positions.ClearLiquidating(ctx, p.ID)
		// 上面已经推过一次"liquidating"快照，这里状态被撤销回normal了，必须补一次快照，
		// 不然客户端会一直卡在"liquidating"这个已经不成立的状态上——这两次推送之间的
		// 短暂窗口期，客户端看到的risk状态跟服务端不一致是可以接受的(风控扫描本来就是
		// 定期轮询，不是绝对实时)，但状态回退之后不推送、永远不同步是不能接受的
		s.engine.PublishUserSnapshot(ctx, p.UID)
		return
	}
	tier, err := s.positionSvc.TierFor(ctx, p.Symbol, p.Volume.Mul(mark))
	if err != nil || tier == nil {
		s.positions.ClearLiquidating(ctx, p.ID)
		s.engine.PublishUserSnapshot(ctx, p.UID)
		return
	}
	buffer := mark.Mul(tier.MaintenanceMarginRate).Mul(decimal.NewFromInt(protectPriceBufferMultiplier))
	protectPrice := mark.Sub(buffer)
	if p.Side == model.SideShort {
		protectPrice = mark.Add(buffer)
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
		Amount:      p.Volume,
		Leverage:    p.Leverage,
		ReduceOnly:  true,
		Liquidation: true,
		Status:      model.OrderStatusOpen,
		CreateTime:  now,
		UpdateTime:  now,
	}
	log.Printf("[WARN] 触发强平-挂单排队, uid=%d, symbol=%s, side=%s, volume=%s, markPrice=%s, 保护价=%s",
		p.UID, p.Symbol, p.Side, p.Volume, mark, protectPrice)
	if err := s.orders.Insert(ctx, o); err != nil {
		log.Printf("[ERROR] insert liquidation order failed: %v", err)
		s.positions.ClearLiquidating(ctx, p.ID)
		s.engine.PublishUserSnapshot(ctx, p.UID)
		return
	}
	if err := s.engine.SubmitOrder(ctx, o, time.Now().UnixNano()); err != nil {
		log.Printf("[ERROR] submit liquidation order failed: %v", err)
	}

	// 超时兜底：挂出去LiquidationOrderTimeoutMs还没成交完，撤掉剩余部分，按当前标记价直接结算
	go func() {
		time.Sleep(time.Duration(s.timeoutMillis) * time.Millisecond)
		s.settleTimeoutFallback(context.Background(), orderID)
	}()
}

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
	closeVolume := p.Volume
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
