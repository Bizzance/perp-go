// 强平/风控：账户权益(available+全部持仓未实现盈亏) 跌破全部仓位维持保证金要求之和就触发
// 强平——先挂保护价限价单排队成交，超时(LiquidationOrderTimeoutMs)没成交完再兜底按标记价
// 直接结算；结算完账户如果还剩正数余额(维持保证金缓冲)不退给用户，扫进保险基金清零，对齐
// Binance"破产价结算、盈余进保险基金"的效果；结算完是负数(穿仓)，保险基金垫付。
// 照抄这次会话对Java版LiquidationService做的全部改动，见plan文件。
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

func NewLiquidationService(engine *EngineService, orders *repo.OrderRepo, positions *repo.PositionRepo,
	positionSvc *PositionService, markPrice *MarkPriceService, accounts *AccountService,
	fund *InsuranceFundService, timeoutMillis int64) *LiquidationService {
	return &LiquidationService{
		engine: engine, orders: orders, positions: positions, positionSvc: positionSvc,
		markPrice: markPrice, accounts: accounts, fund: fund, timeoutMillis: timeoutMillis,
	}
}

// RiskScanOnce 全部有仓位的账户扫一遍——照抄Java版scanAndLiquidateCrossAccounts
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
	maintTotal, positions, ok, err := s.positionSvc.MaintenanceMarginTotal(ctx, uid)
	if err != nil || !ok || len(positions) == 0 {
		return err
	}
	available, err := s.accounts.FindFreshAvailable(ctx, uid)
	if err != nil {
		return err
	}
	totalUnrealized, err := s.positionSvc.TotalUnrealizedPnl(ctx, uid)
	if err != nil {
		return err
	}
	equity := available.Add(totalUnrealized)
	if equity.GreaterThan(maintTotal) {
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
	log.Printf("[WARN] 触发全仓联合强平, uid=%d, 账户权益=%s, 维持保证金要求=%s, 仓位数=%d", uid, equity, maintTotal, len(pending))
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
	mark, hasMark := s.markPrice.Get(ctx, p.Symbol)
	if !hasMark {
		s.positions.ClearLiquidating(ctx, p.ID)
		return
	}
	tier, err := s.positionSvc.TierFor(ctx, p.Symbol, p.Volume.Mul(mark))
	if err != nil || tier == nil {
		s.positions.ClearLiquidating(ctx, p.ID)
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
		OrderID: orderID, UID: p.UID, Symbol: p.Symbol, Side: p.Side, Action: model.ActionClose,
		Type: model.OrderTypeLimit, Price: protectPrice, Amount: p.Volume, Leverage: p.Leverage,
		ReduceOnly: true, Liquidation: true, Status: model.OrderStatusNew, CreateTime: now, UpdateTime: now,
	}
	log.Printf("[WARN] 触发强平-挂单排队, uid=%d, symbol=%s, side=%s, volume=%s, markPrice=%s, 保护价=%s",
		p.UID, p.Symbol, p.Side, p.Volume, mark, protectPrice)
	if err := s.orders.Insert(ctx, o); err != nil {
		log.Printf("[ERROR] insert liquidation order failed: %v", err)
		s.positions.ClearLiquidating(ctx, p.ID)
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
	book.Cancel(o.OrderID) // 摘掉簿子上剩余的部分，避免超时兜底之后又意外撮合成交

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
	log.Printf("[WARN] 触发强平超时兜底直接结算, uid=%d, symbol=%s, side=%s, volume=%s, markPrice=%s", o.UID, o.Symbol, o.Side, closeVolume, mark)

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
}
