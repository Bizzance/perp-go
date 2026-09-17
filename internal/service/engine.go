package service

import (
	"context"
	"log"

	"perp-go/internal/matching"
	"perp-go/internal/model"
	"perp-go/internal/repo"
)

type EngineService struct {
	matchingEngine *matching.Engine // 撮合引擎，里面有多个orderbook，每个币种一个orderbook
	orders         *repo.OrderRepo
	trades         *repo.TradeRepo
	accounts       *AccountService
	positionSvc    *PositionService
	settlement     *SettlementService
	markPrice      *MarkPriceService
	fund           *InsuranceFundService
}

func NewEngineService(
	matchingEngine *matching.Engine,
	orders *repo.OrderRepo,
	trades *repo.TradeRepo,
	accounts *AccountService,
	positionSvc *PositionService,
	settlement *SettlementService,
	markPrice *MarkPriceService,
	fund *InsuranceFundService,
) *EngineService {
	return &EngineService{
		matchingEngine: matchingEngine,
		orders:         orders,
		trades:         trades,
		accounts:       accounts,
		positionSvc:    positionSvc,
		settlement:     settlement,
		markPrice:      markPrice,
		fund:           fund,
	}
}

// 委托已经由调用方落库(status=OPEN)——正常用户下单在contract-api那边落库+冻结保证金之后才发Kafka事件过来；
// 强平单由liquidation.go在这里落库。
// 这个方法负责真正的撮合+结算+挂簿/释放。
func (e *EngineService) SubmitOrder(ctx context.Context, order *model.Order, entryTime int64) error {
	// 进入订单簿的order，只包含了下单的order中的一部分必要数据
	resting := &matching.RestingOrder{
		OrderID:     order.OrderID,
		UID:         order.UID,
		Side:        order.Side,
		Action:      order.Action,
		Direction:   matching.DirectionOf(order.Side, order.Action),
		Price:       order.Price,
		Remaining:   order.RemainingAmount(),
		EntryTime:   entryTime,
		ReduceOnly:  order.ReduceOnly,
		Liquidation: order.Liquidation,
	}
	// 拿到订单对应的订单簿
	book := e.matchingEngine.BookFor(order.Symbol)
	fills := book.Match(resting)

	for _, f := range fills {
		if err := e.settleOneFill(ctx, order, f); err != nil {
			log.Printf("[ERROR] settle fill failed, orderId=%d: %v", order.OrderID, err)
		}
	}

	// LIMIT单还有剩余量就挂回簿子；MARKET单/剩余为0就不挂——市价单吃不满剩下的量直接释放
	// (释放动作由调用方在SubmitOrder返回后，根据委托最终状态决定要不要unfreeze剩余冻结保证金)
	if resting.Remaining.Sign() > 0 && order.Type == model.OrderTypeLimit {
		book.Rest(resting)
	}
	return nil
}

func (e *EngineService) settleOneFill(ctx context.Context, incoming *model.Order, f matching.Fill) error {
	now := NowMillis()

	if err := e.markPrice.UpdateFromTrade(ctx, incoming.Symbol, f.Price); err != nil {
		log.Printf("[WARN] update mark price failed: %v", err)
	}

	tradeID := NextID()
	if err := e.trades.Insert(ctx, &model.Trade{
		TradeID: tradeID, Symbol: incoming.Symbol, Price: f.Price, Volume: f.Volume,
		BuyOrderID: buyOrderID(f), SellOrderID: sellOrderID(f),
		BuyUID: buyUID(f), SellUID: sellUID(f), MakerOrderID: f.MakerOrder.OrderID, CreateTime: now,
	}); err != nil {
		return err
	}

	for _, side := range []struct {
		orderID     uint64
		uid         uint64
		side        model.Side
		action      model.OrderAction
		isMaker     bool
		liquidation bool
	}{
		{f.MakerOrder.OrderID, f.MakerOrder.UID, f.MakerOrder.Side, f.MakerOrder.Action, true, f.MakerOrder.Liquidation},
		{f.TakerOrder.OrderID, f.TakerOrder.UID, f.TakerOrder.Side, f.TakerOrder.Action, false, f.TakerOrder.Liquidation},
	} {
		order, err := e.orders.FindByOrderID(ctx, side.orderID)
		if err != nil || order == nil {
			log.Printf("[ERROR] order not found while settling fill, orderId=%d", side.orderID)
			continue
		}
		newStatus := model.OrderStatusPartiallyFilled
		if order.TradedAmount.Add(f.Volume).GreaterThanOrEqual(order.Amount) {
			newStatus = model.OrderStatusFilled
		}
		if err := e.orders.ApplyFill(ctx, side.orderID, f.Volume, f.Price, newStatus, now); err != nil {
			return err
		}
		if _, err := e.settlement.SettleFill(ctx, order, f.Volume, f.Price, side.isMaker, now); err != nil {
			return err
		}
		if side.liquidation {
			if err := e.HandleLiquidationSettleAftermath(ctx, order.Symbol, side.uid); err != nil {
				log.Printf("[ERROR] liquidation aftermath failed, uid=%d: %v", side.uid, err)
			}
		}
	}
	return nil
}

func buyOrderID(f matching.Fill) uint64 {
	if f.MakerOrder.Direction == matching.Buy {
		return f.MakerOrder.OrderID
	}
	return f.TakerOrder.OrderID
}
func sellOrderID(f matching.Fill) uint64 {
	if f.MakerOrder.Direction == matching.Sell {
		return f.MakerOrder.OrderID
	}
	return f.TakerOrder.OrderID
}
func buyUID(f matching.Fill) uint64 {
	if f.MakerOrder.Direction == matching.Buy {
		return f.MakerOrder.UID
	}
	return f.TakerOrder.UID
}
func sellUID(f matching.Fill) uint64 {
	if f.MakerOrder.Direction == matching.Sell {
		return f.MakerOrder.UID
	}
	return f.TakerOrder.UID
}

// HandleLiquidationSettleAftermath 强平结算之后调用(挂单排队正常成交见settleOneFill、超时
// 兜底直接结算见liquidation.go的settleTimeoutFallback，两条路径都会走到这里)：
//  1. 账户可用余额被打成负数(穿仓)：保险基金垫付，基金不够就让基金余额变负+记日志告警——
//     MVP不做ADL，这是明确排除项，见plan文件
//  2. 这个uid已经没有剩余仓位了(这一轮强平彻底结束)、账户还剩正数余额：这部分是维持保证金
//     要求留下的缓冲，不退给用户——真实交易所是按破产价结算、多出来的差价当清算费进保险基金，
//     这里不改结算价格/撮合逻辑，改成结算完直接把这部分正数余额扫进保险基金、账户清零，
//     经济结果等价
func (e *EngineService) HandleLiquidationSettleAftermath(ctx context.Context, symbol string, uid uint64) error {
	available, err := e.accounts.FindFreshAvailable(ctx, uid)
	if err != nil {
		return err
	}
	if available.Sign() < 0 {
		shortfall := available.Neg()
		if err := e.fund.Adjust(ctx, symbol, uid, 0, shortfall.Neg(), "强平穿仓垫付"); err != nil {
			return err
		}
		return e.accounts.SettleToAvailable(ctx, uid, shortfall)
	}
	if available.Sign() <= 0 {
		return nil
	}
	positions, err := e.positionSvc.FindByUID(ctx, uid)
	if err != nil {
		return err
	}
	for _, p := range positions {
		if p.Volume.Sign() > 0 {
			return nil // 这个uid还有别的仓位没平完，不是这轮强平的最终状态，先不清算
		}
	}
	log.Printf("[WARN] 强平后账户仍有维持保证金缓冲，按清算费扫入保险基金, uid=%d, 缓冲=%s", uid, available)
	if err := e.fund.Adjust(ctx, symbol, uid, 0, available, "全仓强平清算费(维持保证金缓冲)"); err != nil {
		return err
	}
	return e.accounts.SettleToAvailable(ctx, uid, available.Neg())
}

// CancelOrder 从订单簿摘掉委托、退回剩余冻结保证金、落库改CANCELED
func (e *EngineService) CancelOrder(ctx context.Context, o *model.Order) error {
	book := e.matchingEngine.BookFor(o.Symbol)
	remaining, ok := book.Cancel(o.OrderID)
	if !ok {
		remaining = o.RemainingAmount()
	}
	if _, err := e.orders.MarkCanceled(ctx, o.OrderID, NowMillis()); err != nil {
		return err
	}
	if o.Action == model.ActionOpen && remaining.Sign() > 0 {
		releaseMargin := o.FrozenMargin.Mul(remaining).Div(o.Amount)
		return e.accounts.UnfreezeMargin(ctx, o.UID, releaseMargin)
	}
	return nil
}
