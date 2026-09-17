package service

import (
	"context"
	"fmt"
	"log"

	"github.com/shopspring/decimal"

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
// 兜底直接结算见liquidation.go的settleTimeoutFallback，两条路径都会走到这里)。判断依据是
// available+credit的合计，不是available单独判断——credit也是账户权益的一部分(见
// checkAndLiquidate的equity计算)，穿仓/缓冲的定义要跟触发强平时用的权益口径一致：
//  1. 合计为负(穿仓)：用户自己的钱和信用额度都耗尽了还倒欠钱，保险基金垫付缺口，基金不够
//     就让基金余额变负+记日志告警——MVP不做ADL，这是明确排除项，见plan文件
//  2. 这个uid已经没有剩余仓位了(这一轮强平彻底结束)、合计为正：这部分是维持保证金要求留下
//     的缓冲，不退给用户——真实交易所是按破产价结算、多出来的差价当清算费进保险基金，这里
//     不改结算价格/撮合逻辑，改成结算完直接把这部分正数余额扫进保险基金、账户清零，经济
//     结果等价
//
// 两种情况下available和credit最终都会被清零——不管available/credit各自是正是负，"结清"
// 的终态就是两个字段都变成0，保险基金拿走或垫付两者的合计净值，这里不需要区分"先清哪个"：
// 清算的是两个字段的总和，不是循环着一点点从某个字段里扣，最终状态跟顺序无关
func (e *EngineService) HandleLiquidationSettleAftermath(ctx context.Context, symbol string, uid uint64) error {
	available, err := e.accounts.FindFreshAvailable(ctx, uid)
	if err != nil {
		return err
	}
	credit, err := e.accounts.FindFreshCredit(ctx, uid)
	if err != nil {
		return err
	}
	combined := available.Add(credit)
	if combined.Sign() < 0 {
		shortfall := combined.Neg()
		if err := e.fund.Adjust(ctx, symbol, uid, 0, shortfall.Neg(), "强平穿仓垫付"); err != nil {
			return err
		}
		if err := e.accounts.SettleToAvailable(ctx, uid, available.Neg()); err != nil {
			return err
		}
		return e.accounts.SettleToCredit(ctx, uid, credit.Neg())
	}
	if combined.Sign() <= 0 {
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
	log.Printf("[WARN] 强平后账户仍有维持保证金缓冲，按清算费扫入保险基金, uid=%d, 缓冲=%s(available=%s, credit=%s)",
		uid, combined, available, credit)
	if err := e.fund.Adjust(ctx, symbol, uid, 0, combined, "全仓强平清算费(维持保证金缓冲)"); err != nil {
		return err
	}
	if err := e.accounts.SettleToAvailable(ctx, uid, available.Neg()); err != nil {
		return err
	}
	return e.accounts.SettleToCredit(ctx, uid, credit.Neg())
}

// CloseRound 用户主动结束本轮：先撤掉这个uid全部还在排队的委托(跨所有symbol)，再按各自
// symbol当前标记价立即强制平掉全部仓位，最后清算资金状态(credit清零/is_insured复位/round+1)。
// 这不是风险触发的强平，不走LiquidationService那套挂保护价排队+超时兜底——用户自己要结束
// 本轮，没必要等撮合，直接按标记价了结最快，也不需要保护价滑点缓冲。
// 跟CancelOrder/正常下单一样通过Kafka事件从contract-api路由到这里执行——撤单要摘掉
// contract-engine内存里的订单簿，contract-api那边看不到、摸不到。
func (e *EngineService) CloseRound(ctx context.Context, uid uint64) error {
	activeOrders, err := e.orders.FindActiveByUID(ctx, uid, "")
	if err != nil {
		return err
	}
	allDone := true
	for i := range activeOrders {
		if err := e.CancelOrder(ctx, &activeOrders[i]); err != nil {
			log.Printf("[ERROR] 结束本轮撤单失败, uid=%d, orderId=%d: %v", uid, activeOrders[i].OrderID, err)
			allDone = false
		}
	}

	positions, err := e.positionSvc.FindByUID(ctx, uid)
	if err != nil {
		return err
	}
	for _, p := range positions {
		if p.Volume.Sign() <= 0 {
			continue
		}
		mark, hasMark := e.markPrice.Get(ctx, p.Symbol)
		if !hasMark {
			// 缺标记价格没法公允强平，跳过——这个仓位这轮结束不掉，日志告警，人工介入
			log.Printf("[ERROR] 结束本轮强制平仓缺标记价格，跳过, uid=%d, symbol=%s", uid, p.Symbol)
			allDone = false
			continue
		}
		if err := e.forceCloseOnePosition(ctx, p, mark); err != nil {
			log.Printf("[ERROR] 结束本轮强制平仓失败, uid=%d, symbol=%s: %v", uid, p.Symbol, err)
			allDone = false
		}
	}

	// AccountService.CloseRound的前提是这个uid名下已经没有持仓/挂单——只要上面撤单/强平
	// 有任何一笔没成功，就不能清零credit/推进round，否则一个仍然持仓的uid会在credit缓冲
	// 被清空、round已经翻篇的状态下留着旧仓位，风控口径全乱。这里不清算、直接报错返回，
	// 让调用方(Kafka消费者)记录日志——运营/合作方需要在缺失条件解决后(比如标记价格恢复)
	// 重新调用一次结束本轮接口
	if !allDone {
		return fmt.Errorf("结束本轮未完全成功(还有撤单/强平失败)，uid=%d，未清算资金状态，需要重试", uid)
	}
	return e.accounts.CloseRound(ctx, uid)
}

// forceCloseOnePosition 生成一笔"已成交"的市价平仓单落库(留痕、复用SettleFill结算逻辑)，
// 立即按标记价全部结算掉——不进撮合引擎的订单簿，不用等对手盘
func (e *EngineService) forceCloseOnePosition(ctx context.Context, p model.Position, mark decimal.Decimal) error {
	now := NowMillis()
	o := &model.Order{
		OrderID:      NextID(),
		UID:          p.UID,
		Symbol:       p.Symbol,
		Side:         p.Side,
		Action:       model.ActionClose,
		Type:         model.OrderTypeMarket,
		Price:        mark,
		Amount:       p.Volume,
		TradedAmount: p.Volume,
		AvgDealPrice: mark,
		Leverage:     p.Leverage,
		ReduceOnly:   true,
		Status:       model.OrderStatusFilled,
		CreateTime:   now,
		UpdateTime:   now,
	}
	if err := e.orders.Insert(ctx, o); err != nil {
		return err
	}
	_, err := e.settlement.SettleFill(ctx, o, p.Volume, mark, false, now)
	return err
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
		// 按剩余比例分别算这笔委托冻结的available/credit部分该释放多少，不能笼统释放到
		// available——那样等于让信用额度经过"冻结再撤单"这个渠道被洗成可提现的available
		releaseAvailable, releaseCredit := o.ProportionalFrozen(remaining)
		return e.accounts.UnfreezeMargin(ctx, o.UID, releaseAvailable, releaseCredit)
	}
	return nil
}
