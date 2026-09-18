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
	matchingEngine    *matching.Engine // 撮合引擎，里面有多个orderbook，每个币种一个orderbook
	orders            *repo.OrderRepo
	conditionalOrders *repo.ConditionalOrderRepo
	trades            *repo.TradeRepo
	accounts          *AccountService
	positionSvc       *PositionService
	settlement        *SettlementService
	markPrice         *MarkPriceService
	fund              *InsuranceFundService
	kline             *KlineService
	push              *PushService
}

func NewEngineService(
	matchingEngine *matching.Engine,
	orders *repo.OrderRepo,
	conditionalOrders *repo.ConditionalOrderRepo,
	trades *repo.TradeRepo,
	accounts *AccountService,
	positionSvc *PositionService,
	settlement *SettlementService,
	markPrice *MarkPriceService,
	fund *InsuranceFundService,
	kline *KlineService,
	push *PushService,
) *EngineService {
	return &EngineService{
		matchingEngine:    matchingEngine,
		orders:            orders,
		conditionalOrders: conditionalOrders,
		trades:            trades,
		accounts:          accounts,
		positionSvc:       positionSvc,
		settlement:        settlement,
		markPrice:         markPrice,
		fund:              fund,
		kline:             kline,
		push:              push,
	}
}

// 委托已经由调用方落库(status=OPEN)——正常用户下单在contract-api那边落库+冻结保证金之后才发Kafka事件过来；
// 强平单由liquidation.go在这里落库。
// 这个方法负责真正的撮合+结算+挂簿/释放。
func (e *EngineService) SubmitOrder(ctx context.Context, order *model.Order, entryTime int64) error {
	// 防御Kafka at-least-once语义下的重复投递(internal/mq的消费者本身不做去重)：
	// ①状态已经不是"待处理"(已经被别的事件处理成filled/canceled/rejected)——不能对一笔
	// 已经终结的委托再走一遍撮合；②这个orderId已经挂在簿子上——说明上一次投递已经完整
	// 处理过(撮合+挂剩余量)了，不能让它再当一次新的taker去吃对手盘，那会造成不该发生的
	// 二次撮合。两个检查合起来只覆盖"仍在排队中"这一种重复投递场景，不是完整的Kafka
	// 幂等方案(比如"提交后又立刻被撤单，撤单先处理完，提交事件才重复投递过来"这种交叉时序
	// 依然防不住)，见docs/known-limitations.md
	if !model.ActiveOrderStatuses[order.Status] {
		log.Printf("[WARN] orderId=%d 状态已经是%s(不是待处理状态)，跳过(可能是重复的下单事件)", order.OrderID, order.Status)
		return nil
	}
	book := e.matchingEngine.BookFor(order.Symbol)
	if book.Contains(order.OrderID) {
		log.Printf("[WARN] orderId=%d 已经在订单簿里，跳过重复的下单事件", order.OrderID)
		return nil
	}

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
	fills, selfCanceled := book.Match(resting)

	// 这笔提交涉及到的全部uid，成交完毕后每个uid只推一次账户快照——不是每笔fill各推一次：
	// 一笔大额市价单可能一口气吃掉好几档、好几笔fill，taker都是同一个uid，中间几次快照
	// 都会被最后一次覆盖，没必要重复查DB+发Redis
	touchedUIDs := make(map[uint64]bool)
	for _, f := range fills {
		if err := e.settleOneFill(ctx, order, f); err != nil {
			log.Printf("[ERROR] settle fill failed, orderId=%d: %v", order.OrderID, err)
		}
		touchedUIDs[f.MakerOrder.UID] = true
		touchedUIDs[f.TakerOrder.UID] = true
	}
	e.handleSelfCanceled(ctx, selfCanceled)

	// LIMIT单还有剩余量就挂回簿子；MARKET单/剩余为0就不挂——市价单吃不满剩下的量直接释放
	// (释放动作由调用方在SubmitOrder返回后，根据委托最终状态决定要不要unfreeze剩余冻结保证金)
	rested := false
	if resting.Remaining.Sign() > 0 && order.Type == model.OrderTypeLimit {
		rested = book.Rest(resting)
	}
	// 只有订单簿真的发生了变化(成交、自成交摘单、挂进新单)才推送深度快照——一笔市价单
	// 缺流动性、什么都没吃到、也没有剩余量可挂的情况下，订单簿状态没变，不需要推送
	if len(fills) > 0 || len(selfCanceled) > 0 || rested {
		e.push.PublishDepth(ctx, order.Symbol, book.Depth(matching.DefaultDepthLevels))
	}
	// 提交者自己的账户快照必须推——哪怕这笔委托一口成交都没吃到、只是静静挂在簿子上，
	// 它的出现本身也是提交者activeOrders列表的变化，不能只在有成交时才推(那样一笔纯粹
	// 挂单不成交的委托，提交者的WS私有频道永远不会得到通知)
	touchedUIDs[order.UID] = true
	for uid := range touchedUIDs {
		e.push.PublishUserSnapshot(ctx, uid)
	}
	return nil
}

// PublishUserSnapshot/PublishDepth 暴露给LiquidationService/ConditionalOrderService这些
// 跟EngineService协作、但不直接持有PushService的调用方——推送逻辑还是收在EngineService
// 内部，不是把push字段整个导出
func (e *EngineService) PublishUserSnapshot(ctx context.Context, uid uint64) {
	e.push.PublishUserSnapshot(ctx, uid)
}

func (e *EngineService) PublishDepth(ctx context.Context, symbol string) {
	book := e.matchingEngine.BookFor(symbol)
	e.push.PublishDepth(ctx, symbol, book.Depth(matching.DefaultDepthLevels))
}

// handleSelfCanceled 处理自成交保护(STP)摘掉的maker：book.Match内部已经把它们从订单簿里
// 摘掉了，这里只需要按正常撤单的收尾逻辑处理DB状态+退保证金。用RestingOrder.Remaining
// (book.Match返回的、摘除时刻内存里权威的剩余量)，不用再去DB反查——两者理论上一致，但
// 直接用内存值更直接。一笔taker可能一次撮合摘掉好几笔自己的挂单(比如大额市价单扫过自己
// 挂的一串限价单)，批量查一次DB(FindByOrderIDs)而不是每笔单独查一次，避免N次DB往返
// 串行拖慢下单主流程
func (e *EngineService) handleSelfCanceled(ctx context.Context, selfCanceled []*matching.RestingOrder) {
	if len(selfCanceled) == 0 {
		return
	}
	ids := make([]uint64, len(selfCanceled))
	for i, c := range selfCanceled {
		ids[i] = c.OrderID
	}
	orders, err := e.orders.FindByOrderIDs(ctx, ids)
	if err != nil {
		log.Printf("[ERROR] 自成交保护撤单批量查询委托记录失败: %v", err)
		return
	}
	byID := make(map[uint64]model.Order, len(orders))
	for _, o := range orders {
		byID[o.OrderID] = o
	}
	for _, canceled := range selfCanceled {
		o, found := byID[canceled.OrderID]
		if !found {
			log.Printf("[ERROR] 自成交保护撤单找不到委托记录, orderId=%d", canceled.OrderID)
			continue
		}
		if err := e.finalizeOrderCancel(ctx, &o, canceled.Remaining); err != nil {
			log.Printf("[ERROR] 自成交保护撤单收尾失败, orderId=%d: %v", canceled.OrderID, err)
		}
	}
}

func (e *EngineService) settleOneFill(ctx context.Context, incoming *model.Order, f matching.Fill) error {
	now := NowMillis()

	if err := e.markPrice.UpdateFromTrade(ctx, incoming.Symbol, f.Price); err != nil {
		log.Printf("[WARN] update mark price failed: %v", err)
	} else {
		e.push.PublishMarkPrice(ctx, incoming.Symbol, f.Price)
	}

	tradeID := NextID()
	trade := &model.Trade{
		TradeID: tradeID, Symbol: incoming.Symbol, Price: f.Price, Volume: f.Volume,
		BuyOrderID: buyOrderID(f), SellOrderID: sellOrderID(f),
		BuyUID: buyUID(f), SellUID: sellUID(f), MakerOrderID: f.MakerOrder.OrderID, CreateTime: now,
	}
	if err := e.trades.Insert(ctx, trade); err != nil {
		return err
	}
	e.push.PublishTrade(ctx, trade)
	for _, k := range e.kline.RecordTrade(ctx, incoming.Symbol, f.Price, f.Volume, now) {
		e.push.PublishKline(ctx, incoming.Symbol, k)
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
	// 账户快照的推送统一收在SubmitOrder那个调用方的最后(每个涉及到的uid只推一次，见
	// touchedUIDs)，不在这里按每笔fill单独推——一口气吃掉好几档的大额市价单会产生好几笔
	// fill，这里不推能避免同一个uid在一次SubmitOrder调用里被重复查DB+发Redis好几次
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

	// 条件单(止盈止损/条件开仓)触发前从来没进过撮合引擎的订单簿，CancelOrder摘不到它们，
	// 必须单独撤销——不然结束本轮之后credit清零了，这些条件单开仓方向上冻结的frozen_credit
	// 还留在账上没释放，等它们后来真的触发/被撤销，会把上一轮已经作废的credit又"复活"
	// 进新一轮的余额里
	pendingConditional, err := e.conditionalOrders.FindActiveByUID(ctx, uid, "")
	if err != nil {
		return err
	}
	for _, co := range pendingConditional {
		if err := e.cancelConditionalOrder(ctx, co); err != nil {
			log.Printf("[ERROR] 结束本轮撤销条件单失败, uid=%d, orderId=%d: %v", uid, co.OrderID, err)
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
	if err := e.accounts.CloseRound(ctx, uid); err != nil {
		return err
	}
	// 撤单/强平过程中已经推送过中间状态的账户快照，这里再推一次最终状态(credit清零、
	// round+1)——中间那几次快照都还没反映"结束本轮"这个动作本身造成的变化
	e.push.PublishUserSnapshot(ctx, uid)
	return nil
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
	} else {
		// 只有真的从订单簿里摘掉了什么(ok=true)才需要推深度变化——自成交保护(handleSelfCanceled)
		// 走的是finalizeOrderCancel这条共用路径，但那种情况订单簿早在book.Match内部就已经
		// 变过了，SubmitOrder那边已经推过一次深度，这里不需要再推一次
		e.push.PublishDepth(ctx, o.Symbol, book.Depth(matching.DefaultDepthLevels))
	}
	return e.finalizeOrderCancel(ctx, o, remaining)
}

// finalizeOrderCancel 统一负责"标记DB为CANCELED+按剩余量释放冻结保证金+推送账户快照"——
// CancelOrder(正常撤单接口触发)和自成交保护(book.Match内部摘除maker，见SubmitOrder)都要
// 走到这一步，只是"从订单簿摘除"这一步各自的时机/方式不同(前者显式调book.Cancel，后者
// book.Match内部已经摘完了)，DB落库+保证金释放+推送的逻辑完全一样，不应该写两份
func (e *EngineService) finalizeOrderCancel(ctx context.Context, o *model.Order, remaining decimal.Decimal) error {
	marked, err := e.orders.MarkCanceled(ctx, o.OrderID, NowMillis())
	if err != nil {
		return err
	}
	if !marked {
		// MarkCanceled的WHERE status IN ('open','partially_filled')没匹配到行，说明这笔
		// 委托已经被别的路径终结过了(比如正常撤单和自成交保护并发撞到同一笔单子，或者
		// CloseRound用的是撤单前拍的旧快照、这笔单子已经在别处被处理完)——不能再往下走释放
		// 保证金，否则同一笔冻结会被重复释放，凭空多出一笔钱
		return nil
	}
	defer e.push.PublishUserSnapshot(ctx, o.UID)
	if o.Action == model.ActionOpen && remaining.Sign() > 0 {
		// 按剩余比例分别算这笔委托冻结的available/credit部分该释放多少，不能笼统释放到
		// available——那样等于让信用额度经过"冻结再撤单"这个渠道被洗成可提现的available
		releaseAvailable, releaseCredit := o.ProportionalFrozen(remaining)
		return e.accounts.UnfreezeMargin(ctx, o.UID, releaseAvailable, releaseCredit)
	}
	return nil
}

// cancelConditionalOrder 撤销一笔还没触发的条件单——跟router.go里contract-api那个撤销
// 接口是同一套逻辑(原子标记取消+整笔退回冻结保证金，条件单没有"部分成交"这一说)，这里
// 单独实现一份是因为CloseRound跑在contract-engine进程里，摸不到contract-api那边的Server，
// 只能直接调repo/AccountService
func (e *EngineService) cancelConditionalOrder(ctx context.Context, co model.ConditionalOrder) error {
	ok, err := e.conditionalOrders.MarkCanceled(ctx, co.OrderID, NowMillis())
	if err != nil {
		return err
	}
	if !ok {
		return nil // 跟触发扫描并发竞争、扫描赢了，这笔已经变成真正的委托，上面撤单循环会处理
	}
	if co.Action == model.ActionOpen && (co.FrozenMargin.Sign() > 0 || co.FrozenCredit.Sign() > 0) {
		return e.accounts.UnfreezeMargin(ctx, co.UID, co.FrozenMargin, co.FrozenCredit)
	}
	return nil
}
