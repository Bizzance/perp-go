package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/matching"
	"perp-go/internal/model"
	"perp-go/internal/repo"
)

type EngineService struct {
	matchingEngine     *matching.Engine // 撮合引擎，里面有多个orderbook，每个币种一个orderbook
	orders             *repo.OrderRepo
	conditionalOrders  *repo.ConditionalOrderRepo
	trades             *repo.TradeRepo
	accounts           *AccountService
	positionSvc        *PositionService
	settlement         *SettlementService
	markPrice          *MarkPriceService
	fund               *InsuranceFundService
	kline              *KlineService
	push               *PushService
	roundCloseProgress *repo.RoundCloseProgressRepo
	lock               *LockService
	coins              *repo.CoinRepo  // 市价单要查合约的价格保护带
	ownedSymbols       map[string]bool // nil=负责全部symbol(单实例默认)，见docs/engine-sharding.md
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
	roundCloseProgress *repo.RoundCloseProgressRepo,
	lock *LockService,
	coins *repo.CoinRepo,
	engineSymbols []string,
) *EngineService {
	var owned map[string]bool
	if len(engineSymbols) > 0 {
		owned = make(map[string]bool, len(engineSymbols))
		for _, s := range engineSymbols {
			owned[s] = true
		}
	}
	markPrice.OnChange(func(ctx context.Context, symbol string, mark decimal.Decimal) {
		push.PublishMarkPrice(ctx, symbol, mark)
	})
	return &EngineService{
		matchingEngine:     matchingEngine,
		orders:             orders,
		conditionalOrders:  conditionalOrders,
		trades:             trades,
		accounts:           accounts,
		positionSvc:        positionSvc,
		settlement:         settlement,
		markPrice:          markPrice,
		fund:               fund,
		kline:              kline,
		push:               push,
		roundCloseProgress: roundCloseProgress,
		lock:               lock,
		coins:              coins,
		ownedSymbols:       owned,
	}
}

// 这个engine实例是不是负责撮合这个symbol——单实例部署(ownedSymbols为nil)下
// 恒为true，分片部署下只有配置在PERP_ENGINE_SYMBOLS里的symbol才返回true。任何会碰
// e.matchingEngine里某个symbol真实订单簿的操作，在处理前都必须先过这道检查——见
// docs/engine-sharding.md，误判会导致撤单/强平这类操作在一个从来没有真实挂单的本地
// 空订单簿上"假装成功"，跟另一个真正持有这个symbol订单簿的实例的状态对不上
func (e *EngineService) OwnsSymbol(symbol string) bool {
	if e.ownedSymbols == nil {
		return true
	}
	return e.ownedSymbols[symbol]
}

// 定时刷新这个实例负责的每个合约的标记价：用当前订单簿的买一卖一采一个基差样本，再按最新的指数价重算。
// 成交只在有人交易时才发生，指数价变了而没有成交的时候，标记价全靠这个跟上
func (e *EngineService) RefreshMarkPrices(ctx context.Context) {
	coins, err := e.coins.FindAllEnabled(ctx)
	if err != nil {
		log.Printf("[ERROR] 刷新标记价: 查询合约列表失败: %v", err)
		return
	}
	for _, coin := range coins {
		if !e.OwnsSymbol(coin.Symbol) {
			continue
		}
		var bid, ask decimal.Decimal
		depth := e.matchingEngine.BookFor(coin.Symbol).Depth(1)
		if len(depth.Bids) > 0 {
			bid = depth.Bids[0].Price
		}
		if len(depth.Asks) > 0 {
			ask = depth.Asks[0].Price
		}
		if err := e.markPrice.Refresh(ctx, coin.Symbol, bid, ask); err != nil {
			log.Printf("[WARN] 刷新标记价失败, symbol=%s: %v", coin.Symbol, err)
		}
	}
}

// 进程启动时重建内存订单簿——订单簿(matching.Book)是纯内存结构，
// contract-engine重启会丢失全部挂单排队状态，但委托记录本身已经落库，status还是
// open/partially_filled就说明这笔委托还没有走完，剩余量就是RemainingAmount()。
// 按create_time(+order_id兜底同一毫秒内的相对顺序)升序依次重放进撮合，见
// docs/order-book-recovery.md。
//
// 重放走的是正常下单那条"先Match再Rest"的路径(submitOrder，纯挂单不推送)，而不是直接Rest：
// 落库的活跃委托里，除了重启前已经处理过、静静挂在簿子上的，还有落库了但引擎还没处理过的
// (引擎宕机或落后期间，下单接口已经落库、事件还堆在Kafka里)，两种在数据库里长得一模一样。
// 直接Rest会让后一种交叉地挂在簿子上，随后Kafka里它们的下单事件又被当成重复跳过，永远不成交。
// 重放对前一种是安全的：一组互不相交的挂单，按任何顺序重放都不会产生成交。
//
// EntryTime换算成跟运行时time.Now().UnixNano()同一量纲(都是纳秒)，不是直接拿
// 毫秒级的create_time数值当纳秒用——否则恢复的挂单在数值上会比重启后新提交的委托小
// (量纲不一致导致的巧合而已，不能依赖这种巧合)，这里显式换算保证恢复的挂单在时间优先级上
// 确实排在重启后新提交的委托之前，符合它们本来就更早进入订单簿的事实
func (e *EngineService) RecoverOrderBook(ctx context.Context) error {
	orders, err := e.orders.FindActiveLimitOrders(ctx)
	if err != nil {
		return fmt.Errorf("查询待恢复委托失败: %w", err)
	}
	restored, healed := 0, 0
	for i := range orders {
		o := &orders[i]
		if !e.OwnsSymbol(o.Symbol) {
			// 分片部署下，这个symbol的真实订单簿在另一个实例里，这里恢复了也只是个永远
			// 用不到、跟真实状态失联的本地副本，白占内存还可能误导查询到这个实例的/depth，
			// 见docs/engine-sharding.md
			continue
		}
		if o.RemainingAmount().Sign() <= 0 {
			// 理论上不会出现(open/partially_filled不该有remaining<=0)，防御性跳过而不是
			// 直接panic——恢复流程本身不该因为一笔脏数据整体失败，见下面的日志
			log.Printf("[WARN] 恢复订单簿时orderId=%d剩余量%s<=0(不应该发生)，跳过", o.OrderID, o.RemainingAmount())
			continue
		}
		// 按create_time顺序把每笔活跃委托重放进撮合(先Match再Rest，就是正常下单走的那条路径)，
		// 而不是直接Rest。落库的活跃委托里有两种：重启前已经处理过、静静挂在簿子上的；落库了但
		// 引擎还没处理过的(引擎宕机或落后期间，下单接口已经落库、事件还堆在Kafka里)。这两种在
		// 数据库里长得一模一样，直接Rest会让第二种交叉地挂在簿子上，随后Kafka里它们的下单
		// 事件又被当成重复跳过，永远不成交。重放进Match对第一种是安全的——一组互不相交的挂单
		// 不管按什么顺序重放都不会产生成交，见matching.TestBook_ReplayUncrossedSetThroughMatch；
		// 对第二种正好补上它们本该有的撮合。见docs/order-book-recovery.md
		matched, err := e.submitOrder(ctx, o, o.CreateTime*int64(time.Millisecond), true)
		if err != nil {
			return fmt.Errorf("恢复订单簿重放orderId=%d失败: %w", o.OrderID, err)
		}
		if matched {
			healed++
			log.Printf("[WARN] 恢复订单簿时orderId=%d重放产生了成交(落库后引擎没来得及处理的委托，已补上撮合)", o.OrderID)
		}
		restored++
	}
	log.Printf("订单簿重建完成，重放%d笔活跃委托(共查到%d笔待恢复)，其中%d笔重放时补上了撮合", restored, len(orders), healed)
	return nil
}

// 委托已经由调用方落库(status=OPEN)——正常用户下单在contract-api那边落库+冻结保证金之后才发Kafka事件过来；
// 强平单由liquidation.go在这里落库。
// 这个方法负责真正的撮合+结算+挂簿/释放。
func (e *EngineService) SubmitOrder(ctx context.Context, order *model.Order, entryTime int64) error {
	_, err := e.submitOrder(ctx, order, entryTime, false)
	return err
}

// SubmitOrder的实际实现，多返回一个matched表示这次有没有产生成交/自成交摘单。recovering=true是
// 进程启动恢复订单簿时的重放：纯挂单(没有成交)时不推送深度和账户快照——恢复一个有几万笔挂单
// 的订单簿，每笔都推一次会变成几万次Redis发布加几万次数据库查询，而这些挂单在重启前早就
// 推送过了，客户端本来就知道；只有真的产生了成交(重放帮"落库了但引擎还没处理过"的订单补上了
// 撮合)才需要推送
func (e *EngineService) submitOrder(ctx context.Context, order *model.Order, entryTime int64, recovering bool) (matched bool, err error) {
	// 分片部署下，这个symbol可能根本不归这个实例负责——e.matchingEngine.BookFor(order.Symbol)
	// 只会拿到一个从来没有真实挂单的本地空订单簿，绝不能在上面跑真正的撮合，那会把这笔
	// 委托错误地判定成"没有对手盘"，而真正拥有这个symbol订单簿的实例会独立收到同一条
	// (fan-out)事件、正常处理，这里必须直接跳过，不碰book也不做任何DB变更。见
	// docs/engine-sharding.md
	if !e.OwnsSymbol(order.Symbol) {
		return false, nil
	}
	// 防御Kafka at-least-once语义下的重复投递(internal/mq的消费者本身不做去重)：
	// ①状态已经不是"待处理"(已经被别的事件处理成filled/canceled/rejected)——不能对一笔
	// 已经终结的委托再走一遍撮合；②这个orderId已经挂在簿子上——说明上一次投递已经完整
	// 处理过(撮合+挂剩余量)了，不能让它再当一次新的taker去吃对手盘，那会造成不该发生的
	// 二次撮合。两个检查合起来只覆盖"仍在排队中"这一种重复投递场景，不是完整的Kafka
	// 幂等方案(比如"提交后又立刻被撤单，撤单先处理完，提交事件才重复投递过来"这种交叉时序
	// 依然防不住)，见docs/known-limitations.md
	if !model.ActiveOrderStatuses[order.Status] {
		log.Printf("[WARN] orderId=%d 状态已经是%s(不是待处理状态)，跳过(可能是重复的下单事件)", order.OrderID, order.Status)
		return false, nil
	}
	book := e.matchingEngine.BookFor(order.Symbol)
	if book.Contains(order.OrderID) {
		log.Printf("[WARN] orderId=%d 已经在订单簿里，跳过重复的下单事件", order.OrderID)
		return false, nil
	}

	// 冻结账户不能再新增风险：开仓委托(强平单不算，那是系统降风险的动作)在撮合前直接撤掉、
	// 退回冻结的保证金。API层下单时已经拦过一遍，这里是兜底——冻结前已经落库、还在Kafka里排队
	// 的委托，条件单触发后新落地的开仓委托，以及重启恢复时重放的开仓委托，都会走到这里。
	// 查询出错就返回错误、不往下撮合(失败关闭)：不能因为查不出账户状态就让冻结账户的开仓单成交。
	// 代价是这笔委托在数据库里还是活跃状态、却没进订单簿：Kafka消费者对处理失败的消息只记日志、
	// 不重试，要等下次引擎重启恢复订单簿时才会被重新处理(用户在这之前可以撤单，撤单不依赖订单簿里有没有它)，
	// 跟提交路径上其它数据库出错的处理方式一致
	if order.Action == model.ActionOpen && !order.Liquidation {
		frozen, err := e.accounts.IsFrozen(ctx, order.UID)
		if err != nil {
			return false, fmt.Errorf("查询账户冻结状态失败, uid=%d: %w", order.UID, err)
		}
		if frozen {
			log.Printf("[INFO] uid=%d 账户已冻结，撤销开仓委托 orderId=%d", order.UID, order.OrderID)
			if _, err := e.cancelAndReleaseMargin(ctx, order, order.RemainingAmount()); err != nil {
				return false, fmt.Errorf("撤销冻结账户的开仓委托失败, orderId=%d: %w", order.OrderID, err)
			}
			// 恢复时也要推：这是账户的挂单真的少了一笔，不是"重启前早就推送过"的纯挂单重放，
			// 而且只有冻结账户的残留委托才会走到这里，数量很少，不会有恢复大订单簿时的推送风暴
			e.push.PublishUserSnapshot(ctx, order.UID)
			return false, nil
		}
	}

	// 市价单的撮合价。库里存的Price是contract-api下市价单时用标记价估算保证金和数量的参考价，不是限价：
	// 原样当限价传给撮合的话，市价单就变成"按标记价成交的限价单"，市价买只能吃低于等于标记价的卖单、
	// 市价卖只能吃高于等于标记价的买单，而盘口对手价几乎总是在标记价的另一侧，市价单基本一笔都成交不了
	// (页面第一次点市价平仓就暴露了)。所以要按真实对手价一档档吃，成交后按真实成交额多退少补保证金。
	// 但不能完全不设限：价格保护带只校验限价开仓单，市价单不设限的话，共谋账户在远离标记价处挂一笔平仓
	// 限价单(平仓单不校验价格带)，受害者的市价买单就会一路吃过去，保证金按标记价冻结、实际按远高于标记价
	// 的成交额结算，钱流向挂单的共谋账户。所以市价单的撮合价设成参考价±保护带比例，跟限价开仓单受同一个
	// 约束，超出的部分当作没有流动性，剩余量撤销
	matchPrice := order.Price
	if order.Type == model.OrderTypeMarket {
		limit, err := e.marketOrderPriceLimit(ctx, order)
		if err != nil {
			return false, fmt.Errorf("查询合约价格保护带失败, orderId=%d: %w", order.OrderID, err) // 失败关闭，不放开价格限制去撮合
		}
		matchPrice = limit
	}

	// 进入订单簿的order，只包含了下单的order中的一部分必要数据
	resting := &matching.RestingOrder{
		OrderID:     order.OrderID,
		UID:         order.UID,
		Side:        order.Side,
		Action:      order.Action,
		Direction:   matching.DirectionOf(order.Side, order.Action),
		Price:       matchPrice,
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

	// LIMIT单还有剩余量就挂回簿子；MARKET单不挂——市价单本来就不该排队等待，缺流动性/
	// 吃不满剩下的部分直接终结成CANCELED、按比例释放冻结保证金，不然这笔委托会永久停在
	// open/partially_filled状态、冻结的保证金永远要不回来(早期实现的遗留问题，实测
	// 确认过，见docs/known-limitations.md)
	rested := false
	if resting.Remaining.Sign() > 0 {
		if order.Type == model.OrderTypeLimit {
			rested = book.Rest(resting)
		} else if _, err := e.cancelAndReleaseMargin(ctx, order, resting.Remaining); err != nil {
			log.Printf("[ERROR] 终结MARKET单未成交剩余部分失败, orderId=%d: %v", order.OrderID, err)
		}
	}
	// 只有订单簿真的发生了变化(成交、自成交摘单、挂进新单)才推送深度快照——一笔市价单
	// 缺流动性、什么都没吃到、也没有剩余量可挂的情况下，订单簿状态没变，不需要推送
	matched = len(fills) > 0 || len(selfCanceled) > 0
	if recovering && !matched {
		return false, nil // 恢复时的纯挂单：见submitOrder上面的说明，不推送
	}
	if matched || rested {
		e.push.PublishDepth(ctx, order.Symbol, book.Depth(matching.DefaultDepthLevels))
	}
	// 提交者自己的账户快照必须推——哪怕这笔委托一口成交都没吃到、只是静静挂在簿子上，
	// 它的出现本身也是提交者activeOrders列表的变化，不能只在有成交时才推(那样一笔纯粹
	// 挂单不成交的委托，提交者的WS私有频道永远不会得到通知)
	touchedUIDs[order.UID] = true
	for uid := range touchedUIDs {
		e.push.PublishUserSnapshot(ctx, uid)
	}
	return matched, nil
}

// /PublishDepth 暴露给LiquidationService/ConditionalOrderService这些
// 跟EngineService协作、但不直接持有PushService的调用方——推送逻辑还是收在EngineService
// 内部，不是把push字段整个导出
func (e *EngineService) PublishUserSnapshot(ctx context.Context, uid uint64) {
	e.push.PublishUserSnapshot(ctx, uid)
}

func (e *EngineService) PublishDepth(ctx context.Context, symbol string) {
	book := e.matchingEngine.BookFor(symbol)
	e.push.PublishDepth(ctx, symbol, book.Depth(matching.DefaultDepthLevels))
}

// 暴露给ConditionalOrderService用——条件单触发后落地成真正委托失败时
// (见conditional_order.go的trigger)要把触发前冻结的保证金退回去，跟cancelConditionalOrder
// 释放冻结保证金是同一个操作，只是调用方所在的service不直接持有AccountService
func (e *EngineService) UnfreezeMargin(ctx context.Context, uid uint64, availableAmount, creditAmount decimal.Decimal) error {
	return e.accounts.UnfreezeMargin(ctx, uid, availableAmount, creditAmount)
}

// 处理自成交保护(STP)摘掉的maker：book.Match内部已经把它们从订单簿里
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

	// 成交价只是标记价的输入之一，标记价变了才会推送(推送的是算出来的标记价，不是这笔成交价)，见MarkPriceService
	if err := e.markPrice.UpdateFromTrade(ctx, incoming.Symbol, f.Price); err != nil {
		log.Printf("[WARN] update mark price failed: %v", err)
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
		// maker/taker两边分开处理，一方结算失败只记日志、跳过它继续处理另一方，不能直接
		// return——这笔成交在Book.Match阶段已经在内存里生效、trade记录也已经落库了，如果
		// 这里maker结算失败就直接return，taker那一方会连尝试的机会都没有，变成"记了一笔
		// 交易但只有一边真的结算了"的经济不对称状态，比"两边都失败"更难排查。跟
		// funding.go的settlePositions"单个仓位失败只记日志、不中断其它仓位"是同一个取舍
		if err := e.orders.ApplyFill(ctx, side.orderID, f.Volume, f.Price, newStatus, now); err != nil {
			log.Printf("[ERROR] apply fill failed, orderId=%d: %v", side.orderID, err)
			continue
		}
		if _, err := e.settlement.SettleFill(ctx, order, f.Volume, f.Price, side.isMaker, now); err != nil {
			log.Printf("[ERROR] settle fill failed, orderId=%d: %v", side.orderID, err)
			continue
		}
		if side.liquidation {
			if err := e.HandleLiquidationSettleAftermath(ctx, order.Symbol, side.uid, side.side); err != nil {
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

// 市价单撮合时的价格上下限：参考价(下单时的标记价)买入向上、卖出向下各放宽价格保护带的比例，见
// submitOrder里的说明。合约没配保护带(比例<=0)就返回0，撮合里价格0表示不设限
func (e *EngineService) marketOrderPriceLimit(ctx context.Context, order *model.Order) (decimal.Decimal, error) {
	coin, err := e.coins.FindBySymbol(ctx, order.Symbol)
	if err != nil {
		return decimal.Zero, err
	}
	if coin == nil || coin.PriceProtectionRatio.Sign() <= 0 || order.Price.Sign() <= 0 {
		return decimal.Zero, nil
	}
	if matching.DirectionOf(order.Side, order.Action) == matching.Buy {
		return order.Price.Mul(decimal.NewFromInt(1).Add(coin.PriceProtectionRatio)), nil
	}
	return order.Price.Mul(decimal.NewFromInt(1).Sub(coin.PriceProtectionRatio)), nil
}

// 强平结算之后调用(挂单排队正常成交见settleOneFill、超时
// 兜底直接结算见liquidation.go的settleTimeoutFallback，两条路径都会走到这里)。side是刚
// 被强平的这个仓位的方向，穿仓分支触发ADL时要用来定位"该向哪个方向的持仓者强制减仓"（跟
// 被强平方向相反——比如多头被强平是因为价格下跌亏钱，跟这次价格下跌方向相反、真正因为
// 这次下跌赚钱的是空头，ADL该找空头里最赚钱的仓位，不是随便找）。判断依据是available+
// credit的合计，不是available单独判断——credit也是账户权益的一部分(见Equity)。走到判断的时候
// 这个uid已经没有仓位了，仓位保证金是0，所以available+credit就是账户权益，跟触发强平时用的
// 权益口径一致：
//  0. 前提：这个uid名下已经没有剩余仓位(见函数体里的说明)，还有仓位没平完就什么都不做
//  1. 合计为负(穿仓)：用户自己的钱和信用额度都耗尽了还倒欠钱。保险基金余额不够覆盖这笔
//     缺口时，先用ADL(runADL，见adl.go)强制减仓对手方最赚钱的仓位补一部分，补不满剩下的
//     仍然由基金硬扛(基金余额可能因此变得更负)，这是明确接受的取舍，不是ADL的失败——极端
//     行情下没有足够的反向盈利仓位可以减，基金兜底是最后一道防线
//  2. 合计为正：这部分是维持保证金要求留下
//     的缓冲，不退给用户——真实交易所是按破产价结算、多出来的差价当清算费进保险基金，这里
//     不改结算价格/撮合逻辑，改成结算完直接把这部分正数余额扫进保险基金、账户清零，经济
//     结果等价
//
// 两种情况下available和credit最终都会被清零——不管available/credit各自是正是负，"结清"
// 的终态就是两个字段都变成0，保险基金拿走或垫付两者的合计净值，这里不需要区分"先清哪个"：
// 清算的是两个字段的总和，不是循环着一点点从某个字段里扣，最终状态跟顺序无关
//
// 同一个uid的这个结算在锁里串行执行：一个uid的多个仓位是各自异步平仓的，两个仓位几乎同时平完时
// 会同时进到这里。不串行的话，两边可能读到同一份"缺口"各自垫付一遍，或者一边读余额之后另一边
// 才把保证金退回账户、拿着过期的余额去结算
func (e *EngineService) HandleLiquidationSettleAftermath(ctx context.Context, symbol string, uid uint64, side model.Side) error {
	err := e.lock.WithLock(ctx, fmt.Sprintf("perpgo:lock:liqsettle:%d", uid), func() error {
		return e.settleLiquidationAftermath(ctx, symbol, uid, side)
	})
	if errors.Is(err, ErrLockBusy) {
		// 拿不到锁说明另一个仓位的结算正在这里长时间持锁——只有全部仓位都平完了才会走到耗时的
		// 结清/ADL那一步，所以持锁的那个正在处理的就是这个uid的最终结算，这次不用重复做
		log.Printf("[WARN] 强平后资金结算抢锁失败(大概率是这个uid另一个仓位的结算正在处理), uid=%d", uid)
		return nil
	}
	return err
}

func (e *EngineService) settleLiquidationAftermath(ctx context.Context, symbol string, uid uint64, side model.Side) error {
	// 先确认没有剩余仓位，再读余额：反过来的话，读到余额之后别的仓位才平完、保证金才退回来，
	// 拿到的就是过期的余额。两种结局都要等这个uid已经没有剩余仓位了(这一轮强平彻底结束)才判断：
	// 全仓强平是这个uid名下全部仓位各自异步平仓，先平完的那个仓位结算之后，free balance可能
	// 因为它的亏损暂时为负，但别的仓位占用的保证金还锁在frozen_margin里、平仓时会解锁——这时候
	// 就让基金垫付，后面的仓位平完保证金解锁，垫付就成了多余的一进一出，还可能不必要地触发ADL去
	// 强减无辜的对手方。free balance为负本来就是全仓下的合法状态，留着不动，由最后一个平完的
	// 仓位来结清；如果剩下的仓位没有被强平掉，账户权益低于维持保证金的话下一轮风控扫描会继续强平它们
	positions, err := e.positionSvc.FindByUID(ctx, uid)
	if err != nil {
		return err
	}
	for _, p := range positions {
		if p.Volume.Sign() > 0 {
			return nil
		}
	}
	// 兜底：强平触发时已经撤过一遍挂单，但强平窗口期里用户可能又挂了新单。这些单锁定的保证金
	// 不在下面的free balance+free credit里，不撤的话结算时被漏算：free balance为负时基金照常
	// 垫付，之后撤单把锁定的保证金解锁，用户拿到的比真实权益多。撤单失败已经记了ERROR日志，这里
	// 不中断结算——中断的话账户会停在"没有仓位、free balance为负"的状态，没有任何后续动作会
	// 再来结清它
	e.CancelAllPendingOrders(ctx, uid)
	// 走到这里所有仓位/挂单都已经清空，frozen_margin/frozen_credit理论上应该是0，free
	// balance/free credit等于balance/credit本身；仍然按free值算，防御万一还有残留的锁定
	freeBalance, freeCredit, err := e.accounts.FindFreshFreeMargin(ctx, uid)
	if err != nil {
		return err
	}
	combined := freeBalance.Add(freeCredit)
	if combined.Sign() == 0 {
		return nil
	}
	if combined.Sign() < 0 {
		shortfall := combined.Neg()
		fundBalance, err := e.fund.FreshBalance(ctx)
		if err != nil {
			return err
		}
		if usable := decimal.Max(fundBalance, decimal.Zero); shortfall.GreaterThan(usable) {
			if raised := e.runADL(ctx, symbol, side.Opposite(), shortfall.Sub(usable)); raised.Sign() > 0 {
				log.Printf("[INFO] ADL补充保险基金, symbol=%s, 触发uid=%d, 筹到=%s", symbol, uid, raised)
			}
		}
		if err := e.fund.Adjust(ctx, symbol, uid, 0, shortfall.Neg(), "强平穿仓垫付"); err != nil {
			return err
		}
		if err := e.accounts.SettleToBalance(ctx, uid, freeBalance.Neg()); err != nil {
			return err
		}
		return e.accounts.SettleToCredit(ctx, uid, freeCredit.Neg())
	}
	log.Printf("[WARN] 强平后账户仍有维持保证金缓冲，按清算费扫入保险基金, uid=%d, 缓冲=%s(balance=%s, credit=%s)",
		uid, combined, freeBalance, freeCredit)
	if err := e.fund.Adjust(ctx, symbol, uid, 0, combined, "全仓强平清算费(维持保证金缓冲)"); err != nil {
		return err
	}
	if err := e.accounts.SettleToBalance(ctx, uid, freeBalance.Neg()); err != nil {
		return err
	}
	return e.accounts.SettleToCredit(ctx, uid, freeCredit.Neg())
}

// 撤掉这个uid全部还没成交的用户委托和还没触发的条件单，返回撤单失败的笔数(失败的已经记了ERROR日志)。
// 强平用：币安、OKX的全仓强平都是先撤掉账户全部挂单(含条件单/机器人单)再处理仓位，见docs/liquidation.md。
// 这样冻结在挂单里的保证金退回账户，不会在强平结算时被漏算；账户风险已经触及强平线，也不该再让新的
// 成交增加仓位。强平单本身不撤：强平单的成交结算是在SubmitOrder里先于"挂剩余量"执行的，这时候
// 把它标成已撤销，SubmitOrder接着还会把剩余量挂进订单簿，留下一笔数据库里已撤销、簿子里还挂着的幽灵单。
// 分片部署下只撤自己拥有的symbol的挂单(别的实例的订单簿摸不到)，条件单只碰MySQL，不受分片限制
func (e *EngineService) CancelAllPendingOrders(ctx context.Context, uid uint64) (failed int) {
	orders, err := e.orders.FindActiveByUID(ctx, uid, "")
	if err != nil {
		log.Printf("[ERROR] 强平撤挂单：查询委托失败, uid=%d: %v", uid, err)
		return 1
	}
	for i := range orders {
		if orders[i].Liquidation || !e.OwnsSymbol(orders[i].Symbol) {
			continue
		}
		if err := e.CancelOrder(ctx, &orders[i]); err != nil {
			log.Printf("[ERROR] 强平撤挂单失败, uid=%d, orderId=%d: %v", uid, orders[i].OrderID, err)
			failed++
		}
	}
	conditional, err := e.conditionalOrders.FindActiveByUID(ctx, uid, "")
	if err != nil {
		log.Printf("[ERROR] 强平撤条件单：查询失败, uid=%d: %v", uid, err)
		return failed + 1
	}
	for _, co := range conditional {
		if err := e.cancelConditionalOrder(ctx, co); err != nil {
			log.Printf("[ERROR] 强平撤条件单失败, uid=%d, orderId=%d: %v", uid, co.OrderID, err)
			failed++
		}
	}
	return failed
}

// 用户主动结束本轮：先撤掉这个uid全部还在排队的委托(跨所有symbol)，再按各自
// symbol当前标记价立即强制平掉全部仓位，最后清算资金状态(credit清零/is_insured复位/round+1)。
// 这不是风险触发的强平，不走LiquidationService那套挂保护价排队+超时兜底——用户自己要结束
// 本轮，没必要等撮合，直接按标记价了结最快，也不需要保护价滑点缓冲。
// 跟CancelOrder/正常下单一样通过Kafka事件从contract-api路由到这里执行——撤单要摘掉
// contract-engine内存里的订单簿，contract-api那边看不到、摸不到。
// 结束本轮——engine分片部署下(docs/engine-sharding.md)这个函数会在每个分片
// 实例上各自独立跑一遍(round.close事件fan-out给所有实例)，每个实例只处理自己拥有的
// symbol那部分(撤单/撤条件单/强平)，全部symbol都确认处理完之后才由抢到锁的那个实例做
// 一次性的最终资金结算(清零credit、round+1)。单实例部署(没配PERP_ENGINE_SYMBOLS)下
// 这套流程完全退化成"自己处理完自己立刻结算"，行为跟分片之前一样，只是多了一次进度表
// 读写(可以忽略不计的开销)
func (e *EngineService) CloseRound(ctx context.Context, uid, round uint64) error {
	account, err := e.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return err
	}
	// 事件里带的round是"要结束哪一轮"，跟账户当前的round不一致说明这一轮已经结束过了(合作方
	// 重试，是Kafka重复投递之外的另一次独立调用)——必须忽略，不能按"当前轮"再结束一次：那样会
	// 把已经推进到下一轮的账户里新挂的单撤掉、新开的仓强平、新发的信用额度清零。round比账户
	// 当前的还大是不合法的请求(API层已经拦了)，同样忽略
	if account.Round != round {
		log.Printf("[WARN] 结束本轮请求的round=%d跟账户当前round=%d不一致，忽略, uid=%d", round, account.Round, uid)
		return nil
	}

	activeOrders, err := e.orders.FindActiveByUID(ctx, uid, "")
	if err != nil {
		return err
	}
	// 条件单(止盈止损/条件开仓)触发前从来没进过撮合引擎的订单簿，CancelOrder摘不到它们，
	// 必须单独撤销——不然结束本轮之后credit清零了，这些条件单开仓方向上冻结的frozen_credit
	// 还留在账上没释放，等它们后来真的触发/被撤销，会把上一轮已经作废的credit又"复活"
	// 进新一轮的余额里
	pendingConditional, err := e.conditionalOrders.FindActiveByUID(ctx, uid, "")
	if err != nil {
		return err
	}
	positions, err := e.positionSvc.FindByUID(ctx, uid)
	if err != nil {
		return err
	}

	symbols := collectRoundCloseSymbols(activeOrders, pendingConditional, positions)
	now := NowMillis()
	for _, sym := range symbols {
		if err := e.roundCloseProgress.Seed(ctx, uid, round, sym, now); err != nil {
			return fmt.Errorf("结束本轮进度登记失败, uid=%d, symbol=%s: %w", uid, sym, err)
		}
	}

	for _, sym := range symbols {
		if !e.OwnsSymbol(sym) {
			continue // 不归这个实例负责，等真正拥有这个symbol的实例自己(fan-out独立)处理
		}
		if e.closeRoundForSymbol(ctx, uid, sym, activeOrders, pendingConditional, positions) {
			if err := e.roundCloseProgress.MarkDone(ctx, uid, round, sym); err != nil {
				log.Printf("[ERROR] 结束本轮标记分片进度失败, uid=%d, symbol=%s: %v", uid, sym, err)
			}
		}
		// 没成功不标记done，日志已经在closeRoundForSymbol内部打过——等下一次这个uid的
		// round.close事件被重新处理(客户端重新调用接口)时会重试，见tryFinalizeCloseRound
	}

	return e.tryFinalizeCloseRound(ctx, uid, round)
}

// 结束本轮涉及到的全部symbol并集(活跃挂单+条件单+持仓)，去重
func collectRoundCloseSymbols(orders []model.Order, conditional []model.ConditionalOrder, positions []model.Position) []string {
	seen := make(map[string]bool)
	var symbols []string
	add := func(sym string) {
		if !seen[sym] {
			seen[sym] = true
			symbols = append(symbols, sym)
		}
	}
	for _, o := range orders {
		add(o.Symbol)
	}
	for _, co := range conditional {
		add(co.Symbol)
	}
	for _, p := range positions {
		if p.Volume.Sign() > 0 {
			add(p.Symbol)
		}
	}
	return symbols
}

// 结束本轮里属于这一个symbol的部分：撤这个symbol上的挂单+条件单，
// 强平这个symbol上的仓位。调用前调用方已经确认e.OwnsSymbol(symbol)——只有真正拥有这个
// symbol订单簿的实例才能安全执行CancelOrder，见docs/engine-sharding.md。返回true表示
// 这个symbol的部分全部处理成功
func (e *EngineService) closeRoundForSymbol(ctx context.Context, uid uint64, symbol string, orders []model.Order, conditional []model.ConditionalOrder, positions []model.Position) bool {
	allDone := true
	for i := range orders {
		if orders[i].Symbol != symbol {
			continue
		}
		if err := e.CancelOrder(ctx, &orders[i]); err != nil {
			log.Printf("[ERROR] 结束本轮撤单失败, uid=%d, orderId=%d: %v", uid, orders[i].OrderID, err)
			allDone = false
		}
	}
	for _, co := range conditional {
		if co.Symbol != symbol {
			continue
		}
		if err := e.cancelConditionalOrder(ctx, co); err != nil {
			log.Printf("[ERROR] 结束本轮撤销条件单失败, uid=%d, orderId=%d: %v", uid, co.OrderID, err)
			allDone = false
		}
	}
	for _, p := range positions {
		if p.Symbol != symbol || p.Volume.Sign() <= 0 {
			continue
		}
		mark, hasMark := e.markPrice.Get(ctx, p.Symbol)
		if !hasMark {
			// 缺标记价格没法公允强平，跳过——这个仓位这轮结束不掉，日志告警，人工介入
			log.Printf("[ERROR] 结束本轮强制平仓缺标记价格，跳过, uid=%d, symbol=%s", uid, p.Symbol)
			allDone = false
			continue
		}
		if _, err := e.forceCloseOnePosition(ctx, p, p.Volume, mark); err != nil {
			log.Printf("[ERROR] 结束本轮强制平仓失败, uid=%d, symbol=%s: %v", uid, p.Symbol, err)
			allDone = false
		}
	}
	return allDone
}

// 检查这个(uid,round)涉及到的全部symbol是不是都处理完了，全部
// 完成才做"清零credit+round前进"这个只能发生一次的最终结算。还没全部完成不是错误——
// 大概率是负责其它symbol的分片实例还没轮到处理这个round.close事件(fan-out消费不保证
// 同时到达每个实例)，直接返回nil、不打ERROR日志，其它实例做完自己那部分之后会各自
// 再调用一次这个函数，最终由真正凑齐"全部完成"这个条件的那一次触发结算。用
// LockService保证即使多个实例同时观察到"全部完成"也只有一个真正执行结算——
// AccountService.CloseRound自带的round原子条件是第二层保险，理论上锁已经能保证互斥，
// 这层是防御性的
func (e *EngineService) tryFinalizeCloseRound(ctx context.Context, uid, round uint64) error {
	allDone, err := e.roundCloseProgress.AllDone(ctx, uid, round)
	if err != nil {
		return err
	}
	if !allDone {
		return nil
	}
	lockKey := fmt.Sprintf("perpgo:lock:closeround:%d:%d", uid, round)
	err = e.lock.WithLock(ctx, lockKey, func() error {
		recheck, err := e.roundCloseProgress.AllDone(ctx, uid, round)
		if err != nil {
			return err
		}
		if !recheck {
			return nil // 理论上不该发生(done只会0→1，不会倒退)，防御性处理
		}
		ok, closeErr := e.accounts.CloseRound(ctx, uid, round)
		// round只要真的被这次调用推进了(ok=true)，progress记录就必须清理，不能因为
		// 紧跟着的closeErr(比如信用额度清零的审计流水insert失败，round本身已经改完了)
		// 就跳过删除——不然这个(uid,round)的progress行会永远留着孤儿数据(这张表没有
		// processed_messages那样的定期清理任务)，而且round.close这个已经真正成功的
		// 动作还会被上层日志误判成失败
		if ok {
			if err := e.roundCloseProgress.Delete(ctx, uid, round); err != nil {
				log.Printf("[WARN] 清理结束本轮进度记录失败, uid=%d, round=%d: %v", uid, round, err)
			}
		}
		if closeErr != nil {
			return closeErr
		}
		if !ok {
			// round已经被别的实例/别的调用推进过了(比如这是同一个round.close事件的
			// Kafka重复投递，上一次已经成功结算过)，不是错误，收尾
			return nil
		}
		// 撤单/强平过程中已经推送过中间状态的账户快照，这里再推一次最终状态(credit清零、
		// round+1)——中间那几次快照都还没反映"结束本轮"这个动作本身造成的变化
		e.push.PublishUserSnapshot(ctx, uid)
		log.Printf("结束本轮完成, uid=%d, round=%d", uid, round)
		return nil
	})
	if errors.Is(err, ErrLockBusy) {
		// 拿不到锁大概率是另一个分片实例正好也观察到"全部完成"、抢先在做同一件事——
		// 这次尝试本身没有任何实际损失(没有任何副作用发生过)，不是需要客户端重新调用
		// 结束本轮接口才能恢复的真失败，只记WARN，不当error往上传，避免每次这种正常的
		// 竞争都在日志里刷一条容易让人误判的[ERROR]
		log.Printf("[WARN] 结束本轮最终结算抢锁失败(大概率是另一个实例正在处理), uid=%d, round=%d", uid, round)
		return nil
	}
	return err
}

// 生成一笔"已成交"的市价平仓单落库(留痕、复用SettleFill结算逻辑)，
// 立即按标记价全部结算掉——不进撮合引擎的订单簿，不用等对手盘
// forceCloseOnePosition closeVolume是要强制平掉的量，调用方保证不超过p.Volume(CloseRound
// 传p.Volume整笔平掉；ADL(adl.go)按需要筹到的金额反推一个更小的量，只平够用的部分)
func (e *EngineService) forceCloseOnePosition(ctx context.Context, p model.Position, closeVolume, mark decimal.Decimal) (FillResult, error) {
	now := NowMillis()
	o := &model.Order{
		OrderID:      NextID(),
		UID:          p.UID,
		Symbol:       p.Symbol,
		Side:         p.Side,
		Action:       model.ActionClose,
		Type:         model.OrderTypeMarket,
		Price:        mark,
		Amount:       closeVolume,
		TradedAmount: closeVolume,
		AvgDealPrice: mark,
		Leverage:     p.Leverage,
		ReduceOnly:   true,
		Status:       model.OrderStatusFilled,
		CreateTime:   now,
		UpdateTime:   now,
	}
	if err := e.orders.Insert(ctx, o); err != nil {
		return FillResult{}, err
	}
	return e.settlement.SettleFill(ctx, o, closeVolume, mark, false, now)
}

// 从订单簿摘掉委托、退回剩余冻结保证金、落库改CANCELED
func (e *EngineService) CancelOrder(ctx context.Context, o *model.Order) error {
	// 分片部署下这个symbol可能不归这个实例负责——绝不能落到下面的fallback分支(book.Cancel
	// 在本地空订单簿上找不到、以为"已经不在簿子上了"，误把它当CANCELED落库+退保证金，而
	// 真正拥有这个symbol订单簿的实例里这笔委托可能还实实在在挂着，会造成DB状态和真实撮合
	// 状态对不上，见docs/engine-sharding.md
	if !e.OwnsSymbol(o.Symbol) {
		return nil
	}
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

// 统一负责"标记DB为CANCELED+按剩余量释放冻结保证金+推送账户快照"——
// CancelOrder(正常撤单接口触发)和自成交保护(book.Match内部摘除maker，见SubmitOrder)都要
// 走到这一步，只是"从订单簿摘除"这一步各自的时机/方式不同(前者显式调book.Cancel，后者
// book.Match内部已经摘完了)，DB落库+保证金释放+推送的逻辑完全一样，不应该写两份
func (e *EngineService) finalizeOrderCancel(ctx context.Context, o *model.Order, remaining decimal.Decimal) error {
	marked, err := e.cancelAndReleaseMargin(ctx, o, remaining)
	if err != nil {
		return err
	}
	if marked {
		e.push.PublishUserSnapshot(ctx, o.UID)
	}
	return nil
}

// 是finalizeOrderCancel的核心逻辑，单独拆出来是因为SubmitOrder
// 处理MARKET单缺流动性未成交剩余部分时(见下面)也要用这套"标记CANCELED+按比例释放冻结
// 保证金"逻辑，但不能再推一次账户快照——SubmitOrder末尾已经有统一的touchedUIDs快照推送，
// 这里再推会重复
func (e *EngineService) cancelAndReleaseMargin(ctx context.Context, o *model.Order, remaining decimal.Decimal) (bool, error) {
	marked, err := e.orders.MarkCanceled(ctx, o.OrderID, NowMillis())
	if err != nil {
		return false, err
	}
	if !marked {
		// MarkCanceled的WHERE status IN ('open','partially_filled')没匹配到行，说明这笔
		// 委托已经被别的路径终结过了(比如正常撤单和自成交保护并发撞到同一笔单子，或者
		// CloseRound用的是撤单前拍的旧快照、这笔单子已经在别处被处理完)——不能再往下走释放
		// 保证金，否则同一笔冻结会被重复释放，凭空多出一笔钱
		return false, nil
	}
	if o.Action == model.ActionOpen && remaining.Sign() > 0 {
		// 按剩余比例分别算这笔委托冻结的available/credit部分该释放多少，不能笼统释放到
		// available——那样等于让信用额度经过"冻结再撤单"这个渠道被洗成可提现的available
		releaseAvailable, releaseCredit := o.ProportionalFrozen(remaining)
		if err := e.accounts.UnfreezeMargin(ctx, o.UID, releaseAvailable, releaseCredit); err != nil {
			return true, err
		}
	}
	return true, nil
}

// 撤销一笔还没触发的条件单——跟router.go里contract-api那个撤销
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
