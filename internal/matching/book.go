package matching

import (
	"sort"
	"sync"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
)

type Direction int

const (
	Buy Direction = iota
	Sell
)

// 把(side, action)映射成买/卖方向
func DirectionOf(side model.Side, action model.OrderAction) Direction {
	if (side == model.SideLong && action == model.ActionOpen) || (side == model.SideShort && action == model.ActionClose) {
		return Buy
	}
	return Sell
}

// 订单簿里挂着的委托——只存撮合需要的字段，不是完整的DB行，DB落地由调用方
// 单独维护，这里只管"这笔单子还剩多少量、什么时候进来的"
type RestingOrder struct {
	OrderID     uint64
	UID         uint64
	Side        model.Side
	Action      model.OrderAction
	Direction   Direction
	Price       decimal.Decimal
	Remaining   decimal.Decimal
	EntryTime   int64 // 撮合引擎收到这笔单子的纳秒时间戳，同价格档位按这个排先后
	ReduceOnly  bool
	Liquidation bool
}

// 一次撮合成交——taker是主动进来吃单的一方(incoming)，maker是原来挂在簿子上被动等到的一方
type Fill struct {
	Price      decimal.Decimal
	Volume     decimal.Decimal
	MakerOrder *RestingOrder
	TakerOrder *RestingOrder
}

// 双向链表节点，同时是map查找的目标——O(1)按orderID撤单靠的是map[orderID]*orderNode
// 直接定位到链表节点，从链表摘除是O(1)（有prev/next指针，不用像切片那样整体搬移）
type orderNode struct {
	order      *RestingOrder
	prev, next *orderNode
	level      *priceLevel // 直接指回所在价格档位，撤单时不需要再按价格二分查找一次
}

// 一个价格档位：这个价格上排队的全部委托，按时间优先的FIFO双向链表，另外维护
// 档位汇总量/笔数，深度查询(Depth)和撮合时判断"这一档还有没有单"都是O(1)读取，不用遍历链表
type priceLevel struct {
	price       decimal.Decimal
	head, tail  *orderNode
	totalVolume decimal.Decimal
	count       int
}

// 按EntryTime把新节点插入到这一档链表里正确的位置——同一档内先到先得，
// EntryTime更小(更早进入撮合引擎)排在前面。EntryTime是调用方在拿到Book锁之前就已经算好的
// 值(比如Kafka消费者收到消息的纳秒时间戳)，多个goroutine(下单消费者、强平定时扫描、条件单
// 触发扫描)各自准备好委托、再抢Book的锁——谁先抢到锁(即Rest调用的先后顺序)不等于谁的
// EntryTime更早，两者可能不一致，所以必须按EntryTime的值找插入点，不能简单地"谁先调用
// Rest就排在最后"(那样退化成"抢锁顺序优先"，同一价位的成交优先级会随线程调度抖动，这是
// 真实的公平性问题，不是理论上的边界情况)。多数情况下新单子的EntryTime比已经在队列里的
// 都新，从队尾往前扫更快找到插入点，只有在极少数"锁竞争顺序跟EntryTime顺序不一致"的情况
// 才需要扫过几个节点
func (pl *priceLevel) insertOrdered(order *RestingOrder) *orderNode {
	node := &orderNode{order: order, level: pl}
	cur := pl.tail
	for cur != nil && cur.order.EntryTime > order.EntryTime {
		cur = cur.prev
	}
	if cur == nil {
		node.next = pl.head
		if pl.head != nil {
			pl.head.prev = node
		} else {
			pl.tail = node
		}
		pl.head = node
	} else {
		node.prev = cur
		node.next = cur.next
		if cur.next != nil {
			cur.next.prev = node
		} else {
			pl.tail = node
		}
		cur.next = node
	}
	pl.totalVolume = pl.totalVolume.Add(order.Remaining)
	pl.count++
	return node
}

// 单个symbol的订单簿：bids/asks各自是按价格排序的档位数组(sort.Search二分定位最优价/
// 插入点)，每个档位内部是按时间先后排队的FIFO双向链表，另外一个map支持O(1)按orderID撤单——
// 用"档位数组+组内链表"而不是红黑树/跳表，是因为活跃价格档位数量远小于挂单笔数，best
// price/撮合热路径直接读数组端点是O(1)，档位本身的增删(只在某个价格第一次/最后一次有单时
// 发生)频率远低于订单的挂/撤/成交，详见docs/order-book.md
// mu用RWMutex而不是普通Mutex：Depth()是唯一的只读操作，而且是这个订单簿唯一会被外部
// (HTTP深度查询接口)高频调用的入口，用读写锁让多个并发的深度查询之间不用互相排队，只有
// 真正改状态的Match/Rest/Cancel才需要独占锁——减少深度查询接口跟撮合热路径抢锁的开销
type Book struct {
	mu   sync.RWMutex
	bids []*priceLevel // 价格从高到低
	asks []*priceLevel // 价格从低到高
	byID map[uint64]*orderNode
}

func NewBook() *Book {
	return &Book{byID: make(map[uint64]*orderNode)}
}

// 在有序档位数组里二分查找price对应的档位。buy=true表示数组按价格从高到低
// 排序(bids)，false表示从低到高(asks)。没精确找到时返回的索引是"应该插入的位置"，
// 跟sort.Search的约定一致，插入/查找共用同一个函数
func findLevelIndex(levels []*priceLevel, price decimal.Decimal, buy bool) (int, bool) {
	i := sort.Search(len(levels), func(i int) bool {
		if buy {
			return levels[i].price.LessThanOrEqual(price)
		}
		return levels[i].price.GreaterThanOrEqual(price)
	})
	if i < len(levels) && levels[i].price.Equal(price) {
		return i, true
	}
	return i, false
}

func insertLevelAt(levels []*priceLevel, i int, pl *priceLevel) []*priceLevel {
	levels = append(levels, nil)
	copy(levels[i+1:], levels[i:])
	levels[i] = pl
	return levels
}

func removeLevelAt(levels []*priceLevel, i int) []*priceLevel {
	return append(levels[:i], levels[i+1:]...)
}

// 找到(或创建)price对应的档位——找不到就在正确的位置插入一个新档位，
// 保持数组有序
func (b *Book) getOrCreateLevel(price decimal.Decimal, buy bool) *priceLevel {
	levels := b.bids
	if !buy {
		levels = b.asks
	}
	i, found := findLevelIndex(levels, price, buy)
	if found {
		return levels[i]
	}
	pl := &priceLevel{price: price}
	levels = insertLevelAt(levels, i, pl)
	if buy {
		b.bids = levels
	} else {
		b.asks = levels
	}
	return pl
}

// 档位空了，从档位数组里摘掉——只在某个价格最后一笔挂单被吃完/撤销
// 时才会调用，频率远低于订单级别的操作
func (b *Book) removeLevelFromIndex(pl *priceLevel, buy bool) {
	levels := b.bids
	if !buy {
		levels = b.asks
	}
	i, found := findLevelIndex(levels, pl.price, buy)
	if !found {
		return
	}
	levels = removeLevelAt(levels, i)
	if buy {
		b.bids = levels
	} else {
		b.asks = levels
	}
}

// 纯链表摘除+count--+map删除，档位空了顺带从档位数组里摘掉——
// 不碰totalVolume，调用方(applyFill/cancelNode)已经按各自的场景把汇总量减好了
func (b *Book) unlinkAndMaybeRemoveLevel(node *orderNode, buy bool) {
	pl := node.level
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		pl.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		pl.tail = node.prev
	}
	pl.count--
	delete(b.byID, node.order.OrderID)
	if pl.head == nil {
		b.removeLevelFromIndex(pl, buy)
	}
}

// 处理一笔成交对某个挂单节点的影响：减少这个节点的剩余量、同步减少所在档位的
// 汇总量；如果这个节点被完全吃掉，顺带从链表/map/档位数组里摘除
func (b *Book) applyFill(node *orderNode, vol decimal.Decimal, buy bool) {
	node.order.Remaining = node.order.Remaining.Sub(vol)
	node.level.totalVolume = node.level.totalVolume.Sub(vol)
	if node.order.Remaining.Sign() <= 0 {
		b.unlinkAndMaybeRemoveLevel(node, buy)
	}
}

// 撤销/自成交摘除一个还没被成交动过的节点：把它剩余的全部量从档位汇总里扣掉，
// 然后摘除——跟applyFill的区别是这里节点的Remaining还没被改动过，要按"全部剩余量"扣
func (b *Book) cancelNode(node *orderNode, buy bool) {
	node.level.totalVolume = node.level.totalVolume.Sub(node.order.Remaining)
	b.unlinkAndMaybeRemoveLevel(node, buy)
}

// 尝试撮合一笔新进来的委托，返回成交列表 + 因为自成交保护被摘掉的maker委托列表。
// LIMIT单未完全成交的剩余部分，由调用方决定要不要调Rest挂回簿子；MARKET单剩余量直接由
// 调用方释放，不挂簿。
//
// 自成交保护(STP，取消maker模式)：撮合过程中如果对手盘(maker)跟主动吃单方(taker)是
// 同一个uid，不产生这笔成交——把这个maker摘掉(当正常撤单处理，调用方要负责退保证金)，
// taker继续往下尝试撮合。不打乱队列里其他人的排队顺序(自己的单子被摘掉后，后面排队的人
// 自然前移)，也不让taker因为撞到自己的单子就吃不到别人的流动性，是主流交易所的默认STP行为
func (b *Book) Match(order *RestingOrder) (fills []Fill, selfCanceled []*RestingOrder) {
	b.mu.Lock()
	defer b.mu.Unlock()

	isMarket := order.Price.IsZero() // 价格为0就是市价单
	if order.Direction == Buy {
		for order.Remaining.Sign() > 0 && len(b.asks) > 0 {
			level := b.asks[0] // 卖一档
			if !isMarket && level.price.GreaterThan(order.Price) {
				break
			}
			node := level.head // 这一档排在最前面的委托
			if node.order.UID == order.UID {
				selfCanceled = append(selfCanceled, node.order)
				b.cancelNode(node, false)
				continue
			}
			vol := decimal.Min(order.Remaining, node.order.Remaining)
			fills = append(fills, Fill{Price: level.price, Volume: vol, MakerOrder: node.order, TakerOrder: order})
			order.Remaining = order.Remaining.Sub(vol)
			b.applyFill(node, vol, false)
		}
	} else {
		for order.Remaining.Sign() > 0 && len(b.bids) > 0 {
			level := b.bids[0] // 买一档
			if !isMarket && level.price.LessThan(order.Price) {
				break
			}
			node := level.head
			if node.order.UID == order.UID {
				selfCanceled = append(selfCanceled, node.order)
				b.cancelNode(node, true)
				continue
			}
			vol := decimal.Min(order.Remaining, node.order.Remaining)
			fills = append(fills, Fill{Price: level.price, Volume: vol, MakerOrder: node.order, TakerOrder: order})
			order.Remaining = order.Remaining.Sub(vol)
			b.applyFill(node, vol, true)
		}
	}
	return fills, selfCanceled
}

// 把未完全成交的LIMIT单剩余部分挂进簿子，按价格-时间优先插入到正确位置
// Rest 把未完全成交的LIMIT单剩余部分挂进簿子，按价格-时间优先插入到正确位置。如果这个
// orderID已经在簿子里(比如上游Kafka消费者在at-least-once语义下重复投递了同一个下单
// 事件，SubmitOrder被重复调用)，不会插入第二份、静默覆盖map里的旧引用把旧节点变成
// "找不到、但还挂在簿子上"的孤儿——直接跳过，返回false让调用方知道这是一次重复调用
func (b *Book) Rest(order *RestingOrder) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.byID[order.OrderID]; exists {
		return false
	}
	buy := order.Direction == Buy
	level := b.getOrCreateLevel(order.Price, buy)
	b.byID[order.OrderID] = level.insertOrdered(order)
	return true
}

// 从簿子里摘掉一笔委托，返回被摘掉时还剩多少量(调用方要把这部分保证金退回)，
// O(1)(map查找定位节点+链表摘除)，档位是否需要从数组里摘除是唯一的O(log m)+O(m)开销
// (m=档位数)，只在这一档最后一笔单被摘掉时才发生
func (b *Book) Cancel(orderID uint64) (decimal.Decimal, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	node, ok := b.byID[orderID]
	if !ok {
		return decimal.Zero, false
	}
	remaining := node.order.Remaining
	b.cancelNode(node, node.order.Direction == Buy)
	return remaining, true
}

// 这个orderID当前是否正挂在簿子上——SubmitOrder用这个防御Kafka at-least-once
// 语义下的重复投递：如果一笔下单事件被重复消费、这个orderId已经在挂着，说明上一次投递
// 已经完整处理过(撮合+挂剩余量)了，不能对它再跑一遍Match，否则一笔仍在簿子上的挂单会
// 被当成"新的taker"再次尝试撮合，可能吃掉不该被这笔重复事件消耗的对手盘流动性——这不是
// 假设性场景，Kafka消费者(internal/mq)本身不做去重，见docs/websocket.md和
// docs/known-limitations.md
func (b *Book) Contains(orderID uint64) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	_, ok := b.byID[orderID]
	return ok
}

// DefaultDepthLevels 深度快照默认返回的档位数——REST查询接口(internal/api/engine_server.go)
// 和WS实时推送(EngineService.SubmitOrder)共用同一个默认值，不要各自硬编码一份
const DefaultDepthLevels = 20

// 深度快照里聚合后的一档——只暴露价格/总量/笔数，不暴露单笔委托的uid/orderID，
// 公开的深度数据不该泄露个人挂单归属
type PriceLevel struct {
	Price  decimal.Decimal `json:"price"`
	Volume decimal.Decimal `json:"volume"` // 这一档全部挂单剩余量之和
	Count  int             `json:"count"`  // 这一档挂单笔数
}

type DepthSnapshot struct {
	Bids []PriceLevel `json:"bids"` // 价格从高到低
	Asks []PriceLevel `json:"asks"` // 价格从低到高
}

// 按档位聚合的订单簿快照，最多返回每边maxLevels档，<=0表示不限（返回全部档位）
func (b *Book) Depth(maxLevels int) DepthSnapshot {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return DepthSnapshot{
		Bids: snapshotLevels(b.bids, maxLevels),
		Asks: snapshotLevels(b.asks, maxLevels),
	}
}

// 冲击价格：按名义金额notional(USDT，即价格*数量)去吃盘口，返回吃完这些金额的平均成交价。
// bid是卖出(吃买盘)的平均价，ask是买入(吃卖盘)的平均价。任何一侧的总名义价值不够notional，或者
// notional<=0，都返回ok=false——盘口太薄时没有可靠的冲击价格，调用方不能拿一侧的价格凑合。
// 资金费率的溢价用它算(见service.FundingService)：比买一卖一中价难操纵，要影响冲击价格得在盘口上
// 摆出至少notional这么大的单子，而不是在买一挂一手就行
func (b *Book) ImpactPrices(notional decimal.Decimal) (bid, ask decimal.Decimal, ok bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	bid, okBid := impactPrice(b.bids, notional)
	ask, okAsk := impactPrice(b.asks, notional)
	return bid, ask, okBid && okAsk
}

// 从最优价往深处吃，levels已经按"最优价在前"排好序(bids从高到低，asks从低到高)
func impactPrice(levels []*priceLevel, notional decimal.Decimal) (decimal.Decimal, bool) {
	if notional.Sign() <= 0 {
		return decimal.Zero, false
	}
	remaining, qty := notional, decimal.Zero
	for _, pl := range levels {
		levelNotional := pl.price.Mul(pl.totalVolume)
		if remaining.LessThanOrEqual(levelNotional) {
			// 最后一档只吃一部分。中间这次除法用32位小数：默认的16位会让"名义金额/(名义金额/价格)"
			// 差一点点不等于价格(65065变成65064.99999999935)，单档成交时冲击价必须恰好等于这一档的价格，
			// 不然指数价恰好等于买价时溢价会算出一个1e-16量级的噪声而不是0
			qty = qty.Add(remaining.DivRound(pl.price, 32))
			return notional.DivRound(qty, 16), true
		}
		qty = qty.Add(pl.totalVolume)
		remaining = remaining.Sub(levelNotional)
	}
	return decimal.Zero, false
}

func snapshotLevels(levels []*priceLevel, maxLevels int) []PriceLevel {
	n := len(levels)
	if maxLevels > 0 && maxLevels < n {
		n = maxLevels
	}
	out := make([]PriceLevel, n)
	for i := 0; i < n; i++ {
		out[i] = PriceLevel{Price: levels[i].price, Volume: levels[i].totalVolume, Count: levels[i].count}
	}
	return out
}

// 管理全部symbol各自的Book
type Engine struct {
	mu    sync.Mutex
	books map[string]*Book
}

func NewEngine() *Engine { return &Engine{books: make(map[string]*Book)} }

func (e *Engine) BookFor(symbol string) *Book {
	e.mu.Lock()
	defer e.mu.Unlock()
	b, ok := e.books[symbol]
	if !ok {
		b = NewBook()
		e.books[symbol] = b
	}
	return b
}
