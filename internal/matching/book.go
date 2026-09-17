package matching

import (
	"sync"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
)

type Direction int

const (
	Buy Direction = iota
	Sell
)

// DirectionOf 把(side, action)映射成买/卖方向
func DirectionOf(side model.Side, action model.OrderAction) Direction {
	if (side == model.SideLong && action == model.ActionOpen) || (side == model.SideShort && action == model.ActionClose) {
		return Buy
	}
	return Sell
}

// RestingOrder 订单簿里挂着的委托——只存撮合需要的字段，不是完整的DB行，DB落地由调用方
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

// Fill 一次撮合成交——taker是主动进来吃单的一方(incoming)，maker是原来挂在簿子上被动等到的一方
type Fill struct {
	Price      decimal.Decimal
	Volume     decimal.Decimal
	MakerOrder *RestingOrder
	TakerOrder *RestingOrder
}

// Book 单个symbol的订单簿——bids/asks各自按"价格优先、同价格先进先出"排序，用简单切片+
// 每次插入排序：MVP阶段成交量级不需要更高级的数据结构(跳表/红黑树)，先用最直接的实现
type Book struct {
	mu   sync.Mutex
	bids []*RestingOrder // 买盘，价格从高到低
	asks []*RestingOrder // 卖盘，价格从低到高
}

func NewBook() *Book { return &Book{} }

// Match 尝试撮合一笔新进来的委托，返回成交列表+撮合完之后还剩多少量。LIMIT单剩余量>0时
// 由调用方决定要不要挂回簿子(调CancelableRest)；MARKET单剩余量直接由调用方释放，不挂簿。
func (b *Book) Match(order *RestingOrder) []Fill {
	b.mu.Lock()
	defer b.mu.Unlock()

	var fills []Fill
	isMarket := order.Price.IsZero()
	if order.Direction == Buy {
		for order.Remaining.Sign() > 0 && len(b.asks) > 0 {
			best := b.asks[0]
			if !isMarket && best.Price.GreaterThan(order.Price) {
				break
			}
			vol := decimal.Min(order.Remaining, best.Remaining)
			fills = append(fills, Fill{Price: best.Price, Volume: vol, MakerOrder: best, TakerOrder: order})
			order.Remaining = order.Remaining.Sub(vol)
			best.Remaining = best.Remaining.Sub(vol)
			if best.Remaining.Sign() <= 0 {
				b.asks = b.asks[1:]
			}
		}
	} else {
		for order.Remaining.Sign() > 0 && len(b.bids) > 0 {
			best := b.bids[0]
			if !isMarket && best.Price.LessThan(order.Price) {
				break
			}
			vol := decimal.Min(order.Remaining, best.Remaining)
			fills = append(fills, Fill{Price: best.Price, Volume: vol, MakerOrder: best, TakerOrder: order})
			order.Remaining = order.Remaining.Sub(vol)
			best.Remaining = best.Remaining.Sub(vol)
			if best.Remaining.Sign() <= 0 {
				b.bids = b.bids[1:]
			}
		}
	}
	return fills
}

// Rest 把未完全成交的LIMIT单剩余部分挂进簿子，按价格-时间优先插入到正确位置
func (b *Book) Rest(order *RestingOrder) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if order.Direction == Buy {
		b.bids = insertSorted(b.bids, order, func(a, x *RestingOrder) bool {
			if !a.Price.Equal(x.Price) {
				return a.Price.GreaterThan(x.Price) // 买盘价格从高到低
			}
			return a.EntryTime < x.EntryTime
		})
	} else {
		b.asks = insertSorted(b.asks, order, func(a, x *RestingOrder) bool {
			if !a.Price.Equal(x.Price) {
				return a.Price.LessThan(x.Price) // 卖盘价格从低到高
			}
			return a.EntryTime < x.EntryTime
		})
	}
}

// Cancel 从簿子里摘掉一笔委托，返回被摘掉时还剩多少量(调用方要把这部分保证金退回)
func (b *Book) Cancel(orderID uint64) (decimal.Decimal, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if r, ok := removeByID(b.bids, orderID); ok {
		b.bids = r.remaining
		return r.order.Remaining, true
	}
	if r, ok := removeByID(b.asks, orderID); ok {
		b.asks = r.remaining
		return r.order.Remaining, true
	}
	return decimal.Zero, false
}

type removeResult struct {
	remaining []*RestingOrder
	order     *RestingOrder
}

func removeByID(list []*RestingOrder, orderID uint64) (removeResult, bool) {
	for i, o := range list {
		if o.OrderID == orderID {
			out := make([]*RestingOrder, 0, len(list)-1)
			out = append(out, list[:i]...)
			out = append(out, list[i+1:]...)
			return removeResult{remaining: out, order: o}, true
		}
	}
	return removeResult{}, false
}

func insertSorted(list []*RestingOrder, item *RestingOrder, less func(a, x *RestingOrder) bool) []*RestingOrder {
	i := 0
	for i < len(list) && less(list[i], item) {
		i++
	}
	out := make([]*RestingOrder, 0, len(list)+1)
	out = append(out, list[:i]...)
	out = append(out, item)
	out = append(out, list[i:]...)
	return out
}

// Engine 管理全部symbol各自的Book
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
