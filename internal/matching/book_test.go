package matching

import (
	"testing"

	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal {
	v, err := decimal.NewFromString(s)
	if err != nil {
		panic(err)
	}
	return v
}

func newResting(id, uid uint64, dir Direction, price, remaining string, entryTime int64) *RestingOrder {
	return &RestingOrder{
		OrderID:   id,
		UID:       uid,
		Direction: dir,
		Price:     d(price),
		Remaining: d(remaining),
		EntryTime: entryTime,
	}
}

func TestBook_PriceTimePriority(t *testing.T) {
	b := NewBook()
	// 三档买单：99(先到)、100(先到)、100(后到)——最优价100先吃，同价按时间先后
	b.Rest(newResting(1, 1, Buy, "99", "1", 1))
	b.Rest(newResting(2, 2, Buy, "100", "1", 2))
	b.Rest(newResting(3, 3, Buy, "100", "1", 3))

	taker := newResting(4, 4, Sell, "0", "2.5", 4) // 市价卖单，吃3笔里的2.5个
	fills, selfCanceled := b.Match(taker)
	if len(selfCanceled) != 0 {
		t.Fatalf("不该有自成交, got %d", len(selfCanceled))
	}
	if len(fills) != 3 {
		t.Fatalf("期望3笔成交(id2全部+id3全部+id1部分)，实际%d笔: %+v", len(fills), fills)
	}
	// 价格优先：先吃100档(id2再id3，按进入时间)，再吃99档(id1)
	if fills[0].MakerOrder.OrderID != 2 || !fills[0].Volume.Equal(d("1")) {
		t.Fatalf("第1笔应该是id2全部成交, got %+v", fills[0])
	}
	if fills[1].MakerOrder.OrderID != 3 || !fills[1].Volume.Equal(d("1")) {
		t.Fatalf("第2笔应该是id3全部成交, got %+v", fills[1])
	}
	if fills[2].MakerOrder.OrderID != 1 || !fills[2].Volume.Equal(d("0.5")) {
		t.Fatalf("第3笔应该是id1部分成交0.5, got %+v", fills[2])
	}
	if !taker.Remaining.IsZero() {
		t.Fatalf("taker应该被完全吃掉, remaining=%s", taker.Remaining)
	}

	depth := b.Depth(0)
	if len(depth.Bids) != 1 || !depth.Bids[0].Price.Equal(d("99")) || !depth.Bids[0].Volume.Equal(d("0.5")) {
		t.Fatalf("剩余应该只有99档、量0.5, got %+v", depth.Bids)
	}
}

func TestBook_PartialFillStaysAtHead(t *testing.T) {
	b := NewBook()
	b.Rest(newResting(1, 1, Buy, "100", "5", 1))
	taker := newResting(2, 2, Sell, "0", "2", 2)
	fills, _ := b.Match(taker)
	if len(fills) != 1 || !fills[0].Volume.Equal(d("2")) {
		t.Fatalf("期望部分成交2, got %+v", fills)
	}
	depth := b.Depth(0)
	if len(depth.Bids) != 1 || !depth.Bids[0].Volume.Equal(d("3")) || depth.Bids[0].Count != 1 {
		t.Fatalf("剩余应该还有1笔、量3, got %+v", depth.Bids)
	}
}

func TestBook_CancelByID(t *testing.T) {
	b := NewBook()
	b.Rest(newResting(1, 1, Buy, "100", "5", 1))
	b.Rest(newResting(2, 2, Buy, "100", "3", 2))
	b.Rest(newResting(3, 3, Buy, "99", "1", 3))

	remaining, ok := b.Cancel(2)
	if !ok || !remaining.Equal(d("3")) {
		t.Fatalf("撤单应该成功、返回剩余量3, got remaining=%s ok=%v", remaining, ok)
	}
	// 再撤一次同一个id应该失败(已经不在簿子里了)
	if _, ok := b.Cancel(2); ok {
		t.Fatalf("重复撤销同一个订单应该失败")
	}

	depth := b.Depth(0)
	if len(depth.Bids) != 2 {
		t.Fatalf("撤销后应该还剩2档, got %d", len(depth.Bids))
	}
	if depth.Bids[0].Count != 1 || !depth.Bids[0].Volume.Equal(d("5")) {
		t.Fatalf("100档应该只剩id1、量5, got %+v", depth.Bids[0])
	}

	// 撤空一整个档位，验证档位数组也正确摘除
	if _, ok := b.Cancel(1); !ok {
		t.Fatalf("撤销id1应该成功")
	}
	depth = b.Depth(0)
	if len(depth.Bids) != 1 || !depth.Bids[0].Price.Equal(d("99")) {
		t.Fatalf("100档应该被完全摘除，只剩99档, got %+v", depth.Bids)
	}
}

func TestBook_SelfTradePrevention_CancelsMaker(t *testing.T) {
	b := NewBook()
	// uid=1在100档挂了买单，uid=1自己又发一笔卖单——应该触发STP，maker被摘掉，不产生成交
	b.Rest(newResting(1, 1, Buy, "100", "5", 1))
	taker := newResting(2, 1, Sell, "0", "5", 2) // 同一个uid=1
	fills, selfCanceled := b.Match(taker)

	if len(fills) != 0 {
		t.Fatalf("自成交应该被拦截，不产生成交, got %+v", fills)
	}
	if len(selfCanceled) != 1 || selfCanceled[0].OrderID != 1 {
		t.Fatalf("期望id1被自成交保护摘除, got %+v", selfCanceled)
	}
	if !taker.Remaining.Equal(d("5")) {
		t.Fatalf("taker没有对手盘可吃，应该维持原样, remaining=%s", taker.Remaining)
	}
	depth := b.Depth(0)
	if len(depth.Bids) != 0 {
		t.Fatalf("id1应该已经被摘出订单簿, got %+v", depth.Bids)
	}
}

func TestBook_SelfTradePrevention_TakerContinuesPastSelfOrders(t *testing.T) {
	b := NewBook()
	// 同一档100先挂了uid=1的单，再挂uid=2的单——uid=1发taker卖单，应该跳过自己那笔(摘掉)，
	// 继续吃uid=2那笔，不打断整个撮合
	b.Rest(newResting(1, 1, Buy, "100", "3", 1))
	b.Rest(newResting(2, 2, Buy, "100", "4", 2))
	taker := newResting(3, 1, Sell, "0", "4", 3)

	fills, selfCanceled := b.Match(taker)
	if len(selfCanceled) != 1 || selfCanceled[0].OrderID != 1 {
		t.Fatalf("期望id1(自成交)被摘除, got %+v", selfCanceled)
	}
	if len(fills) != 1 || fills[0].MakerOrder.OrderID != 2 || !fills[0].Volume.Equal(d("4")) {
		t.Fatalf("期望taker吃掉id2全部4个, got %+v", fills)
	}
	if !taker.Remaining.IsZero() {
		t.Fatalf("taker应该被完全吃掉, remaining=%s", taker.Remaining)
	}
}

func TestBook_LimitPriceBoundary(t *testing.T) {
	b := NewBook()
	b.Rest(newResting(1, 1, Sell, "100", "1", 1))
	// 买单出价99，够不到卖一价100，不应该成交
	taker := newResting(2, 2, Buy, "99", "1", 2)
	fills, _ := b.Match(taker)
	if len(fills) != 0 {
		t.Fatalf("买价低于卖一价不应该成交, got %+v", fills)
	}
	// 买单出价100，正好打平卖一价，应该成交
	taker2 := newResting(3, 3, Buy, "100", "1", 3)
	fills2, _ := b.Match(taker2)
	if len(fills2) != 1 {
		t.Fatalf("买价等于卖一价应该成交, got %+v", fills2)
	}
}

func TestBook_DepthMaxLevels(t *testing.T) {
	b := NewBook()
	for i := 0; i < 5; i++ {
		price := decimal.NewFromInt(int64(100 - i))
		b.Rest(newResting(uint64(i+1), uint64(i+1), Buy, price.String(), "1", int64(i)))
	}
	depth := b.Depth(2)
	if len(depth.Bids) != 2 {
		t.Fatalf("期望限制在2档, got %d", len(depth.Bids))
	}
	if !depth.Bids[0].Price.Equal(d("100")) || !depth.Bids[1].Price.Equal(d("99")) {
		t.Fatalf("应该是价格最高的前2档, got %+v", depth.Bids)
	}
}
