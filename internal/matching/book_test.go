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

func TestBook_RestOutOfOrderEntryTime(t *testing.T) {
	b := NewBook()
	// 模拟多个goroutine抢锁顺序跟各自EntryTime顺序不一致的情况：先调用Rest的反而
	// EntryTime更晚(比如强平扫描goroutine抢到锁比下单消费者goroutine快，但下单事件
	// 实际更早进入引擎)。同一档内的FIFO顺序必须按EntryTime排，不能按Rest调用顺序排
	b.Rest(newResting(1, 1, Buy, "100", "1", 300)) // 后到但先调用Rest
	b.Rest(newResting(2, 2, Buy, "100", "1", 100)) // 最早，但后调用Rest
	b.Rest(newResting(3, 3, Buy, "100", "1", 200)) // 居中

	taker := newResting(4, 4, Sell, "0", "3", 4)
	fills, _ := b.Match(taker)
	if len(fills) != 3 {
		t.Fatalf("期望3笔成交, got %d", len(fills))
	}
	// 按EntryTime先后应该是: id2(100) → id3(200) → id1(300)，不是Rest调用顺序(1,2,3)
	wantOrder := []uint64{2, 3, 1}
	for i, f := range fills {
		if f.MakerOrder.OrderID != wantOrder[i] {
			t.Fatalf("第%d笔成交应该是id%d(按EntryTime排序), got id%d", i+1, wantOrder[i], f.MakerOrder.OrderID)
		}
	}
}

func TestBook_RestRejectsDuplicateOrderID(t *testing.T) {
	b := NewBook()
	ok1 := b.Rest(newResting(1, 1, Buy, "100", "5", 1))
	if !ok1 {
		t.Fatalf("第一次挂单应该成功")
	}
	// 模拟Kafka消息重复投递导致SubmitOrder对同一个orderId重复调用Rest
	ok2 := b.Rest(newResting(1, 1, Buy, "101", "5", 2))
	if ok2 {
		t.Fatalf("重复的orderId应该被拒绝，不能悄悄覆盖")
	}
	// 原来那笔应该原封不动，还能正常撤销/撮合，没有变成孤儿
	depth := b.Depth(0)
	if len(depth.Bids) != 1 || !depth.Bids[0].Price.Equal(d("100")) || depth.Bids[0].Count != 1 {
		t.Fatalf("订单簿状态应该没被污染, got %+v", depth.Bids)
	}
	remaining, ok := b.Cancel(1)
	if !ok || !remaining.Equal(d("5")) {
		t.Fatalf("原来那笔应该还能正常撤销, remaining=%s ok=%v", remaining, ok)
	}
}

func TestBook_Contains(t *testing.T) {
	b := NewBook()
	if b.Contains(1) {
		t.Fatalf("空订单簿不应该包含任何orderId")
	}
	b.Rest(newResting(1, 1, Buy, "100", "5", 1))
	if !b.Contains(1) {
		t.Fatalf("挂单之后应该能查到这个orderId——EngineService.SubmitOrder靠这个防御Kafka\n\t\t重复投递: 一个仍在排队的委托不该被当成新的taker再次尝试撮合")
	}
	b.Cancel(1)
	if b.Contains(1) {
		t.Fatalf("撤销之后不应该再查到这个orderId")
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

// 订单簿恢复时把落库的活跃挂单按时间顺序逐个重放进Match，依赖这个不变量：一组互不相交的
// 挂单(最高买价<最低卖价)，不管按什么顺序重放，任何一步的对手盘都是这组挂单的子集，不可能
// 交叉，所以不会撮合出任何成交——恢复不会凭空造出世界上没发生过的成交
func TestBook_ReplayUncrossedSetThroughMatch_ProducesNoFills(t *testing.T) {
	orders := []*RestingOrder{
		newResting(1, 1, Buy, "99", "1", 1),
		newResting(2, 2, Sell, "101", "2", 2),
		newResting(3, 3, Buy, "100", "1.5", 3),
		newResting(4, 4, Sell, "102", "1", 4),
		newResting(5, 5, Buy, "98", "3", 5),
		newResting(6, 6, Sell, "101", "0.5", 6),
	}
	// 时间顺序、倒序、乱序三种重放顺序结果必须一样
	for name, order := range map[string][]int{
		"时间顺序": {0, 1, 2, 3, 4, 5},
		"倒序":   {5, 4, 3, 2, 1, 0},
		"乱序":   {3, 0, 5, 2, 4, 1},
	} {
		b := NewBook()
		for _, i := range order {
			o := *orders[i] // 拷贝一份，Match会改Remaining
			fills, selfCanceled := b.Match(&o)
			if len(fills) != 0 || len(selfCanceled) != 0 {
				t.Fatalf("%s: 互不相交的挂单重放不该有成交, orderId=%d got %d fills", name, o.OrderID, len(fills))
			}
			if o.Remaining.Sign() > 0 {
				b.Rest(&o)
			}
		}
		depth := b.Depth(0)
		if len(depth.Bids) != 3 || len(depth.Asks) != 2 {
			t.Fatalf("%s: 重放后应该全部挂在簿子上, got bids=%d asks=%d", name, len(depth.Bids), len(depth.Asks))
		}
	}
}

// 落库了但引擎还没处理过的订单(引擎宕机/落后期间，下单接口已经落库、事件还堆在Kafka里)，
// 重启恢复时它们是交叉的。之前恢复直接Rest不Match，随后Kafka里的下单事件又被当成重复跳过，
// 这两笔单会永远交叉挂在簿子上不成交；重放进Match就能正常撮合
func TestBook_ReplayCrossedPairThroughMatch_Fills(t *testing.T) {
	b := NewBook()
	sell := newResting(1, 1, Sell, "60000", "0.1", 1)
	buy := newResting(2, 2, Buy, "60000", "0.1", 2)

	if fills, _ := b.Match(sell); len(fills) != 0 {
		t.Fatal("第一笔进空簿子不该有成交")
	}
	b.Rest(sell)

	fills, _ := b.Match(buy)
	if len(fills) != 1 || !fills[0].Volume.Equal(d("0.1")) || !fills[0].Price.Equal(d("60000")) {
		t.Fatalf("交叉的一对应该撮合成一笔0.1@60000, got %+v", fills)
	}
	if buy.Remaining.Sign() != 0 {
		t.Errorf("买单应该被吃完, remaining=%s", buy.Remaining)
	}
	if depth := b.Depth(0); len(depth.Bids) != 0 || len(depth.Asks) != 0 {
		t.Errorf("成交后簿子应该是空的, got %+v", depth)
	}
}

// ---- 冲击价格 ----

// 期望值都按定义手算：按名义金额(价格*数量)从最优价往深处吃，平均成交价=名义金额/吃到的总数量
func TestBook_ImpactPrices(t *testing.T) {
	// 买盘：100(2个=200)、99(3个=297)、98(10个=980)；卖盘：101(1个=101)、102(4个=408)、110(5个=550)
	build := func() *Book {
		b := NewBook()
		b.Rest(newResting(1, 1, Buy, "100", "2", 1))
		b.Rest(newResting(2, 2, Buy, "99", "3", 2))
		b.Rest(newResting(3, 3, Buy, "98", "10", 3))
		b.Rest(newResting(4, 4, Sell, "101", "1", 4))
		b.Rest(newResting(5, 5, Sell, "102", "4", 5))
		b.Rest(newResting(6, 6, Sell, "110", "5", 6))
		return b
	}
	cases := []struct {
		name     string
		notional string
		bid, ask string // 期望的冲击买价、卖价，空=这一侧凑不够
	}{
		// 卖盘：101档只有101名义价值，200要吃到102档：1+99/102个，均价=200/(1+99/102)=6800/67
		{"金额在第一档以内：就是最优价", "100", "100", "101"},
		{"恰好吃完买盘第一档(200)：均价还是100；卖盘吃到第二档", "200", "100", "101.4925373134"},
		// 买盘：2个在100，剩100在99档：100/99个，均价=300/(2+100/99)=14850/149；卖盘：1个在101，剩199在102档，均价=300/(1+199/102)=30600/301
		{"吃穿买盘第一档", "300", "99.6644295302", "101.6611295681"},
		// 买盘：200+297=497吃完前两档共5个，剩12在98档，均价=509/(5+12/98)=24941/251；卖盘：101+408=509恰好吃完前两档共5个，均价=509/5
		{"卖盘刚好吃完前两档(101+408=509)", "509", "99.3665338645", "101.8"},
		// 卖盘总名义价值101+408+550=1059，金额恰好等于总深度时还算够(吃完最后一档、没有剩余)：均价=1059/(1+4+5)=105.9；
		// 买盘：吃完前两档5个(497)，剩562在98档，均价=1059/(5+562/98)=51891/526
		{"金额恰好等于卖盘总深度(1059)：还算够", "1059", "98.6520912548", "105.9"},
		{"买盘够(1477)，但卖盘总共只有1059：卖侧不够，整体不可用", "1477", "", ""},
		{"超过卖盘总额1059一点点：卖侧不够", "1060", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bid, ask, ok := build().ImpactPrices(d(c.notional))
			if c.bid == "" {
				if ok {
					t.Fatalf("应该凑不够: bid=%s ask=%s", bid, ask)
				}
				return
			}
			if !ok {
				t.Fatal("应该凑得够")
			}
			if !bid.Round(10).Equal(d(c.bid).Round(10)) || !ask.Round(10).Equal(d(c.ask).Round(10)) {
				t.Fatalf("bid=%s ask=%s, want bid=%s ask=%s", bid, ask, c.bid, c.ask)
			}
		})
	}
}

// 任何一侧不够深就整体不可用，不能拿一侧的价格凑合(否则撤掉一侧挂单就能控制溢价)
func TestBook_ImpactPrices_OneSidedOrEmptyBookIsUnavailable(t *testing.T) {
	empty := NewBook()
	if _, _, ok := empty.ImpactPrices(d("100")); ok {
		t.Fatal("空盘口应该不可用")
	}
	onlyBids := NewBook()
	onlyBids.Rest(newResting(1, 1, Buy, "100", "100", 1))
	if _, _, ok := onlyBids.ImpactPrices(d("100")); ok {
		t.Fatal("只有买盘应该不可用")
	}
	onlyAsks := NewBook()
	onlyAsks.Rest(newResting(1, 1, Sell, "100", "100", 1))
	if _, _, ok := onlyAsks.ImpactPrices(d("100")); ok {
		t.Fatal("只有卖盘应该不可用")
	}
}

// 名义金额<=0没有意义，不返回价格
func TestBook_ImpactPrices_NonPositiveNotional(t *testing.T) {
	b := NewBook()
	b.Rest(newResting(1, 1, Buy, "100", "10", 1))
	b.Rest(newResting(2, 2, Sell, "101", "10", 2))
	for _, n := range []string{"0", "-1"} {
		if _, _, ok := b.ImpactPrices(d(n)); ok {
			t.Fatalf("名义金额%s应该不可用", n)
		}
	}
}

// 只读：算冲击价格不改订单簿
func TestBook_ImpactPrices_DoesNotMutateBook(t *testing.T) {
	b := NewBook()
	b.Rest(newResting(1, 1, Buy, "100", "10", 1))
	b.Rest(newResting(2, 2, Sell, "101", "10", 2))
	before := b.Depth(0)
	b.ImpactPrices(d("500"))
	after := b.Depth(0)
	if len(before.Bids) != len(after.Bids) || !before.Bids[0].Volume.Equal(after.Bids[0].Volume) ||
		!before.Asks[0].Volume.Equal(after.Asks[0].Volume) {
		t.Fatal("算冲击价格不该改订单簿")
	}
}

// 抗操纵：在买一挂一个远高于市场的小单，最优价被拉高，但冲击价格只是被拉动一点点——
// 要拉动冲击价格必须摆出至少名义金额那么大的单子
func TestBook_ImpactPrices_TinyOrderAtTheTouchBarelyMovesIt(t *testing.T) {
	b := NewBook()
	b.Rest(newResting(1, 1, Buy, "100", "100", 1)) // 真实的买盘：100档，10000名义价值
	b.Rest(newResting(2, 2, Sell, "101", "100", 2))
	baseline, _, _ := b.ImpactPrices(d("1000"))

	b.Rest(newResting(3, 3, Buy, "130", "0.1", 3)) // 操纵：买一被拉到130，但只有13的名义价值
	if best := b.Depth(1).Bids[0].Price; !best.Equal(d("130")) {
		t.Fatalf("买一应该被拉到130: %s", best)
	}
	manipulated, _, ok := b.ImpactPrices(d("1000"))
	if !ok {
		t.Fatal("应该可用")
	}
	// 1000名义：13吃在130，剩987吃在100 -> 总数量0.1+9.87=9.97，均价1000/9.97≈100.3009，
	// 而买一中价会被直接拉到(130+101)/2
	if !manipulated.Round(4).Equal(d("100.3009")) {
		t.Fatalf("冲击买价 = %s, want 约100.3009(基线%s)", manipulated, baseline)
	}
}

// 单档成交时冲击价必须恰好等于这一档的价格，不能带除法的舍入噪声：
// 名义金额10000吃65065这一档，先算数量再反推均价，如果中间的除法精度不够会得到65064.99999999935
func TestBook_ImpactPrices_SingleLevelIsExact(t *testing.T) {
	for _, price := range []string{"65065", "3000.5", "0.0123", "97431.27"} {
		b := NewBook()
		b.Rest(newResting(1, 1, Buy, price, "1000000", 1))
		b.Rest(newResting(2, 2, Sell, price, "1000000", 2))
		bid, ask, ok := b.ImpactPrices(d("10000"))
		if !ok || !bid.Equal(d(price)) || !ask.Equal(d(price)) {
			t.Fatalf("价格%s: bid=%s ask=%s ok=%v, want 恰好等于%s", price, bid, ask, ok, price)
		}
	}
}
