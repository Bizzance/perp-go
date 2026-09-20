package main

import (
	"reflect"
	"testing"

	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func lv(price, qty string) level { return level{price: d(price), qty: d(qty)} }

func TestNiceStep(t *testing.T) {
	cases := map[string]string{
		"4":      "5",   // BTC: 80000*0.00005
		"0.115":  "0.2", // ETH
		"1":      "1",
		"1.1":    "2",
		"5":      "5",
		"5.1":    "10",
		"0.03":   "0.05",
		"0.0007": "0.001",
		"0":      "1",
	}
	for in, want := range cases {
		if got := niceStep(d(in)); !got.Equal(d(want)) {
			t.Errorf("niceStep(%s) = %s, want %s", in, got, want)
		}
	}
}

// 买盘向下取整到桶边界：80317.5、80317.4落在80315桶，80314.9落在80310桶；同一个桶的数量累加
func TestAggregate_BidsFloorAndSumWithinBucket(t *testing.T) {
	bids := []level{lv("80317.5", "4.56"), lv("80317.4", "0.003"), lv("80315.0", "1"), lv("80314.9", "2"), lv("80309.9", "3")}

	got := aggregate(bids, d("5"), true, 10)

	want := []level{lv("80315", "5.563"), lv("80310", "2"), lv("80305", "3")}
	if !levelsEqual(got, want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
}

// 卖盘向上取整：80317.6落在80320桶，80320.0也在80320桶，80320.1落在80325桶
func TestAggregate_AsksCeilAndSumWithinBucket(t *testing.T) {
	asks := []level{lv("80317.6", "7.1"), lv("80320.0", "1"), lv("80320.1", "2"), lv("80326", "4")}

	got := aggregate(asks, d("5"), false, 10)

	want := []level{lv("80320", "8.1"), lv("80325", "2"), lv("80330", "4")}
	if !levelsEqual(got, want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
}

// 最多取k个桶，第k+1个桶的数量不能混进去
func TestAggregate_LimitsToKBuckets(t *testing.T) {
	asks := []level{lv("100.1", "1"), lv("105.1", "1"), lv("110.1", "1"), lv("115.1", "1")}
	got := aggregate(asks, d("5"), false, 2)
	if len(got) != 2 || !got[0].price.Equal(d("105")) || !got[1].price.Equal(d("110")) {
		t.Fatalf("应该只有前2个桶: %v", got)
	}
}

// 买一桶一定低于卖一桶，不管币安的价差多小，我们自己的两侧挂单不会交叉
func TestAggregate_BestBidBucketBelowBestAskBucket(t *testing.T) {
	for _, tc := range []struct{ bid, ask string }{
		{"80317.5", "80317.6"}, {"80320.0", "80320.1"}, {"80319.9", "80320.0"}, {"80315", "80315.1"},
	} {
		b := aggregate([]level{lv(tc.bid, "1")}, d("5"), true, 1)[0].price
		a := aggregate([]level{lv(tc.ask, "1")}, d("5"), false, 1)[0].price
		if !b.LessThan(a) {
			t.Errorf("买%s/卖%s聚合后买桶%s应该低于卖桶%s", tc.bid, tc.ask, b, a)
		}
	}
}

func TestScaleQty(t *testing.T) {
	min := d("0.001")
	if got := scaleQty(d("4.5678"), d("0.5"), 3, min); !got.Equal(d("2.283")) {
		t.Errorf("向下取到3位: %s", got)
	}
	if got := scaleQty(d("0.001"), d("0.5"), 3, min); !got.IsZero() {
		t.Errorf("低于最小下单量的档位不挂: %s", got)
	}
}

func TestPlan_PlacesMissingAndCancelsStale(t *testing.T) {
	desired := []level{lv("100", "1"), lv("99", "1"), lv("98", "1")}
	existing := []liveOrder{
		{id: "a", price: d("100"), amount: d("1"), remaining: d("1")},  // 保留
		{id: "b", price: d("101"), amount: d("1"), remaining: d("1")},  // 价格不在期望里，撤
		{id: "c", price: d("99.0"), amount: d("1"), remaining: d("1")}, // 保留(价格写法不同也要认出是同一价位)
	}

	cancels, places := plan(desired, existing)

	if !reflect.DeepEqual(cancels, []string{"b"}) {
		t.Errorf("应该只撤b: %v", cancels)
	}
	if len(places) != 1 || !places[0].price.Equal(d("98")) || !places[0].qty.Equal(d("1")) {
		t.Errorf("应该只补挂98: %v", places)
	}
}

// 被用户吃掉大半的挂单撤掉重挂补回期望量，只吃掉一点点的保留(不要为了数量小幅变化天天撤了重挂)
func TestPlan_RefillsWhenMostlyEatenButKeepsMinorlyEaten(t *testing.T) {
	desired := []level{lv("100", "2"), lv("99", "2")}
	existing := []liveOrder{
		{id: "eaten", price: d("100"), amount: d("2"), remaining: d("0.9")}, // 不到期望量一半
		{id: "minor", price: d("99"), amount: d("2"), remaining: d("1.5")},  // 还有3/4
	}

	cancels, places := plan(desired, existing)

	if !reflect.DeepEqual(cancels, []string{"eaten"}) {
		t.Errorf("应该只撤被吃掉大半的: %v", cancels)
	}
	if len(places) != 1 || !places[0].price.Equal(d("100")) || !places[0].qty.Equal(d("2")) {
		t.Errorf("应该在100重新挂满2: %v", places)
	}
}

// 同一个价位重复挂了多笔，只保留一笔
func TestPlan_DeduplicatesSamePrice(t *testing.T) {
	desired := []level{lv("100", "1")}
	existing := []liveOrder{
		{id: "1", price: d("100"), amount: d("1"), remaining: d("1")},
		{id: "2", price: d("100"), amount: d("1"), remaining: d("1")},
		{id: "3", price: d("100"), amount: d("1"), remaining: d("1")},
	}
	cancels, places := plan(desired, existing)
	if len(cancels) != 2 || len(places) != 0 {
		t.Fatalf("应该撤2笔重复的、不补挂: cancels=%v places=%v", cancels, places)
	}
}

// 期望档位没变、挂单都在：什么都不动
func TestPlan_NoChangeIsNoop(t *testing.T) {
	desired := []level{lv("100", "1"), lv("99", "1")}
	existing := []liveOrder{{id: "a", price: d("100"), amount: d("1"), remaining: d("1")}, {id: "b", price: d("99"), amount: d("1"), remaining: d("1")}}
	cancels, places := plan(desired, existing)
	if len(cancels) != 0 || len(places) != 0 {
		t.Fatalf("没有变化不应该有任何动作: %v %v", cancels, places)
	}
}

// 数量为0的期望档位不挂
func TestPlan_SkipsZeroQty(t *testing.T) {
	_, places := plan([]level{lv("100", "0")}, nil)
	if len(places) != 0 {
		t.Fatalf("数量0不挂: %v", places)
	}
}

func levelsEqual(a, b []level) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].price.Equal(b[i].price) || !a[i].qty.Equal(b[i].qty) {
			return false
		}
	}
	return true
}

// 币安某个价位的挂单量涨了很多，我们已经挂着的单子(只是没被吃掉)不动：跟下单时的数量比，不跟当前期望量比
func TestPlan_DoesNotChurnWhenBinanceQtyChanges(t *testing.T) {
	desired := []level{lv("100", "50")} // 期望量从1涨到50
	existing := []liveOrder{{id: "a", price: d("100"), amount: d("1"), remaining: d("1")}}
	cancels, places := plan(desired, existing)
	if len(cancels) != 0 || len(places) != 0 {
		t.Fatalf("只是币安的量变了，不应该撤了重挂: %v %v", cancels, places)
	}
}

func TestRampOffset(t *testing.T) {
	step := d("1")
	cases := []struct{ cur, target, want string }{
		{"0", "-5", "-1"}, {"-1", "-5", "-2"}, {"-4.5", "-5", "-5"}, {"-5", "-5", "-5"},
		{"-8", "0", "-7"}, {"0.4", "0", "0"}, {"2", "10", "3"},
	}
	for _, c := range cases {
		if got := rampOffset(d(c.cur), d(c.target), step); !got.Equal(d(c.want)) {
			t.Errorf("rampOffset(%s -> %s) = %s, want %s", c.cur, c.target, got, c.want)
		}
	}
}

func TestShiftLevelsAndFactor(t *testing.T) {
	got := shiftLevels([]level{lv("100", "2"), lv("99", "3")}, offsetFactor(d("-10")))
	if !levelsEqual(got, []level{lv("90", "2"), lv("89.1", "3")}) {
		t.Fatalf("价格乘0.9、数量不变: %v", got)
	}
	if !offsetFactor(d("0")).Equal(one) {
		t.Fatal("偏移0的系数是1")
	}
}
