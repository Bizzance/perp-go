package booksync

import (
	"strings"
	"testing"

	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func lv(price, qty string) Level { return Level{Price: d(price), Qty: d(qty)} }

func ord(id, price, amount, remaining string) liveOrder {
	return liveOrder{id: id, price: d(price), amount: d(amount), remaining: d(remaining)}
}

func joinIDs(ids []string) string { return strings.Join(ids, ",") }

// 币安盘口的前n档：价格和数量原样，数量向下取到合约的小数位，取整后低于最小下单量的不挂，不够n档就有多少给多少
func TestDesiredLevels(t *testing.T) {
	levels := []Level{lv("100.1", "2"), lv("100.0", "0.4567"), lv("99.9", "0.0009"), lv("99.8", "5"), lv("99.7", "1")}
	cases := []struct {
		name string
		n    int
		dp   int32
		min  string
		want []Level
	}{
		{"前2档，价格数量原样", 2, 3, "0.001", []Level{lv("100.1", "2"), lv("100.0", "0.456")}},
		{"数量向下取到3位，0.0009取整成0不挂，顺延到后面的档位补够n档", 3, 3, "0.001", []Level{lv("100.1", "2"), lv("100.0", "0.456"), lv("99.8", "5")}},
		{"低于最小下单量的不挂", 5, 3, "1", []Level{lv("100.1", "2"), lv("99.8", "5"), lv("99.7", "1")}},
		{"n比档位数大：有多少给多少", 10, 3, "0.001", []Level{lv("100.1", "2"), lv("100.0", "0.456"), lv("99.8", "5"), lv("99.7", "1")}},
		{"小数位是0：数量取整数", 5, 0, "1", []Level{lv("100.1", "2"), lv("99.8", "5"), lv("99.7", "1")}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := desiredLevels(levels, c.n, c.dp, d(c.min))
			if len(got) != len(c.want) {
				t.Fatalf("got %v, want %v", got, c.want)
			}
			for i := range got {
				if !got[i].Price.Equal(c.want[i].Price) || !got[i].Qty.Equal(c.want[i].Qty) {
					t.Fatalf("第%d档 got %v, want %v", i, got[i], c.want[i])
				}
			}
		})
	}
	if got := desiredLevels(nil, 5, 3, d("0.001")); len(got) != 0 {
		t.Fatalf("空盘口: %v", got)
	}
}

func TestPlan(t *testing.T) {
	desired := []Level{lv("100", "1"), lv("99", "2"), lv("98", "3")}
	cases := []struct {
		name       string
		existing   []liveOrder
		wantCancel string
		wantPlace  []string // 价格
	}{
		{"什么都没挂：全部新挂", nil, "", []string{"100", "99", "98"}},
		{"已经一致：不动", []liveOrder{ord("1", "100", "1", "1"), ord("2", "99", "2", "2"), ord("3", "98", "3", "3")}, "", nil},
		{"价格不在期望里的撤掉，缺的补上", []liveOrder{ord("1", "100", "1", "1"), ord("2", "97", "2", "2")}, "2", []string{"99", "98"}},
		{"剩余量恰好是下单量的一半：还算够，保留", []liveOrder{ord("1", "100", "10", "5"), ord("2", "99", "2", "2"), ord("3", "98", "3", "3")}, "", nil},
		{"剩余量不到一半(被吃掉大半)：撤掉重挂", []liveOrder{ord("1", "100", "10", "4.9"), ord("2", "99", "2", "2"), ord("3", "98", "3", "3")}, "1", []string{"100"}},
		{"同一价位有重复挂单：保留id最小的，其余撤掉", []liveOrder{ord("2", "100", "1", "1"), ord("1", "100", "1", "1"), ord("3", "99", "2", "2"), ord("4", "98", "3", "3")}, "2", nil},
		{"币安数量变了不影响：跟下单时的数量比，不跟当前期望量比", []liveOrder{ord("1", "100", "0.1", "0.1"), ord("2", "99", "2", "2"), ord("3", "98", "3", "3")}, "", nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cancels, places := plan(desired, c.existing)
			if joinIDs(cancels) != c.wantCancel {
				t.Fatalf("cancels = %v, want %q", cancels, c.wantCancel)
			}
			var got []string
			for _, p := range places {
				got = append(got, p.price.String())
			}
			if strings.Join(got, ",") != strings.Join(c.wantPlace, ",") {
				t.Fatalf("places = %v, want %v", got, c.wantPlace)
			}
		})
	}
}

// 补挂的档位不能碰到对面还挂着的单子：买单必须严格低于还挂着的最低卖价，卖单必须严格高于最高买价
func TestPlacesBelowAndAbove(t *testing.T) {
	places := []placement{{price: d("99"), qty: d("1")}, {price: d("100"), qty: d("1")}, {price: d("101"), qty: d("1")}}
	prices := func(ps []placement) string {
		var s []string
		for _, p := range ps {
			s = append(s, p.price.String())
		}
		return strings.Join(s, ",")
	}
	if got := prices(placesBelow(places, d("100"), true)); got != "99" {
		t.Fatalf("placesBelow(100) = %s, want 99(严格低于)", got)
	}
	if got := prices(placesAbove(places, d("100"), true)); got != "101" {
		t.Fatalf("placesAbove(100) = %s, want 101(严格高于)", got)
	}
	if got := prices(placesBelow(places, decimal.Zero, false)); got != "99,100,101" {
		t.Fatalf("没有限制时全保留: %s", got)
	}
	if got := prices(placesAbove(places, decimal.Zero, false)); got != "99,100,101" {
		t.Fatalf("没有限制时全保留: %s", got)
	}
	orders := []liveOrder{ord("1", "100", "1", "1"), ord("2", "98", "1", "1"), ord("3", "103", "1", "1")}
	if p, ok := minLivePrice(orders); !ok || !p.Equal(d("98")) {
		t.Fatalf("minLivePrice = %s %v", p, ok)
	}
	if p, ok := maxLivePrice(orders); !ok || !p.Equal(d("103")) {
		t.Fatalf("maxLivePrice = %s %v", p, ok)
	}
	if _, ok := minLivePrice(nil); ok {
		t.Fatal("没有委托应该返回false")
	}
}

func TestDepthLimitFor(t *testing.T) {
	cases := map[int]int{1: 5, 5: 5, 6: 10, 20: 20, 21: 50, 50: 50, 51: 100, 100: 100, 101: 500, 1000: 1000}
	for n, want := range cases {
		if got, ok := depthLimitFor(n); !ok || got != want {
			t.Errorf("depthLimitFor(%d) = %d,%v want %d", n, got, ok, want)
		}
	}
	if _, ok := depthLimitFor(1001); ok {
		t.Error("超过1000应该不合法")
	}
}
