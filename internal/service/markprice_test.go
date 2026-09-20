package service

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestMedian3_AllOrders(t *testing.T) {
	vals := []string{"1", "2", "3"}
	perms := [][3]int{{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}}
	for _, p := range perms {
		if got := median3(d(vals[p[0]]), d(vals[p[1]]), d(vals[p[2]])); !got.Equal(d("2")) {
			t.Errorf("median3 顺序%v = %s, want 2", p, got)
		}
	}
	if got := median3(d("5"), d("5"), d("1")); !got.Equal(d("5")) {
		t.Errorf("有重复值: got %s, want 5", got)
	}
}

// 期望值都按公式手算：标记价=中位数(指数价, 指数价*(1+基差), 最新成交价)，再夹在指数价±maxDev之内
func TestComputeMark(t *testing.T) {
	cases := []struct {
		name    string
		index   string
		basis   string
		last    string // 空=没有成交
		maxDev  string
		want    string
		comment string
	}{
		{"基差为0时成交价往上拉不动标记价", "100", "0", "120", "0.01", "100", "中位数(100,100,120)"},
		{"基差为0时成交价往下砸不动标记价", "100", "0", "80", "0.01", "100", "中位数(100,100,80)"},
		{"正基差：成交价远高于指数价，标记价只到指数价加基差", "100", "0.005", "120", "0.01", "100.5", "中位数(100,100.5,120)"},
		{"正基差：成交价落在指数价和加基差价之间就用成交价", "100", "0.005", "100.2", "0.01", "100.2", "中位数(100,100.5,100.2)"},
		{"正基差：成交价低于指数价，标记价回到指数价", "100", "0.005", "80", "0.01", "100", "中位数(100,100.5,80)"},
		{"负基差：成交价远低于指数价，标记价只到指数价减基差", "100", "-0.005", "80", "0.01", "99.5", "中位数(100,99.5,80)"},
		{"没有成交时用指数价加基差", "100", "0.004", "", "0.01", "100.4", ""},
		{"没有成交没有基差就是指数价", "100", "0", "", "0.01", "100", ""},
		{"基差超过上限先夹到上限", "100", "0.05", "200", "0.01", "101", "基差夹成1%后中位数(100,101,200)"},
		{"负基差超过上限", "100", "-0.05", "1", "0.01", "99", "基差夹成-1%后中位数(100,99,1)"},
		{"maxDev为0不限制", "100", "0.05", "200", "0", "105", "中位数(100,105,200)"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			last, hasLast := decimal.Zero, false
			if c.last != "" {
				last, hasLast = d(c.last), true
			}
			got := computeMark(d(c.index), d(c.basis), last, hasLast, d(c.maxDev))
			if !got.Equal(d(c.want)) {
				t.Fatalf("computeMark = %s, want %s (%s)", got, c.want, c.comment)
			}
		})
	}
}

// 性质：不管最新成交价是多少，标记价一定落在[指数价, 指数价*(1+基差)]这个区间里(基差为负时区间反过来)，
// 而且一定在指数价±maxDev之内——最新成交价被操纵不能把标记价带出这个区间
func TestComputeMark_NeverLeavesIndexBasisRange(t *testing.T) {
	index := d("100")
	maxDev := d("0.01")
	for _, basis := range []string{"-0.02", "-0.005", "0", "0.003", "0.01", "0.03"} {
		for _, last := range []string{"1", "50", "99", "99.5", "100", "100.5", "101", "150", "100000"} {
			mark := computeMark(index, d(basis), d(last), true, maxDev)
			clamped := clampDecimal(d(basis), maxDev.Neg(), maxDev)
			lo, hi := index, index.Mul(decimal.NewFromInt(1).Add(clamped))
			if lo.GreaterThan(hi) {
				lo, hi = hi, lo
			}
			if mark.LessThan(lo) || mark.GreaterThan(hi) {
				t.Errorf("basis=%s last=%s: mark=%s 跑出了[%s, %s]", basis, last, mark, lo, hi)
			}
			if mark.LessThan(d("99")) || mark.GreaterThan(d("101")) {
				t.Errorf("basis=%s last=%s: mark=%s 超出指数价±1%%", basis, last, mark)
			}
		}
	}
}

func TestBasisSample(t *testing.T) {
	cases := []struct {
		name     string
		bid, ask string
		maxDev   string
		want     string
		ok       bool
	}{
		{"中价高于指数价0.5%", "100.4", "100.6", "0.01", "0.005", true},
		{"中价低于指数价", "99.4", "99.6", "0.01", "-0.005", true},
		{"中价等于指数价", "99.9", "100.1", "0.01", "0", true},
		{"没有买一不采样", "0", "100.1", "0.01", "", false},
		{"没有卖一不采样", "99.9", "0", "0.01", "", false},
		{"买卖倒挂不采样", "100.2", "100", "0.01", "", false},
		{"价差超过2倍maxDev(4%>2%)不采样", "98", "102", "0.01", "", false},
		{"价差刚好2倍maxDev内(1.99%)采样", "99.01", "100.99", "0.01", "0", true},
		{"样本夹到上限", "106.9", "107.1", "0.01", "0.01", true},
		{"样本夹到下限", "92.9", "93.1", "0.01", "-0.01", true},
		{"maxDev为0不限制也不查价差", "98", "112", "0", "0.05", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := basisSample(d("100"), d(c.bid), d(c.ask), d(c.maxDev))
			if ok != c.ok {
				t.Fatalf("ok = %v, want %v (got %s)", ok, c.ok, got)
			}
			if ok && !got.Equal(d(c.want)) {
				t.Fatalf("sample = %s, want %s", got, c.want)
			}
		})
	}
}

func TestBasisWindow_AverageAndEviction(t *testing.T) {
	var w basisWindow
	window := 60 * time.Second
	if got := w.average(0, window); !got.IsZero() {
		t.Fatalf("空窗口应该是0，got %s", got)
	}
	w.add(0, d("0.01"))
	w.add(30_000, d("0.03"))
	if got := w.average(30_000, window); !got.Equal(d("0.02")) {
		t.Fatalf("两个样本都在窗口内: got %s, want 0.02", got)
	}
	// now=70000，窗口起点10000：t=0的样本已经过期，只剩0.03
	if got := w.average(70_000, window); !got.Equal(d("0.03")) {
		t.Fatalf("第一个样本过期: got %s, want 0.03", got)
	}
	// 样本恰好在窗口起点上算窗口内(cutoff=30000，ts=30000不被丢)
	if got := w.average(90_000, window); !got.Equal(d("0.03")) {
		t.Fatalf("恰好在窗口边界上的样本应该保留: got %s, want 0.03", got)
	}
	if got := w.average(100_000, window); !got.IsZero() {
		t.Fatalf("全部过期应该回到0: got %s", got)
	}
}
