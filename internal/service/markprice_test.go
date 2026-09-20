package service

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/cache"
)

type cacheJump = cache.IndexJump

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

// ---- POST /index-price的服务端跳变保护(decideIndexPush) ----

// 阈值5%，确认3秒，断供阈值30秒。当前指数价60000，1秒前写入
func jumpCfg() MarkPriceConfig {
	cfg := DefaultMarkPriceConfig()
	cfg.IndexMaxJump = d("0.05")
	cfg.IndexJumpConfirm = 3 * time.Second
	return cfg
}

const jumpNow = int64(1_700_000_100_000) // 任意基准时间(毫秒)

// 单次推送的判断：只看价格相对当前值的变动
func TestDecideIndexPush_SingleShot(t *testing.T) {
	cases := []struct {
		name       string
		price      string
		curAgeMs   int64 // 当前指数价是多久前写入的
		hasCur     bool
		maxJump    string // 空=用jumpCfg的5%
		wantAccept bool
	}{
		{"正常波动1%：直接写", "60600", 1000, true, "", true},
		{"上涨恰好5%不算跳变(63000/60000)", "63000", 1000, true, "", true},
		{"上涨略超5%：拦下", "63001", 1000, true, "", false},
		{"下跌恰好5%不算跳变(57000)", "57000", 1000, true, "", true},
		{"下跌略超5%：拦下", "56999", 1000, true, "", false},
		{"翻倍：拦下", "120000", 1000, true, "", false},
		{"砸到接近0：拦下", "600", 1000, true, "", false},
		{"没有当前指数价(第一次喂)：不拦", "120000", 0, false, "", true},
		{"当前指数价恰好30秒：不算陈旧，照样拦", "120000", 30_000, true, "", false},
		{"当前指数价超过30秒：陈旧的价格不是可靠参照，不拦", "120000", 30_001, true, "", true},
		{"阈值是0(关闭)：什么都不拦", "120000", 1000, true, "0", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := jumpCfg()
			if c.maxJump != "" {
				cfg.IndexMaxJump = d(c.maxJump)
			}
			accept, next, continued := decideIndexPush(cfg, jumpNow, d(c.price), d("60000"), jumpNow-c.curAgeMs, c.hasCur, cacheJump{}, false)
			if accept != c.wantAccept {
				t.Fatalf("accept = %v, want %v", accept, c.wantAccept)
			}
			if continued {
				t.Fatal("没有待确认状态，不可能是延续")
			}
			if accept && next != (cacheJump{}) {
				t.Fatalf("接受时不该留待确认状态: %+v", next)
			}
			if !accept && (next.Target != c.price || next.FirstTs != jumpNow || next.LastTs != jumpNow) {
				t.Fatalf("拦下时要以这个价位新起一个待确认状态: %+v", next)
			}
		})
	}
}

// 待确认状态的推进：同一价位持续够久才承认
func TestDecideIndexPush_PendingProgression(t *testing.T) {
	cfg := jumpCfg()
	cur, curTs := d("60000"), jumpNow // 当前指数价一直是新鲜的
	pending := cacheJump{Target: "66000", FirstTs: jumpNow - 2000, LastTs: jumpNow - 1000}
	sec := func(ms int64) int64 { return jumpNow + ms }

	cases := []struct {
		name          string
		price         string
		now           int64
		pending       cacheJump
		hasPending    bool
		wantAccept    bool
		wantContinued bool
		wantFirst     int64 // 拦下时next.FirstTs
	}{
		{"同一价位、持续2.999秒：还不够", "66000", sec(999), pending, true, false, true, pending.FirstTs},
		{"同一价位、恰好持续3秒：承认", "66000", sec(1000), pending, true, true, true, 0},
		{"价位在容差内(66660恰好+1%)、持续够久：承认", "66660", sec(1000), pending, true, true, true, 0},
		{"价位略超容差(66661)：不是同一个价位，重新起头", "66661", sec(1000), pending, true, false, false, sec(1000)},
		{"换了个价位(72000)：重新起头，不继承时间", "72000", sec(500), pending, true, false, false, sec(500)},
		{"没有待确认状态：新起", "66000", sec(0), cacheJump{}, false, false, false, sec(0)},
		{"待确认状态里的价位坏了：当没有，新起", "66000", sec(500), cacheJump{Target: "abc", FirstTs: 1, LastTs: jumpNow - 1000}, true, false, false, sec(500)},
		{"待确认状态里的价位是0：当没有，新起", "66000", sec(500), cacheJump{Target: "0", FirstTs: 1, LastTs: jumpNow - 1000}, true, false, false, sec(500)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			accept, next, continued := decideIndexPush(cfg, c.now, d(c.price), cur, curTs, true, c.pending, c.hasPending)
			if accept != c.wantAccept || continued != c.wantContinued {
				t.Fatalf("accept=%v continued=%v, want %v %v", accept, continued, c.wantAccept, c.wantContinued)
			}
			if accept {
				if next != (cacheJump{}) {
					t.Fatalf("接受时不该留待确认状态: %+v", next)
				}
				return
			}
			if next.FirstTs != c.wantFirst || next.LastTs != c.now {
				t.Fatalf("next = %+v, want first=%d last=%d", next, c.wantFirst, c.now)
			}
		})
	}
}

// 连续性：两次推送间隔超过断供阈值(30秒)，不算"持续"，即使价位一样也要重新起头——
// 否则攻击者可以隔很久推一次同样的价，第二次就"持续够久"了
func TestDecideIndexPush_GapBreaksContinuity(t *testing.T) {
	cfg := jumpCfg()
	pending := cacheJump{Target: "66000", FirstTs: jumpNow - 100_000, LastTs: jumpNow - 30_000}
	// 间隔恰好30秒：还算连续
	accept, _, continued := decideIndexPush(cfg, jumpNow, d("66000"), d("60000"), jumpNow, true, pending, true)
	if !accept || !continued {
		t.Fatalf("间隔恰好30秒还算连续: accept=%v continued=%v", accept, continued)
	}
	// 间隔30.001秒：不连续
	pending.LastTs = jumpNow - 30_001
	accept, next, continued := decideIndexPush(cfg, jumpNow, d("66000"), d("60000"), jumpNow, true, pending, true)
	if accept || continued || next.FirstTs != jumpNow {
		t.Fatalf("间隔超过30秒要重新起头: accept=%v continued=%v next=%+v", accept, continued, next)
	}
}

// 确认时间可配置；配成0等于不设防(第一次推送就满足"持续0秒")，配置层不允许配成0，这里只锁定函数本身的行为
func TestDecideIndexPush_ConfirmDurationIsConfigurable(t *testing.T) {
	cfg := jumpCfg()
	cfg.IndexJumpConfirm = 10 * time.Second
	pending := cacheJump{Target: "66000", FirstTs: jumpNow - 9_999, LastTs: jumpNow - 1000}
	if accept, _, _ := decideIndexPush(cfg, jumpNow, d("66000"), d("60000"), jumpNow, true, pending, true); accept {
		t.Fatal("9.999秒还不够10秒")
	}
	pending.FirstTs = jumpNow - 10_000
	if accept, _, _ := decideIndexPush(cfg, jumpNow, d("66000"), d("60000"), jumpNow, true, pending, true); !accept {
		t.Fatal("恰好10秒应该承认")
	}
}
