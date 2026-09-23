//go:build integration

package service_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
	"perp-go/internal/service"
	"perp-go/internal/testutil"
)

// 标记价的期望值都按公式手算：中位数(指数价, 指数价*(1+基差均值), 最新成交价)，夹在指数价±1%之内。
// 盘口基差样本取整齐的值：指数价60000，买一卖一中价60300 -> 基差0.005 -> 指数价加基差=60300。

// 可以手动拨的时钟：指数价的新鲜度和基差窗口都按它判断，测试里不用真的等
type fakeClock struct{ ms atomic.Int64 }

func (c *fakeClock) now() int64              { return c.ms.Load() }
func (c *fakeClock) advance(d time.Duration) { c.ms.Add(d.Milliseconds()) }
func newFakeClock() *fakeClock               { c := &fakeClock{}; c.ms.Store(time.Now().UnixMilli()); return c }

// 把这个env的标记价服务换成手动时钟(Redis里跟标记价有关的键newEngineEnv已经清过，结束时也会清)
func (e *engineEnv) useFakeClock(t *testing.T) *fakeClock {
	t.Helper()
	c := newFakeClock()
	e.markPrice.WithClock(c.now)
	return c
}

func (e *engineEnv) mark(t *testing.T) decimal.Decimal {
	t.Helper()
	m, ok := e.markPrice.Get(context.Background(), testSymbol)
	if !ok {
		t.Fatal("应该有标记价")
	}
	return m
}

func (e *engineEnv) feedIndex(t *testing.T, price string) {
	t.Helper()
	if err := e.markPrice.SetIndexPrice(context.Background(), testSymbol, decimalOf(t, price)); err != nil {
		t.Fatal(err)
	}
}

func (e *engineEnv) trade(t *testing.T, price string) {
	t.Helper()
	if err := e.markPrice.UpdateFromTrade(context.Background(), testSymbol, decimalOf(t, price)); err != nil {
		t.Fatal(err)
	}
}

func (e *engineEnv) refresh(t *testing.T, bid, ask string) {
	t.Helper()
	if err := e.markPrice.Refresh(context.Background(), testSymbol, decimalOf(t, bid), decimalOf(t, ask)); err != nil {
		t.Fatal(err)
	}
}

// ---- 操纵：最新成交价动不了标记价 ----

// 对敲把最新成交价拉到78000或砸到42000(偏离30%)，标记价纹丝不动：盘口中价等于指数价、基差为0，
// 中位数(60000, 60000, 成交价)恒等于60000
func TestMarkPrice_WashTradeCannotMoveMark(t *testing.T) {
	e := newEngineEnv(t)
	clock := e.useFakeClock(t)
	e.feedIndex(t, "60000")
	for i := 0; i < 3; i++ {
		e.refresh(t, "59990", "60010")
		clock.advance(time.Second)
		e.feedIndex(t, "60000")
	}
	mustDec(t, e.mark(t), "60000", "没有成交时标记价=指数价")

	e.trade(t, "78000")
	mustDec(t, e.mark(t), "60000", "成交价拉高30%，标记价不动")
	e.trade(t, "42000")
	mustDec(t, e.mark(t), "60000", "成交价砸低30%，标记价不动")
}

// 成交价落在指数价和"指数价加基差"之间才会影响标记价，而且只在这个区间里动
func TestMarkPrice_LastTradeOnlyMovesMarkInsideIndexAndBasisRange(t *testing.T) {
	e := newEngineEnv(t)
	clock := e.useFakeClock(t)
	e.feedIndex(t, "60000")
	for i := 0; i < 3; i++ {
		e.refresh(t, "60290", "60310") // 中价60300，基差0.005
		clock.advance(time.Second)
		e.feedIndex(t, "60000")
	}
	mustDec(t, e.mark(t), "60300", "没有成交：指数价加基差")

	e.trade(t, "90000")
	mustDec(t, e.mark(t), "60300", "成交价远高于区间：取区间上沿")
	e.trade(t, "60100")
	mustDec(t, e.mark(t), "60100", "成交价在区间里：取成交价")
	e.trade(t, "50000")
	mustDec(t, e.mark(t), "60000", "成交价低于指数价：取指数价")
}

// 长时间把盘口摆偏也只能把标记价拉到指数价的1%：中价偏离10%，样本夹到1%
func TestMarkPrice_SkewedBookIsCappedAtMaxDeviation(t *testing.T) {
	e := newEngineEnv(t)
	e.useFakeClock(t)
	e.feedIndex(t, "60000")
	e.refresh(t, "66000", "66020") // 中价66010，偏离10%，样本夹到1%
	e.trade(t, "90000")
	mustDec(t, e.mark(t), "60600", "指数价60000的1%上限")
}

// 太薄的盘口(买卖价差超过最大偏离的2倍)中价没有意义，不采样：摆出一个又宽又偏的盘口影响不了基差
func TestMarkPrice_WideSpreadBookIsNotSampled(t *testing.T) {
	e := newEngineEnv(t)
	e.useFakeClock(t)
	e.feedIndex(t, "60000")
	e.refresh(t, "60600", "63000") // 价差4%，超过2%，跳过
	mustDec(t, e.mark(t), "60000", "没有采到样本，基差按0算")
}

// 基差是时间窗口内的平均：窗口外的样本不再起作用，标记价回到指数价
func TestMarkPrice_BasisSamplesExpireOutOfWindow(t *testing.T) {
	e := newEngineEnv(t)
	clock := e.useFakeClock(t)
	e.feedIndex(t, "60000")
	e.refresh(t, "60590", "60610") // 中价60600，基差0.01
	e.trade(t, "90000")
	mustDec(t, e.mark(t), "60600", "窗口内的基差生效")

	clock.advance(61 * time.Second) // 超过60秒窗口，同时指数价要重新喂，不然算断供
	e.feedIndex(t, "60000")
	e.refresh(t, "0", "0")
	mustDec(t, e.mark(t), "60000", "样本过期后回到指数价")
}

// 窗口是平均不是最新值：一个偏的样本被更多正常样本稀释
func TestMarkPrice_BasisIsAveragedOverWindow(t *testing.T) {
	e := newEngineEnv(t)
	clock := e.useFakeClock(t)
	e.feedIndex(t, "60000")
	e.refresh(t, "60590", "60610") // 基差0.01
	clock.advance(time.Second)
	e.feedIndex(t, "60000")
	e.refresh(t, "59990", "60010") // 基差0
	// 平均0.005 -> 60300
	mustDec(t, e.mark(t), "60300", "两个样本的平均")
}

// ---- 没有指数价 ----

// 开发环境(没配RequireIndex)：没喂过指数价时标记价=最新成交价，老的行为保留
func TestMarkPrice_WithoutIndexFallsBackToLastTrade(t *testing.T) {
	e := newEngineEnv(t)
	e.useFakeClock(t)
	e.trade(t, "65000")
	mustDec(t, e.mark(t), "65000", "标记价=最新成交价")
	if _, ok := e.markPrice.GetFresh(context.Background(), testSymbol); !ok {
		t.Fatal("没喂过指数价的开发环境，标记价应该算新鲜")
	}
	e.trade(t, "64000")
	mustDec(t, e.mark(t), "64000", "跟着成交价走")
}

// 生产配置(RequireIndex)：没有指数价就不产生标记价，不会退回可以被操纵的成交价；
// 指数价来了以后才有标记价；指数价又没了，已有的标记价算过期
func TestMarkPrice_RequireIndexRefusesLastTradeOnly(t *testing.T) {
	e := newEngineEnv(t)
	e.useFakeClock(t)
	cfg := service.DefaultMarkPriceConfig()
	cfg.RequireIndex = true
	e.markPrice.WithConfig(cfg)
	ctx := context.Background()

	e.trade(t, "65000")
	if _, ok := e.markPrice.Get(ctx, testSymbol); ok {
		t.Fatal("RequireIndex下没有指数价不应该产生标记价")
	}
	if _, st := e.markPrice.Lookup(ctx, testSymbol); st != service.MarkMissing {
		t.Fatalf("Lookup状态 = %v, want MarkMissing", st)
	}

	e.feedIndex(t, "60000")
	e.refresh(t, "0", "0")
	mustDec(t, e.mark(t), "60000", "指数价来了：中位数(60000,60000,65000)")
	if _, st := e.markPrice.Lookup(ctx, testSymbol); st != service.MarkFresh {
		t.Fatalf("Lookup状态 = %v, want MarkFresh", st)
	}

	if err := testutil.NewRedisClient(t).Del(ctx, "perpgo:index:"+testSymbol, "perpgo:index_ts:"+testSymbol).Err(); err != nil {
		t.Fatal(err)
	}
	if _, st := e.markPrice.Lookup(ctx, testSymbol); st != service.MarkStale {
		t.Fatalf("指数价没了Lookup状态 = %v, want MarkStale", st)
	}
}

// ---- 喂价断了 ----

// 指数价超过MaxIndexAge没更新：标记价冻结(成交也拉不动)，风控读法GetFresh拿不到；喂价恢复后跟上
func TestMarkPrice_StaleIndexFreezesMarkAndBlocksFreshReads(t *testing.T) {
	e := newEngineEnv(t)
	clock := e.useFakeClock(t)
	ctx := context.Background()
	e.feedIndex(t, "60000")
	e.refresh(t, "0", "0")
	if m, ok := e.markPrice.GetFresh(ctx, testSymbol); !ok || !m.Equal(decimalOf(t, "60000")) {
		t.Fatalf("喂价正常时GetFresh = %s %v, want 60000 true", m, ok)
	}

	clock.advance(30 * time.Second) // 恰好等于MaxIndexAge，还不算陈旧
	if _, ok := e.markPrice.GetFresh(ctx, testSymbol); !ok {
		t.Fatal("刚好30秒还不算陈旧")
	}
	clock.advance(time.Second)
	if _, ok := e.markPrice.GetFresh(ctx, testSymbol); ok {
		t.Fatal("31秒没喂价应该算陈旧")
	}
	if _, st := e.markPrice.Lookup(ctx, testSymbol); st != service.MarkStale {
		t.Fatalf("Lookup状态 = %v, want MarkStale", st)
	}
	// 成交价60300落在指数价和"指数价加基差"之间、盘口基差也偏正：不冻结的话标记价会被这两个输入拉到60300
	e.trade(t, "60300")
	e.refresh(t, "60590", "60610")
	mustDec(t, e.mark(t), "60000", "喂价断了标记价冻结在断之前的值")

	e.feedIndex(t, "61000")
	e.refresh(t, "0", "0")
	mustDec(t, e.mark(t), "61000", "喂价恢复：中位数(61000,61000,60300)，断供期间的盘口样本没有被采进去")
	if _, ok := e.markPrice.GetFresh(ctx, testSymbol); !ok {
		t.Fatal("喂价恢复后应该新鲜")
	}
}

// 标记价推送只在变了的时候发，不是每次重算都发
func TestMarkPrice_OnChangeFiresOnlyWhenMarkChanges(t *testing.T) {
	e := newEngineEnv(t)
	e.useFakeClock(t)
	var got []string
	e.markPrice.OnChange(func(_ context.Context, symbol string, mark decimal.Decimal) {
		if symbol == testSymbol {
			got = append(got, mark.String())
		}
	})
	e.feedIndex(t, "60000")
	e.refresh(t, "0", "0")
	e.refresh(t, "0", "0")
	e.trade(t, "90000") // 中位数还是60000
	if len(got) != 1 || got[0] != "60000" {
		t.Fatalf("没变化不应该再推送: %v", got)
	}
	e.feedIndex(t, "60500")
	e.refresh(t, "0", "0")
	if len(got) != 2 || got[1] != "60500" {
		t.Fatalf("指数价变了应该推送新值: %v", got)
	}
}

// ---- 引擎按盘口定时刷新 ----

// RefreshMarkPrices读订单簿的买一卖一采基差样本：买一60290、卖一60310 -> 中价60300 -> 基差0.005
func TestMarkPrice_EngineRefreshSamplesBookBasis(t *testing.T) {
	e := newEngineEnv(t)
	e.useFakeClock(t)
	a := e.newAccount(t, 1, "10000")
	b := e.newAccount(t, 2, "10000")
	e.restBid(t, a, "60290", "0.1", "603")
	ask := e.insertOrder(t, b, orderOpts{side: model.SideShort, action: model.ActionOpen, price: "60310", amount: "0.1", margin: "603"})
	if err := e.engine.SubmitOrder(context.Background(), ask, 9); err != nil {
		t.Fatal(err)
	}
	e.feedIndex(t, "60000")

	e.engine.RefreshMarkPrices(context.Background())

	mustDec(t, e.mark(t), "60300", "指数价60000加基差0.005")
}

// 分片部署：不负责这个symbol的实例不能刷新它的标记价。非owner的本地订单簿是空的、采不到基差样本，
// 它要是照样重算并写Redis，会把owner算好的标记价(带基差)覆盖成不带基差的值，两个实例来回打架
func TestMarkPrice_EngineRefreshSkipsSymbolsItDoesNotOwn(t *testing.T) {
	e := newEngineEnv(t)
	e.useFakeClock(t)
	e.restartEngineOwning(t, []string{"ETHUSDT"})
	e.feedIndex(t, "60000")

	e.engine.RefreshMarkPrices(context.Background())

	if m, ok := e.markPrice.Get(context.Background(), testSymbol); ok {
		t.Fatalf("不负责BTCUSDT的实例不应该写它的标记价, got %s", m)
	}
}

// ---- 风控读新鲜的标记价 ----

// 受害者是满仓的多头(强平点55250)。攻击者对敲把成交价砸到强平点：喂着指数价时标记价还是65000，
// 不会被强平。对照组见下一个用例
func TestLiquidation_WashTradeCannotLiquidateWhenIndexIsFed(t *testing.T) {
	e := newEngineEnv(t)
	e.useFakeClock(t)
	a := e.newAccount(t, 1, "1000")
	b := e.newAccount(t, 2, "10000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")
	e.feedIndex(t, "65000")
	e.refresh(t, "0", "0")

	e.trade(t, "55250") // 对敲，价格正好是这个仓位的强平点
	mustDec(t, e.mark(t), "65000", "标记价不被成交价拉走")
	e.liq.RiskScanOnce(context.Background())

	time.Sleep(300 * time.Millisecond) // 强平单是异步落库的，多等一会儿确认真的没有
	if n := len(e.liquidationOrders(t, a)); n != 0 {
		t.Fatalf("对敲不应该触发强平, got %d 笔强平委托", n)
	}
	if p := e.position(t, a, model.SideLong); p.Status != model.PositionStatusNormal || p.Volume.IsZero() {
		t.Fatalf("仓位应该原封不动: status=%s volume=%s", p.Status, p.Volume)
	}
}

// 对照组：没喂过指数价的开发环境，同样的对敲一笔就把标记价砸到强平点、受害者被强平。
// 这就是生产必须开RequireIndex并喂指数价的原因，见docs/mark-price.md
func TestLiquidation_WashTradeLiquidatesInLegacyModeWithoutIndex(t *testing.T) {
	e := newEngineEnv(t)
	e.useFakeClock(t)
	a := e.newAccount(t, 1, "1000")
	b := e.newAccount(t, 2, "10000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")
	e.setCredit(t, a, "0", true) // 投保(阈值199.35)才会在55250这个价位触发，未投保阈值是0

	e.trade(t, "55250")
	mustDec(t, e.mark(t), "55250", "没有指数价，标记价=最新成交价")
	e.liq.RiskScanOnce(context.Background())

	waitFor(t, "挂出强平委托", func() bool { return len(e.liquidationOrders(t, a)) == 1 })
	e.waitLiquidationDone(t, a, 0) // 权益是正的(21.75)，没有穿仓，不涉及保险基金
}

// 指数价断供：即使标记价已经在强平线以下，也不做强平判断；喂价恢复后才强平
func TestLiquidation_PausedWhileIndexIsStale(t *testing.T) {
	e := newEngineEnv(t)
	clock := e.useFakeClock(t)
	a := e.newAccount(t, 1, "1000")
	b := e.newAccount(t, 2, "10000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")
	e.setCredit(t, a, "0", true) // 投保(阈值199.35)才会在55250这个价位触发，未投保阈值是0
	// a还挂着一笔买单：误触发强平会先把它撤掉，所以它还在就说明连"触发"这一步都没有发生
	pending := e.insertOrder(t, a, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "50000", amount: "0.01", margin: "50"})
	if err := e.engine.SubmitOrder(context.Background(), pending, 9); err != nil {
		t.Fatal(err)
	}
	e.feedIndex(t, "55250")
	e.refresh(t, "0", "0")
	mustDec(t, e.mark(t), "55250", "指数价已经到了强平点")

	clock.advance(31 * time.Second) // 喂价断了
	e.liq.RiskScanOnce(context.Background())
	time.Sleep(300 * time.Millisecond)
	if n := len(e.liquidationOrders(t, a)); n != 0 {
		t.Fatalf("喂价断了不应该强平, got %d 笔强平委托", n)
	}
	if got := e.order(t, pending.OrderID).Status; got != model.OrderStatusOpen || !e.book.BookFor(testSymbol).Contains(pending.OrderID) {
		t.Fatalf("喂价断了用户的挂单不应该被撤, status=%s", got)
	}
	if p := e.position(t, a, model.SideLong); p.Status != model.PositionStatusNormal {
		t.Fatalf("仓位状态应该还是normal, got %s", p.Status)
	}

	e.feedIndex(t, "55250") // 喂价恢复
	e.liq.RiskScanOnce(context.Background())
	waitFor(t, "喂价恢复后挂出强平委托", func() bool { return len(e.liquidationOrders(t, a)) == 1 })
	e.waitLiquidationDone(t, a, 0) // 权益是正的(21.75)，没有穿仓，不涉及保险基金
}

// 资金费率：喂价断了不采样、不结算(用过期价格算出来的溢价率和资金费都不可信)，恢复后照常
func TestFunding_PausedWhileIndexIsStale(t *testing.T) {
	e := newEngineEnv(t)
	clock := e.useFakeClock(t)
	ctx := context.Background()
	e.fundingPositions(t)
	e.sampleFunding(t, "65065", "65000")
	_, count, err := testutil.NewCache(t).GetFundingAccumulator(ctx, testSymbol)
	if err != nil || count != 1 {
		t.Fatalf("喂价正常时应该采到1个样本, got %d %v", count, err)
	}

	clock.advance(31 * time.Second)
	e.funding.SampleOnce(ctx)
	_, count, _ = testutil.NewCache(t).GetFundingAccumulator(ctx, testSymbol)
	if count != 1 {
		t.Fatalf("喂价断了不应该再采样, got %d 个样本", count)
	}
	e.funding.SettleIfDue(ctx, fundingNow1)
	if rows := e.fundingHistoryRows(t); len(rows) != 0 {
		t.Fatalf("喂价断了不应该结算, got %d 条记录", len(rows))
	}

	e.setIndex(t, testSymbol, "65000") // 喂价恢复
	e.funding.SettleIfDue(ctx, fundingNow1)
	if rows := e.fundingHistoryRows(t); len(rows) != 1 {
		t.Fatalf("喂价恢复后应该结算, got %d 条记录", len(rows))
	}
}

// 条件单：喂价断了不触发，恢复后触发
func TestConditionalOrder_NotTriggeredWhileIndexIsStale(t *testing.T) {
	e := newEngineEnv(t)
	clock := e.useFakeClock(t)
	ctx := context.Background()
	uid := e.newAccount(t, 1, "10000")
	acc := e.account(t, uid)
	if ok, err := e.accountRepo.FreezeFromBalance(ctx, acc.ID, decimalOf(t, "330")); err != nil || !ok {
		t.Fatalf("冻结保证金: ok=%v err=%v", ok, err)
	}
	co := newConditionalOpen(e, uid, "66000", "0.05", "330")
	if err := e.conditional.Insert(ctx, co); err != nil {
		t.Fatal(err)
	}
	e.feedIndex(t, "66500") // 已经满足触发条件
	e.refresh(t, "0", "0")

	clock.advance(31 * time.Second)
	e.condSvc.ScanOnce(ctx)
	pending, err := e.conditional.FindAllPending(ctx)
	if err != nil || len(pending) != 1 {
		t.Fatalf("喂价断了条件单不应该触发, pending=%d err=%v", len(pending), err)
	}

	e.feedIndex(t, "66500")
	e.condSvc.ScanOnce(ctx)
	pending, err = e.conditional.FindAllPending(ctx)
	if err != nil || len(pending) != 0 {
		t.Fatalf("喂价恢复后条件单应该触发, pending=%d err=%v", len(pending), err)
	}
}
