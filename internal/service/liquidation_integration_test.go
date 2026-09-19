//go:build integration

package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
)

// 期望值都按经济公式手算：BTCUSDT档位1(名义价值<=50000)维持保证金率0.4%，强平单是taker(费率0.05%)，
// 保护价=标记价-标记价*维持保证金率*2(多头卖出平仓)。开仓统一是"多头a在65000买0.1，对手方b卖出"，
// 保证金650、开仓taker手续费3.25。

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("等待超时: %s", what)
}

func (e *engineEnv) fundBalance(t *testing.T) decimal.Decimal {
	t.Helper()
	b, err := e.fund.FreshBalance(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func (e *engineEnv) setFundBalance(t *testing.T, balance string) {
	t.Helper()
	if _, err := e.db.Exec(`UPDATE insurance_fund SET balance = ? WHERE id = 1`, balance); err != nil {
		t.Fatal(err)
	}
}

func (e *engineEnv) fundLedgerCount(t *testing.T) int {
	t.Helper()
	var n int
	if err := e.db.Get(&n, `SELECT COUNT(*) FROM insurance_fund_ledger`); err != nil {
		t.Fatal(err)
	}
	return n
}

func (e *engineEnv) fundLedgerAmount(t *testing.T, remark string) decimal.Decimal {
	t.Helper()
	var sum decimal.Decimal
	if err := e.db.Get(&sum, `SELECT COALESCE(SUM(amount), 0) FROM insurance_fund_ledger WHERE remark = ?`, remark); err != nil {
		t.Fatal(err)
	}
	return sum
}

func (e *engineEnv) liquidationOrders(t *testing.T, uid uint64) []model.Order {
	t.Helper()
	var orders []model.Order
	if err := e.db.Select(&orders, `SELECT * FROM orders WHERE uid = ? AND liquidation = 1 ORDER BY order_id`, uid); err != nil {
		t.Fatal(err)
	}
	return orders
}

// 挂一笔多头开仓限价单当对手盘(流动性)
func (e *engineEnv) restBid(t *testing.T, uid uint64, price, amount, margin string) {
	t.Helper()
	o := e.insertOrder(t, uid, orderOpts{side: model.SideLong, action: model.ActionOpen, price: price, amount: amount, margin: margin})
	if err := e.engine.SubmitOrder(context.Background(), o, 9); err != nil {
		t.Fatal(err)
	}
}

// 等一次强平完整结束：仓位清零，并且强平后的资金结算(保险基金记账)已经落了账
func (e *engineEnv) waitLiquidationDone(t *testing.T, uid uint64, wantFundLedgerRows int) {
	t.Helper()
	waitFor(t, "仓位被强平清零", func() bool { return e.position(t, uid, model.SideLong).Volume.IsZero() })
	waitFor(t, "强平后的保险基金记账", func() bool { return e.fundLedgerCount(t) >= wantFundLedgerRows })
}

// ---- 账户权益口径 ----

// 权益要把仓位占用的保证金和挂单冻结的保证金算进去：开仓只是钱换了个地方放，价格不动权益只被手续费拉低
func TestEquity_CountsPositionMarginAndFrozenMargin(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	a := e.newAccount(t, 1, "653.25")
	b := e.newAccount(t, 2, "10000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")

	v, err := e.accounts.View(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	mustDec(t, v.Available, "0", "开仓后可用余额")
	mustDec(t, v.PositionMargin, "650", "仓位保证金")
	mustDec(t, v.Equity, "650", "权益=0+仓位保证金650(653.25充值只被3.25手续费拉低)")

	// 挂一笔开仓单冻结600：钱只是从available挪到frozen，权益不变
	e.restBid(t, b, "60000", "0.1", "600")
	vb, _ := e.accounts.View(ctx, b)
	mustDec(t, vb.FrozenMargin, "600", "挂单冻结")
	// b: 10000 - 650保证金 - 1.3手续费(maker) = 9348.7，其中600冻结在挂单里，仓位保证金650
	mustDec(t, vb.Equity, "9998.7", "权益=可用+冻结+仓位保证金=8748.7+600+650")
}

// 回归：满仓开仓、价格一动不动，不能被强平。之前权益只算available，这种账户开仓瞬间权益就是0，
// 下一次风控扫描就把它强平了
func TestLiquidation_FullyUsedPositionIsNotLiquidatedWithoutPriceMove(t *testing.T) {
	e := newEngineEnv(t)
	a := e.newAccount(t, 1, "653.25")
	b := e.newAccount(t, 2, "10000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")

	e.liq.RiskScanOnce(context.Background())

	if p := e.position(t, a, model.SideLong); p.Status != model.PositionStatusNormal || p.Volume.IsZero() {
		t.Fatalf("价格没动，仓位不应该被强平: status=%s volume=%s", p.Status, p.Volume)
	}
	if n := len(e.liquidationOrders(t, a)); n != 0 {
		t.Fatalf("不应该有强平委托, got %d", n)
	}
}

// ---- 触发边界 ----

// a充值1000，开仓后权益=996.75+0.1*(标记价-65000)，维持保证金=0.1*标记价*0.4%。
// 标记价55300：权益26.75 > 维持22.12，不触发；标记价55250：权益21.75 <= 维持22.1，触发
func TestLiquidation_TriggerBoundary(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	a := e.newAccount(t, 1, "1000")
	b := e.newAccount(t, 2, "10000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")

	e.setMark(t, testSymbol, "55300")
	e.liq.RiskScanOnce(ctx)
	if p := e.position(t, a, model.SideLong); p.Status != model.PositionStatusNormal {
		t.Fatalf("权益高于维持保证金不应该触发, status=%s", p.Status)
	}
	if n := len(e.liquidationOrders(t, a)); n != 0 {
		t.Fatalf("不应该有强平委托, got %d", n)
	}

	e.setMark(t, testSymbol, "55250")
	e.liq.RiskScanOnce(ctx)
	// 强平单是异步goroutine里落库的，RiskScanOnce返回时可能还没写入
	waitFor(t, "挂出强平委托", func() bool { return len(e.liquidationOrders(t, a)) == 1 })
	e.waitLiquidationDone(t, a, 1) // 等异步的强平走完，别让它在测试库被删掉之后还在跑
}

// ---- 强平之后的资金结局 ----

// 强平单在订单簿里被吃掉，成交价是对手挂单的价格；平仓后还剩正数(维持保证金留下的缓冲)，
// 没有别的仓位了，这部分扫进保险基金，账户清零
func TestLiquidation_FilledAgainstBookSweepsBufferIntoFund(t *testing.T) {
	e := newEngineEnv(t)
	a := e.newAccount(t, 1, "1000")
	b := e.newAccount(t, 2, "10000")
	c := e.newAccount(t, 3, "10000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")
	e.restBid(t, c, "55300", "0.1", "553")
	e.setMark(t, testSymbol, "55250")

	e.liq.RiskScanOnce(context.Background())
	e.waitLiquidationDone(t, a, 1)

	orders := e.liquidationOrders(t, a)
	if len(orders) != 1 || orders[0].Status != model.OrderStatusFilled {
		t.Fatalf("强平委托应该在订单簿里成交, got %+v", orders)
	}
	// 保护价=55250-55250*0.004*2=54808，对手买单挂在55300(在保护价之上)，按对手价55300成交
	mustDec(t, orders[0].AvgDealPrice, "55300", "强平成交价")
	// 平仓盈亏(55300-65000)*0.1=-970，taker手续费5530*0.0005=2.765；
	// 平仓后可用=346.75+650保证金退回-970-2.765=23.985，全部扫进保险基金
	mustDec(t, e.ledgerSum(t, a, model.TxRealizedPnl), "-970", "已实现盈亏")
	acc := e.account(t, a)
	mustDec(t, acc.Available, "0", "缓冲扫进基金后账户清零")
	mustDec(t, acc.Credit, "0", "信用额度清零")
	mustDec(t, e.fundBalance(t), "23.985", "保险基金余额")
	mustDec(t, e.fundLedgerAmount(t, "全仓强平清算费(维持保证金缓冲)"), "23.985", "基金流水")
}

// 没有对手盘时靠超时兜底：撤掉强平单，按标记价直接结算，结果跟在订单簿成交是同一套账
func TestLiquidation_TimeoutFallbackSettlesAtMarkPrice(t *testing.T) {
	e := newEngineEnv(t)
	a := e.newAccount(t, 1, "1000")
	b := e.newAccount(t, 2, "10000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")
	e.setMark(t, testSymbol, "55250")

	e.liq.RiskScanOnce(context.Background())
	e.waitLiquidationDone(t, a, 1)

	orders := e.liquidationOrders(t, a)
	if len(orders) != 1 || orders[0].Status != model.OrderStatusFilled {
		t.Fatalf("兜底后强平委托应该是filled, got %+v", orders)
	}
	mustDec(t, orders[0].AvgDealPrice, "55250", "兜底按标记价结算")
	// (55250-65000)*0.1=-975，手续费5525*0.0005=2.7625；346.75+650-975-2.7625=18.9875
	mustDec(t, e.ledgerSum(t, a, model.TxRealizedPnl), "-975", "已实现盈亏")
	mustDec(t, e.fundBalance(t), "18.9875", "保险基金余额")
	mustDec(t, e.account(t, a).Available, "0", "账户清零")
	if e.book.BookFor(testSymbol).Contains(orders[0].OrderID) {
		t.Fatal("兜底之后强平单不应该还留在订单簿里")
	}
}

// 穿仓：a只充值700，标记价跌到50000，平仓后可用余额为负。保险基金够的话由基金垫付、不动ADL，
// 账户清零，对手方(空头b)的仓位不受影响
func TestLiquidation_ShortfallCoveredByFundWithoutADL(t *testing.T) {
	e := newEngineEnv(t)
	a := e.newAccount(t, 1, "700")
	b := e.newAccount(t, 2, "10000")
	c := e.newAccount(t, 3, "10000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")
	e.restBid(t, c, "50000", "0.1", "500")
	e.setFundBalance(t, "100000")
	e.setMark(t, testSymbol, "50000")

	e.liq.RiskScanOnce(context.Background())
	e.waitLiquidationDone(t, a, 1)

	// 平仓盈亏(50000-65000)*0.1=-1500，手续费5000*0.0005=2.5；46.75+650-1500-2.5=-805.75，缺口805.75
	mustDec(t, e.ledgerSum(t, a, model.TxRealizedPnl), "-1500", "已实现盈亏")
	acc := e.account(t, a)
	mustDec(t, acc.Available, "0", "穿仓由基金垫付后账户清零")
	mustDec(t, e.fundBalance(t), "99194.25", "基金余额=100000-805.75")
	mustDec(t, e.fundLedgerAmount(t, "强平穿仓垫付"), "-805.75", "垫付流水")
	mustDec(t, e.position(t, b, model.SideShort).Volume, "0.1", "基金够用，不应该动对手方的仓位(没有ADL)")
}

// 穿仓且基金不够：先用ADL强制减仓对手方(盈利的空头b)，把它刚实现的盈利划一部分给基金补缺口，
// 补完之后基金刚好抵消这笔垫付
func TestLiquidation_ShortfallTriggersADLWhenFundInsufficient(t *testing.T) {
	e := newEngineEnv(t)
	a := e.newAccount(t, 1, "700")
	b := e.newAccount(t, 2, "10000")
	c := e.newAccount(t, 3, "10000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")
	e.restBid(t, c, "50000", "0.1", "500")
	e.setFundBalance(t, "0")
	e.setMark(t, testSymbol, "50000")

	e.liq.RiskScanOnce(context.Background())
	e.waitLiquidationDone(t, a, 2) // ADL注入 + 穿仓垫付 两行

	// b空头盈利：(65000-50000)=15000/BTC，缺口805.75需要减仓805.75/15000=0.0537166666666667 BTC
	mustDec(t, e.fundLedgerAmount(t, "ADL强制减仓注入保险基金"), "805.75", "ADL注入基金的金额")
	mustDec(t, e.fundLedgerAmount(t, "强平穿仓垫付"), "-805.75", "垫付金额")
	mustDec(t, e.fundBalance(t), "0", "ADL补足缺口后基金刚好不亏")
	mustDec(t, e.position(t, b, model.SideShort).Volume, "0.0462833333333333", "b被强制减仓后剩余的空头")
	mustDec(t, e.account(t, a).Available, "0", "a账户清零")
}

// 多仓位账户：先平完的仓位结算后available可能暂时为负，但别的仓位的保证金还锁在仓位里，这时候
// 不能让基金垫付(也不能触发ADL)——等保证金退回来才知道有没有真的穿仓。确定性场景：直接构造
// "BTC仓位刚平完、available=-100，ETH仓位还开着"的状态
func TestLiquidation_ShortfallDeferredWhileAnotherPositionStillOpen(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	a := e.newAccount(t, 1, "5000")
	b := e.newAccount(t, 2, "50000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")
	e.openLongAgainst(t, a, b, "ETHUSDT", "3000", "1", "300")
	e.setFundBalance(t, "1000")
	if _, err := e.db.Exec(`UPDATE positions SET volume = 0, position_margin = 0, status = 'closed' WHERE uid = ? AND symbol = ?`, a, testSymbol); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.Exec(`UPDATE accounts SET available = -100 WHERE uid = ?`, a); err != nil {
		t.Fatal(err)
	}

	if err := e.engine.HandleLiquidationSettleAftermath(ctx, testSymbol, a, model.SideLong); err != nil {
		t.Fatal(err)
	}

	mustDec(t, e.account(t, a).Available, "-100", "还有仓位没平完时不能结清账户")
	mustDec(t, e.fundBalance(t), "1000", "基金不能提前垫付")
	if n := e.fundLedgerCount(t); n != 0 {
		t.Fatalf("不应该有基金流水, got %d", n)
	}

	// 最后一个仓位也平完(模拟：ETH仓位归零，保证金300退回账户)，这时才结算：合计为正200是缓冲，扫进基金
	if _, err := e.db.Exec(`UPDATE positions SET volume = 0, position_margin = 0, status = 'closed' WHERE uid = ? AND symbol = 'ETHUSDT'`, a); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.Exec(`UPDATE accounts SET available = 200 WHERE uid = ?`, a); err != nil {
		t.Fatal(err)
	}
	if err := e.engine.HandleLiquidationSettleAftermath(ctx, "ETHUSDT", a, model.SideLong); err != nil {
		t.Fatal(err)
	}
	mustDec(t, e.account(t, a).Available, "0", "全部仓位平完之后才结清")
	mustDec(t, e.fundBalance(t), "1200", "缓冲扫进基金")
}

// 真实的并发流程：两个仓位被同时强平(各自异步平仓、无对手盘走超时兜底)。一次成交的结算不是原子的，
// 先平完的仓位做强平后结算时，另一个仓位可能正好处于结算的中间状态，所以基金流水可能出现"先垫付、
// 后回收"的多余记录，见docs/known-limitations.md；但每一笔垫付/回收都恰好等于当时被清零的金额，
// 总额始终守恒：不管谁先谁后，最终基金只应该净垫付整个账户真正的缺口，账户清零
// a充值1000：BTC多头0.1@65000(保证金650)、ETH多头1@3000(保证金300)，开仓手续费3.25+1.5，
// available=45.25。BTC标记价跌到50000亏1500，平仓手续费2.5，ETH标记价不动平仓手续费1.5：
// 总缺口=-(45.25+950-1500-2.5-1.5)=508.75
func TestLiquidation_TwoPositionsConcurrentlyEndInConsistentState(t *testing.T) {
	e := newEngineEnv(t)
	a := e.newAccount(t, 1, "1000")
	b := e.newAccount(t, 2, "50000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")
	e.openLongAgainst(t, a, b, "ETHUSDT", "3000", "1", "300")
	e.setFundBalance(t, "100000") // 基金够用，避免ADL让数字变复杂
	e.setMark(t, testSymbol, "50000")

	e.liq.RiskScanOnce(context.Background())
	waitFor(t, "两个仓位都被强平清零", func() bool {
		return e.position(t, a, model.SideLong).Volume.IsZero() && e.positionOf(t, a, "ETHUSDT", model.SideLong).Volume.IsZero()
	})
	waitFor(t, "基金记账", func() bool { return e.fundLedgerCount(t) >= 1 })
	time.Sleep(500 * time.Millisecond) // 等两个仓位各自的强平后结算都走完

	mustDec(t, e.fundBalance(t), "99491.25", "基金净垫付=100000-508.75，不管中间怎么进出")
	acc := e.account(t, a)
	mustDec(t, acc.Available, "0", "账户清零")
	mustDec(t, acc.Credit, "0", "信用额度")
	mustDec(t, acc.FrozenMargin, "0", "冻结保证金")
}

// 强平后结算在同一个uid上串行：同一个缺口被多个调用同时读到时只能垫付一次。不串行的话每个调用
// 各自垫付一遍、各自把余额加回0，账户最后反而多出钱来，基金白亏
func TestLiquidation_ConcurrentAftermathSettlesShortfallOnce(t *testing.T) {
	e := newEngineEnv(t)
	a := e.newAccount(t, 1, "0")
	e.setFundBalance(t, "1000")
	if _, err := e.db.Exec(`UPDATE accounts SET available = -100 WHERE uid = ?`, a); err != nil {
		t.Fatal(err)
	}

	const workers = 8
	errs := make(chan error, workers)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		go func() {
			<-start
			errs <- e.engine.HandleLiquidationSettleAftermath(context.Background(), testSymbol, a, model.SideLong)
		}()
	}
	close(start)
	for i := 0; i < workers; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("强平后结算: %v", err)
		}
	}

	mustDec(t, e.account(t, a).Available, "0", "缺口结清后账户是0，不是被多加了钱")
	mustDec(t, e.fundBalance(t), "900", "基金只垫付一次")
	if n := e.fundLedgerCount(t); n != 1 {
		t.Fatalf("基金流水应该只有1行, got %d", n)
	}
}
