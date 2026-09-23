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

// 权益=balance+credit+浮动盈亏：balance是不随冻结变化的总额，已经包含了仓位占用/挂单冻结的
// 保证金，开仓/挂单只是让frozen_margin变多，价格不动权益只被手续费拉低
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
	mustDec(t, v.Balance.Sub(v.FrozenMargin), "0", "开仓后可用余额")
	mustDec(t, v.PositionMargin, "650", "仓位保证金")
	mustDec(t, v.Equity, "650", "权益=balance(650，653.25充值只被3.25手续费拉低)+credit(0)+浮动盈亏(0)")

	// 挂一笔开仓单再锁定600：balance不变，frozen_margin变多，权益不变
	vbBefore, _ := e.accounts.View(ctx, b)
	e.restBid(t, b, "60000", "0.1", "600")
	vb, _ := e.accounts.View(ctx, b)
	mustDec(t, vb.FrozenMargin.Sub(vbBefore.FrozenMargin), "600", "挂单新增锁定")
	// b: 10000 - 1.3手续费(maker，开仓那笔650名义值的成交) = 9998.7，balance不受挂单/仓位锁定影响
	mustDec(t, vb.Equity, "9998.7", "权益=balance(9998.7)+credit(0)+浮动盈亏(0)")
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

// ---- 触发边界：维持保证金要求现在是"账户当前balance+credit亏损达到固定比例"——投保保的是
// 整个账户，不是某一笔仓位的保证金。未投保100%(权益<=0才触发)、已投保80%(权益<=20%*(balance+
// credit)才触发)，不再用risk_limit_tiers的分档mmr公式，见
// PositionService.LossRatioUninsured/LossRatioInsured、LiquidationService.checkAndLiquidate ----

// a充值1000，开仓后balance=996.75(扣taker手续费3.25)，权益=996.75+0.1*(标记价-65000)。
// 未投保：维持保证金要求=996.75*(1-100%)=0，标记价55040：权益0.75 > 0，不触发；
// 标记价55030：权益-0.25 <= 0，触发
func TestLiquidation_TriggerBoundary_Uninsured(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	a := e.newAccount(t, 1, "1000")
	b := e.newAccount(t, 2, "10000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")

	e.setMark(t, testSymbol, "55040")
	e.liq.RiskScanOnce(ctx)
	if p := e.position(t, a, model.SideLong); p.Status != model.PositionStatusNormal {
		t.Fatalf("权益还没亏光初始保证金，不应该触发, status=%s", p.Status)
	}
	if n := len(e.liquidationOrders(t, a)); n != 0 {
		t.Fatalf("不应该有强平委托, got %d", n)
	}

	e.setMark(t, testSymbol, "55030")
	e.liq.RiskScanOnce(ctx)
	// 强平单是异步goroutine里落库的，RiskScanOnce返回时可能还没写入
	waitFor(t, "挂出强平委托", func() bool { return len(e.liquidationOrders(t, a)) == 1 })
	e.waitLiquidationDone(t, a, 1) // 等异步的强平走完，别让它在测试库被删掉之后还在跑
}

// 同样的账户投保之后(不发信用额度，只改投保状态；用setCredit(amount="0")是为了避免顺带
// 触发下面的投保赔付把balance/credit算复杂)：维持保证金要求=996.75*(1-80%)=199.35，
// 标记价57030：权益199.75 > 199.35，不触发；标记价57020：权益198.75 <= 199.35，触发——
// 阈值比未投保时宽松得多(198.75远大于0)
func TestLiquidation_TriggerBoundary_Insured(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	a := e.newAccount(t, 1, "1000")
	b := e.newAccount(t, 2, "10000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")
	e.setCredit(t, a, "0", true)

	e.setMark(t, testSymbol, "57030")
	e.liq.RiskScanOnce(ctx)
	if p := e.position(t, a, model.SideLong); p.Status != model.PositionStatusNormal {
		t.Fatalf("权益还没跌破投保阈值，不应该触发, status=%s", p.Status)
	}
	if n := len(e.liquidationOrders(t, a)); n != 0 {
		t.Fatalf("不应该有强平委托, got %d", n)
	}

	e.setMark(t, testSymbol, "57020")
	e.liq.RiskScanOnce(ctx)
	waitFor(t, "挂出强平委托", func() bool { return len(e.liquidationOrders(t, a)) == 1 })
	e.waitLiquidationDone(t, a, 0) // 权益是正的(198.75)，没有穿仓，不涉及保险基金
}

// 用户随时可以投保，投保后强平线立刻变严格：同一个标记价，未投保时权益还没跌破0(不触发)，
// 投保之后跌破199.35(触发)——不需要等下一轮价格变化，下一次风控扫描直接用新的投保状态判断
func TestLiquidation_BuyingInsuranceTightensThresholdImmediately(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	a := e.newAccount(t, 1, "1000")
	b := e.newAccount(t, 2, "10000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")
	// 权益=996.75+0.1*(56032-65000)=99.95：未投保(阈值0)不触发，已投保(阈值199.35)会触发
	e.setMark(t, testSymbol, "56032")

	e.liq.RiskScanOnce(ctx)
	if p := e.position(t, a, model.SideLong); p.Status != model.PositionStatusNormal {
		t.Fatalf("未投保，权益还没亏光全部余额，不应该触发, status=%s", p.Status)
	}

	e.setCredit(t, a, "0", true)
	e.liq.RiskScanOnce(ctx)
	waitFor(t, "投保后阈值收紧，同样的标记价立刻触发强平", func() bool { return len(e.liquidationOrders(t, a)) == 1 })
	e.waitLiquidationDone(t, a, 0) // 权益是正的(99.95)，没有穿仓，不涉及保险基金
}

// ---- 强平之后的资金结局 ----

// 强平单在订单簿里被吃掉，成交价是对手挂单的价格；平仓后balance/credit都还剩正数，两者都
// 不算穿仓、都不扫进保险基金，留给用户继续交易。账户投保(阈值199.35)才会在55250这个价位
// 触发——未投保阈值是0，55250时权益21.75还是正的，不会触发，见
// TestLiquidation_TriggerBoundary_Uninsured
func TestLiquidation_FilledAgainstBookSweepsBufferIntoFund(t *testing.T) {
	e := newEngineEnv(t)
	a := e.newAccount(t, 1, "1000")
	b := e.newAccount(t, 2, "10000")
	c := e.newAccount(t, 3, "10000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")
	e.setCredit(t, a, "0", true)
	e.restBid(t, c, "55300", "0.1", "553")
	e.setMark(t, testSymbol, "55250")

	e.liq.RiskScanOnce(context.Background())
	e.waitLiquidationDone(t, a, 0) // 没有穿仓，不涉及保险基金

	orders := e.liquidationOrders(t, a)
	if len(orders) != 1 || orders[0].Status != model.OrderStatusFilled {
		t.Fatalf("强平委托应该在订单簿里成交, got %+v", orders)
	}
	// 保护价=55250-55250*0.004*2=54808，对手买单挂在55300(在保护价之上)，按对手价55300成交
	mustDec(t, orders[0].AvgDealPrice, "55300", "强平成交价")
	// 平仓盈亏(55300-65000)*0.1=-970，taker手续费5530*0.0005=2.765；balance=996.75-970-2.765=
	// 23.985(650保证金本来就没从balance里扣过，解锁不改变balance)，不算穿仓，留给用户
	mustDec(t, e.ledgerSum(t, a, model.TxRealizedPnl), "-970", "已实现盈亏")
	acc := e.account(t, a)
	mustDec(t, acc.Balance, "23.985", "没有穿仓，balance留给用户，不扫进基金")
	// 触发那一刻(mark=55250前)投保赔付=(balance996.75+credit0)*50%=498.375
	mustDec(t, acc.Credit, "498.375", "投保赔付留在信用额度里")
	mustDec(t, e.fundBalance(t), "0", "没有穿仓，基金不受影响")
}

// 没有对手盘时靠超时兜底：撤掉强平单，按标记价直接结算，结果跟在订单簿成交是同一套账。
// 账户投保(阈值199.35)才会在55250这个价位触发，理由同上一个测试
func TestLiquidation_TimeoutFallbackSettlesAtMarkPrice(t *testing.T) {
	e := newEngineEnv(t)
	a := e.newAccount(t, 1, "1000")
	b := e.newAccount(t, 2, "10000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")
	e.setCredit(t, a, "0", true)
	e.setMark(t, testSymbol, "55250")

	e.liq.RiskScanOnce(context.Background())
	e.waitLiquidationDone(t, a, 0) // 没有穿仓，不涉及保险基金

	orders := e.liquidationOrders(t, a)
	if len(orders) != 1 || orders[0].Status != model.OrderStatusFilled {
		t.Fatalf("兜底后强平委托应该是filled, got %+v", orders)
	}
	mustDec(t, orders[0].AvgDealPrice, "55250", "兜底按标记价结算")
	// (55250-65000)*0.1=-975，手续费5525*0.0005=2.7625；balance=996.75-975-2.7625=18.9875
	// (650保证金本来就没从balance里扣过，解锁不改变balance，见account-and-margin.md)
	mustDec(t, e.ledgerSum(t, a, model.TxRealizedPnl), "-975", "已实现盈亏")
	mustDec(t, e.fundBalance(t), "0", "没有穿仓，基金不受影响")
	mustDec(t, e.account(t, a).Balance, "18.9875", "没有穿仓，balance留给用户，不扫进基金")
	mustDec(t, e.account(t, a).Credit, "498.375", "投保赔付=(996.75+0)*50%，留在信用额度里")
	if e.book.BookFor(testSymbol).Contains(orders[0].OrderID) {
		t.Fatal("兜底之后强平单不应该还留在订单簿里")
	}
}

// ---- 投保赔付：触发强平线那一刻，按(balance+credit)*50%发放信用额度，不用等平仓结算完 ----

// a充值1000开仓500(50000@0.1，杠杆10)，投保。balance=1000-2.5(手续费)=997.5，投保阈值=
// 997.5*20%=199.5，权益=997.5+0.1*(标记价-50000)<=199.5即触发(标记价<=42020)。一旦触发就
// 应该立刻拿到(997.5+0)*50%=498.75的信用额度，不需要等强平单真正成交
func TestLiquidation_InsuredAccountGetsCreditPayoutOnTrigger(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	a := e.newAccount(t, 1, "1000")
	b := e.newAccount(t, 2, "10000")
	e.openLongAgainst(t, a, b, testSymbol, "50000", "0.1", "500")
	e.setCredit(t, a, "0", true)
	e.setMark(t, testSymbol, "42000")

	e.liq.RiskScanOnce(ctx)
	waitFor(t, "投保赔付到账", func() bool {
		return e.account(t, a).Credit.Equal(decimalOf(t, "498.75"))
	})

	// 再扫几轮：GrantCredit按uid+round幂等，重复触发不能重复发放
	for i := 0; i < 3; i++ {
		e.liq.RiskScanOnce(ctx)
	}
	time.Sleep(100 * time.Millisecond)
	mustDec(t, e.account(t, a).Credit, "498.75", "多轮风控扫描不能重复发放赔付")
	e.waitLiquidationDone(t, a, 0) // 触发时权益是正的(197.5)，没有穿仓，不涉及保险基金
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
	mustDec(t, acc.Balance, "0", "穿仓由基金垫付后账户清零")
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
	mustDec(t, e.account(t, a).Balance, "0", "a账户清零")
}

// 多仓位账户：先平完的仓位结算后free balance(balance-frozen_margin)可能暂时为负，但别的
// 仓位的保证金还锁在frozen_margin里，这时候不能让基金垫付(也不能触发ADL)——等保证金解锁
// 才知道有没有真的穿仓。确定性场景：直接构造"BTC仓位刚平完释放了它的650锁定、free balance=-100，
// ETH仓位(300锁定)还开着"的状态。全部平完后如果是正数结余，不算穿仓，留给用户，不扫进保险基金
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
	// BTC平仓释放了它的650锁定，frozen_margin只剩ETH的300；balance摆成让
	// free balance(balance-frozen_margin=200-300)等于-100
	if _, err := e.db.Exec(`UPDATE accounts SET balance = 200, frozen_margin = 300 WHERE uid = ?`, a); err != nil {
		t.Fatal(err)
	}

	if err := e.engine.HandleLiquidationSettleAftermath(ctx, testSymbol, a, model.SideLong); err != nil {
		t.Fatal(err)
	}

	mustDec(t, e.freeBalance(t, a), "-100", "还有仓位没平完时不能结清账户")
	mustDec(t, e.fundBalance(t), "1000", "基金不能提前垫付")
	if n := e.fundLedgerCount(t); n != 0 {
		t.Fatalf("不应该有基金流水, got %d", n)
	}

	// 最后一个仓位也平完(模拟：ETH仓位归零，它的300锁定也释放)，这时才结算：合计为正200，
	// 不是穿仓，留给用户，不动保险基金
	if _, err := e.db.Exec(`UPDATE positions SET volume = 0, position_margin = 0, status = 'closed' WHERE uid = ? AND symbol = 'ETHUSDT'`, a); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.Exec(`UPDATE accounts SET balance = 200, frozen_margin = 0 WHERE uid = ?`, a); err != nil {
		t.Fatal(err)
	}
	if err := e.engine.HandleLiquidationSettleAftermath(ctx, "ETHUSDT", a, model.SideLong); err != nil {
		t.Fatal(err)
	}
	mustDec(t, e.account(t, a).Balance, "200", "全部仓位平完之后，正数结余留给用户")
	mustDec(t, e.fundBalance(t), "1000", "没有穿仓，基金不受影响")
}

// 真实的并发流程：两个仓位被同时强平(各自异步平仓、无对手盘走超时兜底)。一次成交的结算不是原子的，
// 先平完的仓位做强平后结算时，另一个仓位可能正好处于结算的中间状态，所以基金流水可能出现"先垫付、
// 后回收"的多余记录，见docs/known-limitations.md；但每一笔垫付/回收都恰好等于当时被清零的金额，
// 总额始终守恒：不管谁先谁后，最终基金只应该净垫付整个账户真正的缺口，账户清零
// a充值1000：BTC多头0.1@65000(保证金650)、ETH多头1@3000(保证金300)，开仓手续费3.25+1.5，
// free balance=45.25。BTC标记价跌到50000亏1500，平仓手续费2.5，ETH标记价不动平仓手续费1.5：
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
	mustDec(t, acc.Balance, "0", "账户清零")
	mustDec(t, acc.Credit, "0", "信用额度")
	mustDec(t, acc.FrozenMargin, "0", "冻结保证金")
}

// 强平后结算在同一个uid上串行：同一个缺口被多个调用同时读到时只能垫付一次。不串行的话每个调用
// 各自垫付一遍、各自把余额加回0，账户最后反而多出钱来，基金白亏
func TestLiquidation_ConcurrentAftermathSettlesShortfallOnce(t *testing.T) {
	e := newEngineEnv(t)
	a := e.newAccount(t, 1, "0")
	e.setFundBalance(t, "1000")
	if _, err := e.db.Exec(`UPDATE accounts SET balance = -100 WHERE uid = ?`, a); err != nil {
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

	mustDec(t, e.account(t, a).Balance, "0", "缺口结清后账户是0，不是被多加了钱")
	mustDec(t, e.fundBalance(t), "900", "基金只垫付一次")
	if n := e.fundLedgerCount(t); n != 1 {
		t.Fatalf("基金流水应该只有1行, got %d", n)
	}
}

// ---- 强平时撤挂单(币安/OKX全仓强平的做法) ----

// 强平触发时先撤掉这个uid的全部挂单和条件单(含条件平仓单)，锁定的保证金解锁，再处理仓位；
// 别的账户的挂单不受影响。之前不撤的话，结算只看free balance+free credit，挂单锁定的保证金
// 被漏算：这里free balance会被亏到-705.75、基金垫付705.75，之后撤单解锁700，账户白拿700
// a充值1500：多头0.1@65000(保证金650、开仓手续费3.25)，挂开仓限价单再锁定600，条件开仓单再锁定100，
// 另有一笔条件平仓单。标记价跌到50000：权益=balance(1496.75)+credit(0)+持仓浮亏(-1500)=-3.25，
// 触发强平——balance是不随锁定变化的总额，不需要再单独加挂单/仓位占用的部分
// 撤单后free=846.75，平仓亏1500、手续费2.5、650锁定解锁，free=-5.75，这就是真实缺口
func TestLiquidation_TriggerCancelsAllPendingOrdersAndSettlesRealShortfall(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	a := e.newAccount(t, 1, "1500")
	b := e.newAccount(t, 2, "10000")
	c := e.newAccount(t, 3, "10000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")

	rest := e.insertOrder(t, a, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "60000", amount: "0.1", margin: "600"})
	if err := e.engine.SubmitOrder(ctx, rest, 3); err != nil {
		t.Fatal(err)
	}
	if ok, err := e.accountRepo.FreezeFromBalance(ctx, e.account(t, a).ID, decimalOf(t, "100")); err != nil || !ok {
		t.Fatalf("冻结条件单保证金: ok=%v err=%v", ok, err)
	}
	condOpen := newConditionalOpen(e, a, "90000", "0.05", "100")
	condClose := newConditionalOpen(e, a, "90000", "0.05", "0")
	condClose.Action = model.ActionClose
	for _, co := range []*model.ConditionalOrder{condOpen, condClose} {
		if err := e.conditional.Insert(ctx, co); err != nil {
			t.Fatal(err)
		}
	}
	// c的挂单价格要低于a的强平保护价(50000-50000*0.4%*2=49600)，否则会被强平单当对手盘吃掉
	otherOrder := e.insertOrder(t, c, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "40000", amount: "0.1", margin: "400"})
	if err := e.engine.SubmitOrder(ctx, otherOrder, 4); err != nil {
		t.Fatal(err)
	}
	// frozen_margin现在是挂单+仓位占用的合计：650(仓位)+600(挂单)+100(条件单)=1350
	mustDec(t, e.account(t, a).FrozenMargin, "1350", "强平前挂单+条件单+仓位的锁定合计")
	e.setFundBalance(t, "100000")
	e.setMark(t, testSymbol, "50000")

	e.liq.RiskScanOnce(ctx)
	// 触发时的撤单是同步做的，RiskScanOnce一返回就已经生效，不用等异步的强平走完。这一条能区分
	// "触发时先撤"和"只靠结算前的兜底撤"：后者要等强平走完才撤，两者最终账目一样
	if got := e.order(t, rest.OrderID).Status; got != model.OrderStatusCanceled {
		t.Fatalf("触发强平时挂单应该立刻被撤销, status=%s", got)
	}
	if e.book.BookFor(testSymbol).Contains(rest.OrderID) {
		t.Fatal("被撤销的挂单应该立刻摘出订单簿")
	}
	e.waitLiquidationDone(t, a, 1)

	for _, id := range []uint64{condOpen.OrderID, condClose.OrderID} {
		var st model.ConditionalOrderStatus
		if err := e.db.Get(&st, `SELECT status FROM conditional_orders WHERE order_id = ?`, id); err != nil || st != model.ConditionalStatusCanceled {
			t.Fatalf("条件单%d应该被撤销(含条件平仓单), status=%s err=%v", id, st, err)
		}
	}
	if got := e.order(t, otherOrder.OrderID).Status; got != model.OrderStatusOpen {
		t.Fatalf("别的账户的挂单不能被撤, status=%s", got)
	}

	acc := e.account(t, a)
	mustDec(t, acc.FrozenMargin, "0", "冻结保证金全部退回")
	mustDec(t, acc.Balance, "0", "缺口由基金垫付后账户清零")
	mustDec(t, e.fundLedgerAmount(t, "强平穿仓垫付"), "-5.75", "垫付的是真实缺口，不是被漏算了挂单保证金的705.75")
	mustDec(t, e.fundBalance(t), "99994.25", "基金余额")
}

// 兜底：强平窗口期里用户新挂的单，在结算前也要撤掉。这里直接构造"没有仓位、free balance=-100、
// 还有一笔挂单锁定600(真实的，来自实际下单)"的状态：真实权益是500，不撤单的话free balance
// 会算成-100(漏算了这笔锁定)，误判成穿仓让基金多垫付100；撤单之后正确解出free balance=500，
// 没有穿仓，也不动保险基金
func TestLiquidation_AftermathCancelsOrdersPlacedDuringLiquidation(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	a := e.newAccount(t, 1, "5000")
	o := e.insertOrder(t, a, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "60000", amount: "0.1", margin: "600"})
	if err := e.engine.SubmitOrder(ctx, o, 1); err != nil {
		t.Fatal(err)
	}
	e.setFundBalance(t, "1000")
	// 这笔600是insertOrder真实锁进frozen_margin的，balance摆成让free balance(balance-600)等于-100
	if _, err := e.db.Exec(`UPDATE accounts SET balance = 500 WHERE uid = ?`, a); err != nil {
		t.Fatal(err)
	}

	if err := e.engine.HandleLiquidationSettleAftermath(ctx, testSymbol, a, model.SideLong); err != nil {
		t.Fatal(err)
	}

	if got := e.order(t, o.OrderID).Status; got != model.OrderStatusCanceled {
		t.Fatalf("结算前应该把窗口期里的挂单撤掉, status=%s", got)
	}
	acc := e.account(t, a)
	mustDec(t, acc.FrozenMargin, "0", "冻结保证金退回")
	mustDec(t, acc.Balance, "500", "没有穿仓，正数结余留给用户")
	mustDec(t, e.fundBalance(t), "1000", "没有穿仓，基金不受影响")
	mustDec(t, e.fundLedgerAmount(t, "强平穿仓垫付"), "0", "没有穿仓，不应该垫付")
}

// CancelAllPendingOrders本身：撤用户委托和条件单，强平单不撤(避免SubmitOrder里先结算后挂剩余量
// 时留下数据库已撤销、订单簿还挂着的幽灵单)，别的账户不动
func TestCancelAllPendingOrders_SkipsLiquidationOrdersAndOtherAccounts(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	a := e.newAccount(t, 1, "10000")
	other := e.newAccount(t, 2, "10000")
	normal := e.insertOrder(t, a, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "60000", amount: "0.1", margin: "600"})
	liq := e.insertOrder(t, a, orderOpts{side: model.SideLong, action: model.ActionClose, price: "70000", amount: "0.1", liquidation: true})
	theirs := e.insertOrder(t, other, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "60000", amount: "0.1", margin: "600"})
	for _, o := range []*model.Order{normal, liq, theirs} {
		if err := e.engine.SubmitOrder(ctx, o, 1); err != nil {
			t.Fatal(err)
		}
	}
	cond := newConditionalOpen(e, a, "90000", "0.05", "0")
	if err := e.conditional.Insert(ctx, cond); err != nil {
		t.Fatal(err)
	}

	if failed := e.engine.CancelAllPendingOrders(ctx, a); failed != 0 {
		t.Fatalf("不应该有撤单失败, got %d", failed)
	}

	if got := e.order(t, normal.OrderID).Status; got != model.OrderStatusCanceled {
		t.Fatalf("用户委托应该被撤, status=%s", got)
	}
	if got := e.order(t, liq.OrderID).Status; got != model.OrderStatusOpen {
		t.Fatalf("强平单不能被撤, status=%s", got)
	}
	if !e.book.BookFor(testSymbol).Contains(liq.OrderID) {
		t.Fatal("强平单应该还在订单簿里")
	}
	var st model.ConditionalOrderStatus
	if err := e.db.Get(&st, `SELECT status FROM conditional_orders WHERE order_id = ?`, cond.OrderID); err != nil || st != model.ConditionalStatusCanceled {
		t.Fatalf("条件单应该被撤, status=%s err=%v", st, err)
	}
	if got := e.order(t, theirs.OrderID).Status; got != model.OrderStatusOpen {
		t.Fatalf("别的账户的委托不能被撤, status=%s", got)
	}
	mustDec(t, e.account(t, a).FrozenMargin, "0", "撤单退回冻结保证金")
	mustDec(t, e.account(t, other).FrozenMargin, "600", "别的账户的冻结不变")
}
