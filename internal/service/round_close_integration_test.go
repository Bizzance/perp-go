//go:build integration

package service_test

import (
	"context"
	"testing"

	"perp-go/internal/model"
)

// 让longUID在symbol上持有amount数量的多头(shortUID是对手方空头，先挂单)，开仓价price，杠杆10倍
func (e *engineEnv) openLongAgainst(t *testing.T, longUID, shortUID uint64, symbol, price, amount, margin string) {
	t.Helper()
	ctx := context.Background()
	sell := e.insertOrder(t, shortUID, orderOpts{symbol: symbol, side: model.SideShort, action: model.ActionOpen, price: price, amount: amount, margin: margin})
	if err := e.engine.SubmitOrder(ctx, sell, 1); err != nil {
		t.Fatal(err)
	}
	buy := e.insertOrder(t, longUID, orderOpts{symbol: symbol, side: model.SideLong, action: model.ActionOpen, price: price, amount: amount, margin: margin})
	if err := e.engine.SubmitOrder(ctx, buy, 2); err != nil {
		t.Fatal(err)
	}
	if got := e.order(t, buy.OrderID).Status; got != model.OrderStatusFilled {
		t.Fatalf("建仓的吃单应该全部成交, status=%s", got)
	}
}

func (e *engineEnv) roundState(t *testing.T, uid uint64) (round uint64, insured bool) {
	t.Helper()
	a := e.account(t, uid)
	return a.Round, a.IsInsured
}

func (e *engineEnv) progressRows(t *testing.T, uid uint64) int {
	t.Helper()
	var n int
	if err := e.db.Get(&n, `SELECT COUNT(*) FROM round_close_progress WHERE uid = ?`, uid); err != nil {
		t.Fatal(err)
	}
	return n
}

// 结束本轮：撤掉全部挂单和条件单(含退保证金)、按标记价强平仓位、清零信用额度、复位投保、round+1
func TestCloseRound_SettlesEverything(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	a := e.newAccount(t, 1, "10000")
	b := e.newAccount(t, 2, "10000")
	e.setCredit(t, a, "500", true)

	// 建仓：a多头0.1@65000。a先付保证金650、taker手续费3.25 -> 可用余额9346.75
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")
	mustDec(t, e.freeBalance(t, a), "9346.75", "建仓后可用余额")

	// 再挂一笔开仓限价单(锁定600)、一笔条件开仓单(锁定300)、一笔条件平仓单(不锁定)
	rest := e.insertOrder(t, a, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "60000", amount: "0.1", margin: "600"})
	if err := e.engine.SubmitOrder(ctx, rest, 3); err != nil {
		t.Fatal(err)
	}
	acc := e.account(t, a)
	if ok, err := e.accountRepo.FreezeFromBalance(ctx, acc.ID, decimalOf(t, "300")); err != nil || !ok {
		t.Fatalf("冻结条件单保证金: ok=%v err=%v", ok, err)
	}
	condOpen := newConditionalOpen(e, a, "90000", "0.05", "300")
	if err := e.conditional.Insert(ctx, condOpen); err != nil {
		t.Fatal(err)
	}
	condClose := newConditionalOpen(e, a, "90000", "0.05", "0")
	condClose.Action = model.ActionClose
	if err := e.conditional.Insert(ctx, condClose); err != nil {
		t.Fatal(err)
	}
	// frozen_margin现在是仓位占用+挂单+条件单的合计：650(仓位)+600(挂单)+300(条件单)=1550
	mustDec(t, e.account(t, a).FrozenMargin, "1550", "仓位+挂单+条件单锁定的保证金合计")

	e.setMark(t, testSymbol, "66000")
	if err := e.engine.CloseRound(ctx, a, 1); err != nil {
		t.Fatalf("CloseRound: %v", err)
	}

	if got := e.order(t, rest.OrderID).Status; got != model.OrderStatusCanceled {
		t.Fatalf("挂单应该被撤销, status=%s", got)
	}
	if e.book.BookFor(testSymbol).Contains(rest.OrderID) {
		t.Fatal("挂单应该已经摘出订单簿")
	}
	for _, id := range []uint64{condOpen.OrderID, condClose.OrderID} {
		var st model.ConditionalOrderStatus
		if err := e.db.Get(&st, `SELECT status FROM conditional_orders WHERE order_id = ?`, id); err != nil || st != model.ConditionalStatusCanceled {
			t.Fatalf("条件单%d应该被撤销, status=%s err=%v", id, st, err)
		}
	}
	// 仓位按标记价66000强平：盈利(66000-65000)*0.1=100，taker手续费6600*0.0005=3.3，保证金650退回
	p := e.position(t, a, model.SideLong)
	mustDec(t, p.Volume, "0", "仓位数量")
	if p.Status != model.PositionStatusClosed {
		t.Fatalf("仓位应该是closed, got %s", p.Status)
	}
	mustDec(t, e.ledgerSum(t, a, model.TxRealizedPnl), "100", "已实现盈亏流水")
	mustDec(t, e.ledgerSum(t, a, model.TxFee), "-6.55", "手续费流水(建仓3.25+强平3.3)")

	final := e.account(t, a)
	// 结束本轮：balance/credit都清零，每一轮都是完全独立的资金周期。清零前balance=
	// 9996.75(建仓后) + 100盈利 - 3.3手续费 = 10093.45
	mustDec(t, final.Balance, "0", "结束本轮balance清零")
	mustDec(t, final.FrozenMargin, "0", "冻结保证金")
	mustDec(t, final.Credit, "0", "信用额度清零")
	mustDec(t, e.ledgerSum(t, a, model.TxRoundClose), "-10593.45", "回收信用额度500+清零balance10093.45的流水合计")
	if round, insured := e.roundState(t, a); round != 2 || insured {
		t.Fatalf("round应该推进到2且投保状态复位, got round=%d insured=%v", round, insured)
	}
	if n := e.progressRows(t, a); n != 0 {
		t.Fatalf("结束本轮完成后进度记录应该清理掉, got %d行", n)
	}
	// 对手方不受影响
	mustDec(t, e.position(t, b, model.SideShort).Volume, "0.1", "对手方仓位不受影响")
	if round, _ := e.roundState(t, b); round != 1 {
		t.Fatal("别的账户的round不能被推进")
	}
}

// round必须跟账户当前轮数一致才生效：过期的(已经结束过的)、超前的都忽略，
// 防止超时重试把下一轮刚挂的单撤掉、刚发的额度清零
func TestCloseRound_IgnoresStaleOrFutureRound(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	uid := e.newAccount(t, 1, "10000")
	if _, err := e.db.Exec(`UPDATE accounts SET round = 3 WHERE uid = ?`, uid); err != nil {
		t.Fatal(err)
	}
	e.setCredit(t, uid, "500", true)
	rest := e.insertOrder(t, uid, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "60000", amount: "0.1", margin: "600"})
	if err := e.engine.SubmitOrder(ctx, rest, 1); err != nil {
		t.Fatal(err)
	}

	for _, round := range []uint64{2, 4} {
		if err := e.engine.CloseRound(ctx, uid, round); err != nil {
			t.Fatalf("CloseRound(round=%d): %v", round, err)
		}
		if got := e.order(t, rest.OrderID).Status; got != model.OrderStatusOpen {
			t.Fatalf("round=%d不匹配，挂单不应该被撤, status=%s", round, got)
		}
		if a := e.account(t, uid); a.Round != 3 || !a.Credit.Equal(decimalOf(t, "500")) {
			t.Fatalf("round=%d不匹配，账户不应该变化: round=%d credit=%s", round, a.Round, a.Credit)
		}
	}

	if err := e.engine.CloseRound(ctx, uid, 3); err != nil {
		t.Fatal(err)
	}
	if got := e.order(t, rest.OrderID).Status; got != model.OrderStatusCanceled {
		t.Fatalf("round匹配时挂单应该被撤, status=%s", got)
	}
	if round, insured := e.roundState(t, uid); round != 4 || insured {
		t.Fatalf("应该推进到round=4, got round=%d insured=%v", round, insured)
	}

	// 已经进入第4轮：结束第3轮时balance跟credit一样清零了，第4轮是全新的资金周期，
	// 要先充值才能挂单。账户里新挂了单、新发了额度；重复结束第3轮(超时重试)一样都不能动
	e.newAccount(t, 1, "1000")
	e.setCredit(t, uid, "200", true)
	rest2 := e.insertOrder(t, uid, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "60000", amount: "0.1", margin: "600"})
	if err := e.engine.SubmitOrder(ctx, rest2, 2); err != nil {
		t.Fatal(err)
	}
	if err := e.engine.CloseRound(ctx, uid, 3); err != nil {
		t.Fatal(err)
	}
	if got := e.order(t, rest2.OrderID).Status; got != model.OrderStatusOpen {
		t.Fatalf("重复结束第3轮不应该撤第4轮的挂单, status=%s", got)
	}
	if a := e.account(t, uid); a.Round != 4 || !a.Credit.Equal(decimalOf(t, "200")) || !a.IsInsured {
		t.Fatalf("重复结束第3轮不应该动第4轮的账户: round=%d credit=%s insured=%v", a.Round, a.Credit, a.IsInsured)
	}
}

// 有仓位但缺标记价格时没法公允强平：这个仓位保留、round不推进、信用额度不清零；补上标记价格后
// 用同一个round重试就能完成
func TestCloseRound_MissingMarkPriceBlocksFinalizeUntilRetried(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	a := e.newAccount(t, 1, "10000")
	b := e.newAccount(t, 2, "10000")
	e.setCredit(t, a, "500", true)
	const eth = "ETHUSDT"
	e.openLongAgainst(t, a, b, eth, "3000", "1", "300")
	e.clearMark(t, eth)

	if err := e.engine.CloseRound(ctx, a, 1); err != nil {
		t.Fatalf("CloseRound: %v", err)
	}

	if p := e.positionOf(t, a, eth, model.SideLong); p.Volume.Sign() <= 0 {
		t.Fatal("缺标记价格时仓位不应该被强平")
	}
	if round, _ := e.roundState(t, a); round != 1 {
		t.Fatalf("有仓位没处理完，round不能推进, got %d", round)
	}
	mustDec(t, e.account(t, a).Credit, "500", "round没推进，信用额度不能清零")

	e.setMark(t, eth, "3100")
	if err := e.engine.CloseRound(ctx, a, 1); err != nil {
		t.Fatalf("补上标记价格后重试: %v", err)
	}
	if p := e.positionOf(t, a, eth, model.SideLong); p.Volume.Sign() != 0 {
		t.Fatalf("重试后仓位应该已经强平, volume=%s", p.Volume)
	}
	// 盈利(3100-3000)*1=100
	mustDec(t, e.ledgerSum(t, a, model.TxRealizedPnl), "100", "已实现盈亏")
	if round, insured := e.roundState(t, a); round != 2 || insured {
		t.Fatalf("重试后应该完成: round=%d insured=%v", round, insured)
	}
	mustDec(t, e.account(t, a).Credit, "0", "重试完成后信用额度清零")
}
