//go:build integration

package service_test

import (
	"context"
	"testing"

	"perp-go/internal/model"
)

var openLong = orderOpts{side: model.SideLong, action: model.ActionOpen, price: "64000", amount: "0.1", margin: "640"}

// 冻结账户的开仓委托：不进订单簿，撤销并把冻结的保证金原样退回
func TestEngineFreeze_FrozenAccountOpenOrderIsCanceledAndRefunded(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	uid := e.newAccount(t, 1, "10000")
	o := e.insertOrder(t, uid, openLong)
	mustDec(t, e.account(t, uid).FrozenMargin, "640", "下单后冻结保证金")
	e.setStatus(t, uid, model.AccountStatusFrozen)

	if err := e.engine.SubmitOrder(ctx, o, 1); err != nil {
		t.Fatalf("SubmitOrder: %v", err)
	}

	if got := e.order(t, o.OrderID).Status; got != model.OrderStatusCanceled {
		t.Fatalf("冻结账户的开仓委托应该被撤销, status=%s", got)
	}
	acc := e.account(t, uid)
	mustDec(t, acc.FrozenMargin, "0", "冻结保证金应该退回")
	mustDec(t, acc.Balance, "10000", "可用余额应该恢复")
	if e.book.BookFor(testSymbol).Contains(o.OrderID) {
		t.Fatal("被撤销的委托不应该在订单簿里")
	}

	// 同一条下单事件重复投递：状态已经不是活跃，直接跳过，不能再退一遍保证金
	if err := e.engine.SubmitOrder(ctx, o, 1); err != nil {
		t.Fatalf("重复投递: %v", err)
	}
	mustDec(t, e.account(t, uid).Balance, "10000", "重复投递后可用余额不能变多")
}

// 对照组：没冻结的账户同样的开仓委托正常挂进订单簿，保证金继续冻结着
func TestEngineFreeze_ActiveAccountOpenOrderRests(t *testing.T) {
	e := newEngineEnv(t)
	uid := e.newAccount(t, 1, "10000")
	o := e.insertOrder(t, uid, openLong)

	if err := e.engine.SubmitOrder(context.Background(), o, 1); err != nil {
		t.Fatal(err)
	}

	if got := e.order(t, o.OrderID).Status; got != model.OrderStatusOpen {
		t.Fatalf("正常账户的委托应该保持open, status=%s", got)
	}
	if !e.book.BookFor(testSymbol).Contains(o.OrderID) {
		t.Fatal("正常账户的委托应该在订单簿里")
	}
	mustDec(t, e.account(t, uid).FrozenMargin, "640", "挂单期间保证金保持冻结")
}

// 冻结只拦新增风险：平仓委托照常挂单
func TestEngineFreeze_FrozenAccountCloseOrderStillRests(t *testing.T) {
	e := newEngineEnv(t)
	uid := e.newAccount(t, 1, "10000")
	e.setStatus(t, uid, model.AccountStatusFrozen)
	o := e.insertOrder(t, uid, orderOpts{side: model.SideLong, action: model.ActionClose, price: "66000", amount: "0.05"})

	if err := e.engine.SubmitOrder(context.Background(), o, 1); err != nil {
		t.Fatal(err)
	}

	if got := e.order(t, o.OrderID).Status; got != model.OrderStatusOpen {
		t.Fatalf("冻结账户的平仓委托应该保持open, status=%s", got)
	}
	if !e.book.BookFor(testSymbol).Contains(o.OrderID) {
		t.Fatal("冻结账户的平仓委托应该在订单簿里")
	}
}

// 强平单是系统降风险的动作，不受冻结影响。这里用action=open+liquidation=true专门钉住
// "!order.Liquidation"这个条件(真实的强平单是平仓方向，会被action判断先放行)
func TestEngineFreeze_LiquidationOrderBypassesGate(t *testing.T) {
	e := newEngineEnv(t)
	uid := e.newAccount(t, 1, "10000")
	e.setStatus(t, uid, model.AccountStatusFrozen)
	opts := openLong
	opts.liquidation = true
	o := e.insertOrder(t, uid, opts)

	if err := e.engine.SubmitOrder(context.Background(), o, 1); err != nil {
		t.Fatal(err)
	}

	if got := e.order(t, o.OrderID).Status; got != model.OrderStatusOpen {
		t.Fatalf("强平委托不应该被冻结撤销, status=%s", got)
	}
	if !e.book.BookFor(testSymbol).Contains(o.OrderID) {
		t.Fatal("强平委托应该在订单簿里")
	}
}

// 冻结账户的开仓单就算价格能吃到对手盘，也不能成交：对手挂单原样留在簿子上，没有产生成交记录
func TestEngineFreeze_FrozenAccountOrderDoesNotTrade(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	maker := e.newAccount(t, 1, "10000")
	taker := e.newAccount(t, 2, "10000")
	sell := e.insertOrder(t, maker, orderOpts{side: model.SideShort, action: model.ActionOpen, price: "64000", amount: "0.1", margin: "640"})
	if err := e.engine.SubmitOrder(ctx, sell, 1); err != nil {
		t.Fatal(err)
	}
	e.setStatus(t, taker, model.AccountStatusFrozen)
	buy := e.insertOrder(t, taker, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "64000", amount: "0.1", margin: "640"})

	if err := e.engine.SubmitOrder(ctx, buy, 2); err != nil {
		t.Fatal(err)
	}

	if got := e.order(t, buy.OrderID).Status; got != model.OrderStatusCanceled {
		t.Fatalf("冻结账户的吃单委托应该被撤销, status=%s", got)
	}
	if got := e.order(t, sell.OrderID); got.Status != model.OrderStatusOpen || !got.TradedAmount.IsZero() {
		t.Fatalf("对手挂单不应该被动成交: %+v", got)
	}
	if !e.book.BookFor(testSymbol).Contains(sell.OrderID) {
		t.Fatal("对手挂单应该还在订单簿里")
	}
	var n int
	if err := e.db.Get(&n, `SELECT COUNT(*) FROM trades`); err != nil || n != 0 {
		t.Fatalf("不应该有成交记录, got %d err=%v", n, err)
	}
	mustDec(t, e.account(t, taker).FrozenMargin, "0", "冻结账户的保证金应该退回")
}

// 解冻之后恢复正常
func TestEngineFreeze_UnfrozenAccountTradesAgain(t *testing.T) {
	e := newEngineEnv(t)
	uid := e.newAccount(t, 1, "10000")
	e.setStatus(t, uid, model.AccountStatusFrozen)
	e.setStatus(t, uid, model.AccountStatusActive)
	o := e.insertOrder(t, uid, openLong)

	if err := e.engine.SubmitOrder(context.Background(), o, 1); err != nil {
		t.Fatal(err)
	}

	if !e.book.BookFor(testSymbol).Contains(o.OrderID) {
		t.Fatal("解冻后的开仓委托应该正常挂单")
	}
}

// 查不出账户状态时失败关闭：返回错误、不撮合、不挂单，委托和保证金保持原样等重启恢复
func TestEngineFreeze_StatusLookupFailureFailsClosed(t *testing.T) {
	e := newEngineEnv(t)
	uid := e.newAccount(t, 1, "10000")
	o := e.insertOrder(t, uid, openLong)
	canceled, cancel := context.WithCancel(context.Background())
	cancel() // 数据库查询会因为context已取消而失败

	if err := e.engine.SubmitOrder(canceled, o, 1); err == nil {
		t.Fatal("查不出账户状态时应该返回错误")
	}

	if e.book.BookFor(testSymbol).Contains(o.OrderID) {
		t.Fatal("查不出账户状态时不能让委托进订单簿")
	}
	if got := e.order(t, o.OrderID).Status; got != model.OrderStatusOpen {
		t.Fatalf("委托应该保持open等重启恢复, status=%s", got)
	}
	mustDec(t, e.account(t, uid).FrozenMargin, "640", "保证金保持冻结")
}

// 重启恢复：冻结账户残留的开仓委托在重放时被撤销退款，别的账户的委托正常恢复
func TestEngineFreeze_RecoverOrderBookCancelsFrozenAccountsOrders(t *testing.T) {
	e := newEngineEnv(t)
	frozenUID := e.newAccount(t, 1, "10000")
	normalUID := e.newAccount(t, 2, "10000")
	frozenOrder := e.insertOrder(t, frozenUID, openLong)
	normalOrder := e.insertOrder(t, normalUID, openLong)
	closeOrder := e.insertOrder(t, frozenUID, orderOpts{side: model.SideLong, action: model.ActionClose, price: "66000", amount: "0.05"})
	e.setStatus(t, frozenUID, model.AccountStatusFrozen)

	e.restartEngine(t)
	if err := e.engine.RecoverOrderBook(context.Background()); err != nil {
		t.Fatalf("RecoverOrderBook: %v", err)
	}

	if got := e.order(t, frozenOrder.OrderID).Status; got != model.OrderStatusCanceled {
		t.Fatalf("冻结账户的开仓委托恢复时应该被撤销, status=%s", got)
	}
	mustDec(t, e.account(t, frozenUID).FrozenMargin, "0", "恢复时撤单应该退回保证金")
	b := e.book.BookFor(testSymbol)
	if b.Contains(frozenOrder.OrderID) {
		t.Fatal("被撤销的委托不应该回到订单簿")
	}
	if !b.Contains(normalOrder.OrderID) {
		t.Fatal("正常账户的委托应该恢复到订单簿")
	}
	if !b.Contains(closeOrder.OrderID) {
		t.Fatal("冻结账户的平仓委托应该恢复到订单簿")
	}
}

// 条件开仓单在账户冻结后被触发：落地成委托后被引擎兜底撤销，保证金退回，不会成交
func TestEngineFreeze_ConditionalOpenTriggeredOnFrozenAccount(t *testing.T) {
	e := newEngineEnv(t)
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
	e.setStatus(t, uid, model.AccountStatusFrozen)
	if err := e.markPrice.UpdateFromTrade(ctx, testSymbol, decimalOf(t, "66500")); err != nil {
		t.Fatal(err)
	}

	e.condSvc.ScanOnce(ctx)

	if got := e.order(t, co.OrderID).Status; got != model.OrderStatusCanceled {
		t.Fatalf("触发落地的开仓委托应该被撤销, status=%s", got)
	}
	a := e.account(t, uid)
	mustDec(t, a.FrozenMargin, "0", "保证金应该退回")
	mustDec(t, a.Balance, "10000", "可用余额应该恢复")
	if e.book.BookFor(testSymbol).Contains(co.OrderID) {
		t.Fatal("不应该进订单簿")
	}
}
