//go:build integration

package service_test

import (
	"context"
	"testing"

	"perp-go/internal/model"
)

// 期望值按经济公式手算，不是照代码输出抄：BTCUSDT种子数据里maker费率0.0002、taker费率0.0005，
// 杠杆10倍，开仓保证金=名义价值/10，成交价取先挂单(maker)的价格

// 完整走一遍：开仓成交 -> 平仓盈利成交。校验余额、冻结保证金、仓位、手续费流水、已实现盈亏流水、成交记录、标记价格
func TestSettlement_OpenThenCloseWithProfit(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	short := e.newAccount(t, 1, "10000") // 先挂单，maker
	long := e.newAccount(t, 2, "10000")  // 后吃单，taker
	buyer := e.newAccount(t, 3, "10000") // 之后挂买单接long的平仓

	// ---- 开仓：short挂卖0.1@65000，long吃单 ----
	sell := e.insertOrder(t, short, orderOpts{side: model.SideShort, action: model.ActionOpen, price: "65000", amount: "0.1", margin: "650"})
	if err := e.engine.SubmitOrder(ctx, sell, 1); err != nil {
		t.Fatal(err)
	}
	buy := e.insertOrder(t, long, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "65000", amount: "0.1", margin: "650"})
	if err := e.engine.SubmitOrder(ctx, buy, 2); err != nil {
		t.Fatal(err)
	}

	if got := e.order(t, sell.OrderID); got.Status != model.OrderStatusFilled || !got.TradedAmount.Equal(mustParse(t, "0.1")) {
		t.Fatalf("maker委托应该全部成交: %+v", got)
	}
	if got := e.order(t, buy.OrderID).Status; got != model.OrderStatusFilled {
		t.Fatalf("taker委托应该全部成交, status=%s", got)
	}
	// 名义价值6500：taker手续费3.25，maker手续费1.3；冻结的保证金650转成仓位占用，继续锁在frozen_margin里
	takerAcc, makerAcc := e.account(t, long), e.account(t, short)
	mustDec(t, e.freeBalance(t, long), "9346.75", "taker可用余额=10000-650保证金-3.25手续费")
	mustDec(t, takerAcc.FrozenMargin, "650", "taker冻结保证金(仓位占用)")
	mustDec(t, e.freeBalance(t, short), "9348.7", "maker可用余额=10000-650保证金-1.3手续费")
	mustDec(t, makerAcc.FrozenMargin, "650", "maker冻结保证金(仓位占用)")

	lp := e.position(t, long, model.SideLong)
	mustDec(t, lp.Volume, "0.1", "多头仓位数量")
	mustDec(t, lp.AvgEntryPrice, "65000", "多头开仓均价")
	mustDec(t, lp.PositionMargin, "650", "多头仓位保证金")
	sp := e.position(t, short, model.SideShort)
	mustDec(t, sp.Volume, "0.1", "空头仓位数量")
	mustDec(t, sp.PositionMargin, "650", "空头仓位保证金")

	mustDec(t, e.ledgerSum(t, long, model.TxFee), "-3.25", "taker手续费流水")
	mustDec(t, e.ledgerSum(t, short, model.TxFee), "-1.3", "maker手续费流水")

	var tradeCount int
	if err := e.db.Get(&tradeCount, `SELECT COUNT(*) FROM trades WHERE price = 65000 AND volume = 0.1`); err != nil || tradeCount != 1 {
		t.Fatalf("应该有1笔成交记录, got %d err=%v", tradeCount, err)
	}
	if mark, ok := e.markPrice.Get(ctx, testSymbol); !ok {
		t.Fatal("成交后应该有标记价格")
	} else {
		mustDec(t, mark, "65000", "标记价格")
	}

	// ---- 平仓：buyer挂买0.1@66000，long卖出平仓吃单，盈利(66000-65000)*0.1=100 ----
	bid := e.insertOrder(t, buyer, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "66000", amount: "0.1", margin: "660"})
	if err := e.engine.SubmitOrder(ctx, bid, 3); err != nil {
		t.Fatal(err)
	}
	closeOrder := e.insertOrder(t, long, orderOpts{side: model.SideLong, action: model.ActionClose, price: "66000", amount: "0.1"})
	if err := e.engine.SubmitOrder(ctx, closeOrder, 4); err != nil {
		t.Fatal(err)
	}

	if got := e.order(t, closeOrder.OrderID).Status; got != model.OrderStatusFilled {
		t.Fatalf("平仓委托应该全部成交, status=%s", got)
	}
	// 成交额6600，taker手续费3.3：平仓后650的锁定解除，balance=9996.75(开仓后)+100盈利-3.3手续费=10093.45
	closed := e.account(t, long)
	mustDec(t, closed.Balance, "10093.45", "平仓后可用余额")
	mustDec(t, closed.FrozenMargin, "0", "平仓后冻结保证金")
	lp = e.position(t, long, model.SideLong)
	mustDec(t, lp.Volume, "0", "平仓后仓位数量")
	if lp.Status != model.PositionStatusClosed {
		t.Fatalf("平仓后仓位应该是closed, got %s", lp.Status)
	}
	mustDec(t, e.ledgerSum(t, long, model.TxRealizedPnl), "100", "已实现盈亏流水")
	mustDec(t, e.ledgerSum(t, long, model.TxFee), "-6.55", "两次成交的手续费流水合计(3.25+3.3)")

	// 接盘的buyer(maker)：开多0.1@66000，手续费6600*0.0002=1.32，660保证金继续锁在frozen_margin里
	mustDec(t, e.freeBalance(t, buyer), "9338.68", "buyer可用余额=10000-660保证金-1.32手续费")
	mustDec(t, e.position(t, buyer, model.SideLong).AvgEntryPrice, "66000", "buyer开仓均价")
	// 没参与第二笔成交的short仓位不变
	mustDec(t, e.position(t, short, model.SideShort).Volume, "0.1", "无关仓位不受影响")
}

// 部分成交后撤销剩余部分：只按未成交比例退回冻结保证金，已成交部分的保证金留在仓位里
func TestSettlement_PartialFillThenCancelReleasesProportionalMargin(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	maker := e.newAccount(t, 1, "10000")
	taker := e.newAccount(t, 2, "10000")

	sell := e.insertOrder(t, maker, orderOpts{side: model.SideShort, action: model.ActionOpen, price: "65000", amount: "0.2", margin: "1300"})
	if err := e.engine.SubmitOrder(ctx, sell, 1); err != nil {
		t.Fatal(err)
	}
	buy := e.insertOrder(t, taker, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "65000", amount: "0.05", margin: "325"})
	if err := e.engine.SubmitOrder(ctx, buy, 2); err != nil {
		t.Fatal(err)
	}

	partial := e.order(t, sell.OrderID)
	if partial.Status != model.OrderStatusPartiallyFilled {
		t.Fatalf("maker应该是部分成交, status=%s", partial.Status)
	}
	mustDec(t, partial.TradedAmount, "0.05", "已成交数量")
	// 成交0.05：1300*0.05/0.2=325转成仓位占用(继续锁着)，剩下975还锁在未成交部分
	mustDec(t, e.account(t, maker).FrozenMargin, "1300", "部分成交后maker的锁定=仓位占用325+未成交部分975")

	if err := e.engine.CancelOrder(ctx, partial); err != nil {
		t.Fatalf("CancelOrder: %v", err)
	}

	if got := e.order(t, sell.OrderID).Status; got != model.OrderStatusCanceled {
		t.Fatalf("撤单后应该是canceled, status=%s", got)
	}
	acc := e.account(t, maker)
	mustDec(t, acc.FrozenMargin, "325", "撤单后只剩已成交部分的仓位占用还锁着")
	// 可用余额=10000 - 325(已成交部分的仓位保证金) - 手续费3250*0.0002=0.65，未成交的975解锁
	mustDec(t, e.freeBalance(t, maker), "9674.35", "撤单后可用余额")
	mustDec(t, e.position(t, maker, model.SideShort).PositionMargin, "325", "仓位保证金只有已成交部分")
	if e.book.BookFor(testSymbol).Contains(sell.OrderID) {
		t.Fatal("撤单后不应该还在订单簿里")
	}

	// 已经终结的委托再撤一次：不能重复退保证金
	before := e.freeBalance(t, maker)
	if err := e.engine.CancelOrder(ctx, e.order(t, sell.OrderID)); err != nil {
		t.Fatalf("重复撤单: %v", err)
	}
	mustDec(t, e.freeBalance(t, maker), before.String(), "重复撤单不能再退一次保证金")
}

// 自成交保护：同一个账户的买卖单撞上时不成交，被摘掉的挂单退回保证金，没有成交记录也没有手续费
func TestSettlement_SelfTradeIsPreventedAndRefunded(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	uid := e.newAccount(t, 1, "10000")

	sell := e.insertOrder(t, uid, orderOpts{side: model.SideShort, action: model.ActionOpen, price: "65000", amount: "0.1", margin: "650"})
	if err := e.engine.SubmitOrder(ctx, sell, 1); err != nil {
		t.Fatal(err)
	}
	buy := e.insertOrder(t, uid, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "65000", amount: "0.1", margin: "650"})
	if err := e.engine.SubmitOrder(ctx, buy, 2); err != nil {
		t.Fatal(err)
	}

	var tradeCount int
	if err := e.db.Get(&tradeCount, `SELECT COUNT(*) FROM trades`); err != nil || tradeCount != 0 {
		t.Fatalf("自成交不应该产生成交记录, got %d err=%v", tradeCount, err)
	}
	mustDec(t, e.ledgerSum(t, uid, model.TxFee), "0", "自成交不收手续费")
	// 被摘掉的那笔退回保证金，剩下的一笔仍然冻结着：总共只冻结一笔的650
	acc := e.account(t, uid)
	mustDec(t, acc.FrozenMargin, "650", "只剩一笔挂单在冻结保证金")
	mustDec(t, e.freeBalance(t, uid), "9350", "另一笔的保证金已经退回")
	canceled := 0
	for _, id := range []uint64{sell.OrderID, buy.OrderID} {
		if e.order(t, id).Status == model.OrderStatusCanceled {
			canceled++
		}
	}
	if canceled != 1 {
		t.Fatalf("自成交保护应该恰好撤掉一笔, got %d", canceled)
	}
}
