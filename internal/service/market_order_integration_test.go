//go:build integration

package service_test

import (
	"context"
	"testing"

	"perp-go/internal/model"
)

// 市价单：撮合时不带价格限制，按盘口对手的真实价格一档档吃，吃不满的剩余量撤销并释放保证金。
// contract-api下市价单时会把标记价存进委托的price字段(估算保证金和数量用)，那不是限价——
// 之前引擎把它原样传给撮合，市价单就变成了"按标记价成交的限价单"：市价买只能吃低于等于标记价的卖单、
// 市价卖只能吃高于等于标记价的买单，而盘口对手价几乎总是在标记价的另一侧，市价单基本一笔都成交不了

// 标记价65000，卖盘65100(0.1)、65200(0.1)都高于标记价。市价买0.15：吃65100的0.1、65200的0.05
func TestMarketOrder_BuySweepsAsksAboveMarkPrice(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	maker := e.newAccount(t, 1, "10000")
	taker := e.newAccount(t, 2, "10000")
	for i, price := range []string{"65100", "65200"} {
		ask := e.insertOrder(t, maker, orderOpts{side: model.SideShort, action: model.ActionOpen, price: price, amount: "0.1", margin: "652"})
		if err := e.engine.SubmitOrder(ctx, ask, int64(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	e.setMark(t, testSymbol, "65000")
	// contract-api按标记价估算：0.15*65000/10=975
	buy := e.insertOrder(t, taker, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "65000", amount: "0.15", margin: "975", market: true})

	if err := e.engine.SubmitOrder(ctx, buy, 3); err != nil {
		t.Fatal(err)
	}

	got := e.order(t, buy.OrderID)
	if got.Status != model.OrderStatusFilled {
		t.Fatalf("市价买单应该吃掉高于标记价的卖单并全部成交, status=%s traded=%s", got.Status, got.TradedAmount)
	}
	mustDec(t, got.TradedAmount, "0.15", "成交数量")
	// 成交额 0.1*65100+0.05*65200=9770：开仓保证金977(比冻结的975多2，从可用余额补)，taker手续费4.885
	p := e.position(t, taker, model.SideLong)
	mustDec(t, p.Volume, "0.15", "仓位数量")
	mustDec(t, p.PositionMargin, "977", "仓位保证金按真实成交额算")
	acc := e.account(t, taker)
	mustDec(t, acc.FrozenMargin, "977", "冻结的保证金转成了仓位占用，继续锁着")
	mustDec(t, e.freeBalance(t, taker), "9018.115", "可用=10000-977-4.885")
	if e.book.BookFor(testSymbol).Contains(buy.OrderID) {
		t.Fatal("市价单不挂簿")
	}
}

// 标记价65000，买盘64900(0.1)、64800(0.1)都低于标记价。市价卖(开空)0.15：吃64900的0.1、64800的0.05
func TestMarketOrder_SellSweepsBidsBelowMarkPrice(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	maker := e.newAccount(t, 1, "10000")
	taker := e.newAccount(t, 2, "10000")
	for i, price := range []string{"64900", "64800"} {
		bid := e.insertOrder(t, maker, orderOpts{side: model.SideLong, action: model.ActionOpen, price: price, amount: "0.1", margin: "649"})
		if err := e.engine.SubmitOrder(ctx, bid, int64(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	e.setMark(t, testSymbol, "65000")
	sell := e.insertOrder(t, taker, orderOpts{side: model.SideShort, action: model.ActionOpen, price: "65000", amount: "0.15", margin: "975", market: true})

	if err := e.engine.SubmitOrder(ctx, sell, 3); err != nil {
		t.Fatal(err)
	}

	got := e.order(t, sell.OrderID)
	if got.Status != model.OrderStatusFilled {
		t.Fatalf("市价卖单应该吃掉低于标记价的买单并全部成交, status=%s traded=%s", got.Status, got.TradedAmount)
	}
	// 成交额 0.1*64900+0.05*64800=9730：保证金973，taker手续费4.865
	p := e.position(t, taker, model.SideShort)
	mustDec(t, p.Volume, "0.15", "空头仓位数量")
	mustDec(t, p.PositionMargin, "973", "仓位保证金按真实成交额算")
	mustDec(t, e.freeBalance(t, taker), "9022.135", "可用=10000-973-4.865(冻结的975里多冻的2已退回)")
}

// 平仓的市价单同样要能吃到标记价另一侧的对手盘：多头持仓市价平仓(卖出)，买盘都在标记价之下
func TestMarketOrder_ClosePositionSweepsBidsBelowMarkPrice(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	holder := e.newAccount(t, 1, "10000")
	seller := e.newAccount(t, 2, "10000")
	e.openLongAgainst(t, holder, seller, testSymbol, "65000", "0.1", "650")
	bidder := e.newAccount(t, 3, "10000")
	bid := e.insertOrder(t, bidder, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "64900", amount: "0.1", margin: "649"})
	if err := e.engine.SubmitOrder(ctx, bid, 5); err != nil {
		t.Fatal(err)
	}
	e.setMark(t, testSymbol, "65000")
	closeOrder := e.insertOrder(t, holder, orderOpts{side: model.SideLong, action: model.ActionClose, price: "65000", amount: "0.1", market: true})

	if err := e.engine.SubmitOrder(ctx, closeOrder, 6); err != nil {
		t.Fatal(err)
	}

	if got := e.order(t, closeOrder.OrderID).Status; got != model.OrderStatusFilled {
		t.Fatalf("市价平仓单应该按买一64900成交, status=%s", got)
	}
	mustDec(t, e.position(t, holder, model.SideLong).Volume, "0", "仓位应该已经平掉")
	// 平仓亏(64900-65000)*0.1=-10
	mustDec(t, e.ledgerSum(t, holder, model.TxRealizedPnl), "-10", "已实现盈亏")
}

// 盘口不够吃：吃到多少算多少，剩余量撤销并按比例释放冻结保证金，市价单不挂簿
func TestMarketOrder_InsufficientLiquidityCancelsRemainderAndReleasesMargin(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	maker := e.newAccount(t, 1, "10000")
	taker := e.newAccount(t, 2, "10000")
	ask := e.insertOrder(t, maker, orderOpts{side: model.SideShort, action: model.ActionOpen, price: "65100", amount: "0.1", margin: "651"})
	if err := e.engine.SubmitOrder(ctx, ask, 1); err != nil {
		t.Fatal(err)
	}
	e.setMark(t, testSymbol, "65000")
	buy := e.insertOrder(t, taker, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "65000", amount: "0.3", margin: "1950", market: true})

	if err := e.engine.SubmitOrder(ctx, buy, 2); err != nil {
		t.Fatal(err)
	}

	got := e.order(t, buy.OrderID)
	if got.Status != model.OrderStatusCanceled {
		t.Fatalf("吃不满的市价单剩余部分应该撤销, status=%s", got.Status)
	}
	mustDec(t, got.TradedAmount, "0.1", "只吃到盘口上有的0.1")
	acc := e.account(t, taker)
	// 剩余0.2对应的锁定已释放，成交的0.1对应的651仓位保证金继续锁着
	mustDec(t, acc.FrozenMargin, "651", "剩余0.2对应的锁定已释放，成交部分仍锁着")
	// 成交额6510：保证金651，taker手续费3.255
	mustDec(t, e.freeBalance(t, taker), "9345.745", "可用=10000-651-3.255")
	if e.book.BookFor(testSymbol).Contains(buy.OrderID) {
		t.Fatal("市价单不挂簿")
	}
}

// ---- 价格保护带：市价单也不能无限滑点 ----
// 价格保护带只校验限价开仓单，市价单不设限的话，共谋账户在远离标记价处挂一笔平仓限价单(平仓单不校验价格带)，
// 受害者的市价买单会一路吃过去，保证金按标记价冻结、实际按远高于标记价的成交额结算，钱流向共谋账户。
// 所以市价单的撮合价是参考价±保护带比例(BTCUSDT种子数据里是5%)，超出的部分当作没有流动性，剩余量撤销

// 共谋场景：colluder持有多头，挂一笔130000的平仓卖单；受害者按标记价65000估算冻结保证金，市价买入。
// 卖单价格离标记价100%，超出保护带，一笔都不能成交，受害者的钱原样退回
func TestMarketOrder_CollusiveFarAwayCloseOrderIsNotSweptByVictim(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	colluder := e.newAccount(t, 1, "10000")
	helper := e.newAccount(t, 2, "10000")
	victim := e.newAccount(t, 3, "1000")
	e.openLongAgainst(t, colluder, helper, testSymbol, "65000", "0.1", "650")
	trap := e.insertOrder(t, colluder, orderOpts{side: model.SideLong, action: model.ActionClose, price: "130000", amount: "0.1"})
	if err := e.engine.SubmitOrder(ctx, trap, 5); err != nil {
		t.Fatal(err)
	}
	e.setMark(t, testSymbol, "65000")
	buy := e.insertOrder(t, victim, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "65000", amount: "0.1", margin: "650", market: true})

	if err := e.engine.SubmitOrder(ctx, buy, 6); err != nil {
		t.Fatal(err)
	}

	if got := e.order(t, buy.OrderID); got.Status != model.OrderStatusCanceled || !got.TradedAmount.IsZero() {
		t.Fatalf("超出保护带的卖单不能被市价单吃掉: status=%s traded=%s", got.Status, got.TradedAmount)
	}
	if got := e.order(t, trap.OrderID).Status; got != model.OrderStatusOpen {
		t.Fatalf("远离标记价的平仓卖单应该原样挂着, status=%s", got)
	}
	acc := e.account(t, victim)
	mustDec(t, acc.Balance, "1000", "受害者的钱原样退回，没有被吃穿")
	mustDec(t, acc.FrozenMargin, "0", "冻结的保证金已释放")
}

// 边界：参考价65000的5%是3250，卖单在68250(恰好+5%)能成交，68250.1(略超)不能
func TestMarketOrder_BuyProtectionBoundaryIsInclusive(t *testing.T) {
	for _, tc := range []struct {
		askPrice string
		fills    bool
	}{{"68250", true}, {"68250.1", false}} {
		e := newEngineEnv(t)
		ctx := context.Background()
		maker := e.newAccount(t, 1, "10000")
		taker := e.newAccount(t, 2, "10000")
		ask := e.insertOrder(t, maker, orderOpts{side: model.SideShort, action: model.ActionOpen, price: tc.askPrice, amount: "0.1", margin: "700"})
		if err := e.engine.SubmitOrder(ctx, ask, 1); err != nil {
			t.Fatal(err)
		}
		e.setMark(t, testSymbol, "65000")
		buy := e.insertOrder(t, taker, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "65000", amount: "0.1", margin: "650", market: true})

		if err := e.engine.SubmitOrder(ctx, buy, 2); err != nil {
			t.Fatal(err)
		}

		filled := e.order(t, buy.OrderID).Status == model.OrderStatusFilled
		if filled != tc.fills {
			t.Fatalf("卖单在%s时成交=%v，期望%v", tc.askPrice, filled, tc.fills)
		}
	}
}

// 卖出方向同样：参考价65000的-5%是61750，买单在61750能成交，61749.9不能
func TestMarketOrder_SellProtectionBoundaryIsInclusive(t *testing.T) {
	for _, tc := range []struct {
		bidPrice string
		fills    bool
	}{{"61750", true}, {"61749.9", false}} {
		e := newEngineEnv(t)
		ctx := context.Background()
		maker := e.newAccount(t, 1, "10000")
		taker := e.newAccount(t, 2, "10000")
		bid := e.insertOrder(t, maker, orderOpts{side: model.SideLong, action: model.ActionOpen, price: tc.bidPrice, amount: "0.1", margin: "620"})
		if err := e.engine.SubmitOrder(ctx, bid, 1); err != nil {
			t.Fatal(err)
		}
		e.setMark(t, testSymbol, "65000")
		sell := e.insertOrder(t, taker, orderOpts{side: model.SideShort, action: model.ActionOpen, price: "65000", amount: "0.1", margin: "650", market: true})

		if err := e.engine.SubmitOrder(ctx, sell, 2); err != nil {
			t.Fatal(err)
		}

		filled := e.order(t, sell.OrderID).Status == model.OrderStatusFilled
		if filled != tc.fills {
			t.Fatalf("买单在%s时成交=%v，期望%v", tc.bidPrice, filled, tc.fills)
		}
	}
}

// 保护带内的部分成交、超出的部分撤销：65100(在带内)吃掉0.1，68500(超出)不吃，剩余0.1撤销并释放保证金
func TestMarketOrder_FillsInsideBandAndCancelsRemainderBeyondIt(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	maker := e.newAccount(t, 1, "10000")
	taker := e.newAccount(t, 2, "10000")
	for i, price := range []string{"65100", "68500"} {
		ask := e.insertOrder(t, maker, orderOpts{side: model.SideShort, action: model.ActionOpen, price: price, amount: "0.1", margin: "690"})
		if err := e.engine.SubmitOrder(ctx, ask, int64(i+1)); err != nil {
			t.Fatal(err)
		}
	}
	e.setMark(t, testSymbol, "65000")
	buy := e.insertOrder(t, taker, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "65000", amount: "0.2", margin: "1300", market: true})

	if err := e.engine.SubmitOrder(ctx, buy, 3); err != nil {
		t.Fatal(err)
	}

	got := e.order(t, buy.OrderID)
	if got.Status != model.OrderStatusCanceled {
		t.Fatalf("超出保护带的剩余部分应该撤销, status=%s", got.Status)
	}
	mustDec(t, got.TradedAmount, "0.1", "只吃到保护带内的0.1")
	// 超出保护带的0.1对应的锁定已释放，成交的0.1对应的651仓位保证金继续锁着
	mustDec(t, e.account(t, taker).FrozenMargin, "651", "剩余部分的锁定已释放，成交部分仍锁着")
}
