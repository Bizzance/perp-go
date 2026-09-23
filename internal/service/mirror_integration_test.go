//go:build integration

package service_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"perp-go/internal/binancefeed"
	"perp-go/internal/model"
	"perp-go/internal/service"
)

// 假的币安深度接口：MirrorService只调用GET /fapi/v1/depth
type fakeMirrorBinance struct {
	srv *httptest.Server
	mu  sync.Mutex
	// symbol -> {bids, asks}，每档["价格","数量"]
	books map[string][2][][2]string
	down  bool // true时全部请求返回500，模拟拿不到币安数据
}

func newFakeMirrorBinance(t *testing.T) *fakeMirrorBinance {
	f := &fakeMirrorBinance{books: map[string][2][][2]string{}}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.down {
			w.WriteHeader(500)
			return
		}
		bk, ok := f.books[r.URL.Query().Get("symbol")]
		if !ok {
			w.WriteHeader(400)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"bids": bk[0], "asks": bk[1]})
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeMirrorBinance) set(symbol string, bids, asks [][2]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.books[symbol] = [2][][2]string{bids, asks}
}

func (f *fakeMirrorBinance) setDown(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = v
}

// 起一个接到真实引擎的MirrorService，Levels/Interval/Leverage/StaleAfter用测试常用的值。
// 系统账户uid是固定值service.UID(不需要配置、也不需要充值)，每个测试用自己独立的MySQL库，
// 不会跟别的测试冲突
func newMirrorEnv(t *testing.T, e *engineEnv, staleAfter time.Duration) (*service.MirrorService, *fakeMirrorBinance, uint64) {
	t.Helper()
	bn := newFakeMirrorBinance(t)
	cfg := service.MirrorConfig{
		Symbols: []string{testSymbol}, Levels: 2, Interval: time.Second,
		Leverage: 5, StaleAfter: staleAfter,
	}
	m, err := service.NewMirrorService(cfg, &binancefeed.Binance{BaseURL: bn.srv.URL, Client: bn.srv.Client()}, e.accounts, e.orders, e.coins, e.engine)
	if err != nil {
		t.Fatal(err)
	}
	return m, bn, service.UID
}

func liveOrderPrices(t *testing.T, e *engineEnv, uid uint64, side model.Side) []string {
	t.Helper()
	orders, err := e.orders.FindActiveByUID(context.Background(), uid, testSymbol)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, o := range orders {
		if o.Side == side {
			out = append(out, fmt.Sprintf("%s:%s", o.Price, o.Amount))
		}
	}
	return out
}

// 第一轮：建好系统账户(不需要充值)，币安盘口的前2档原样镜像成真实挂单(数量按合约精度3位取整)，
// 买盘/卖盘都进同一个账户，方向分别是long/short
func TestMirror_PlacesRestingOrdersMatchingBinanceDepth(t *testing.T) {
	e := newEngineEnv(t)
	m, bn, uid := newMirrorEnv(t, e, 10*time.Second)
	bn.set(testSymbol, [][2]string{{"65000", "1"}, {"64990", "2"}}, [][2]string{{"65010", "1"}, {"65020", "2"}})

	m.Step(context.Background())

	bids := liveOrderPrices(t, e, uid, model.SideLong)
	asks := liveOrderPrices(t, e, uid, model.SideShort)
	if len(bids) != 2 || len(asks) != 2 {
		t.Fatalf("应该各挂2档: bids=%v asks=%v", bids, asks)
	}
	depth := e.book.BookFor(testSymbol).Depth(2)
	if len(depth.Bids) != 2 || !depth.Bids[0].Price.Equal(mustParse(t, "65000")) || !depth.Asks[0].Price.Equal(mustParse(t, "65010")) {
		t.Fatalf("订单簿深度应该反映镜像挂单: %+v", depth)
	}
	acc := e.account(t, uid)
	// 系统账户不需要充值：balance一直是0，锁定的保证金全部来自FreezeMargin对这个uid的
	// 无条件冻结路径(不检查balance够不够)
	mustDec(t, acc.Balance, "0", "系统账户不需要充值")
	if acc.FrozenMargin.Sign() <= 0 {
		t.Fatalf("系统账户应该已经建好、挂单已经锁定保证金: %+v", acc)
	}
}

// 系统账户代表系统自己的资金，视为无限：即使balance从来没有充值过、frozen_margin远超
// balance(自由余额深度为负)，FreezeMargin对这个uid依然无条件成功——这是修复"镜像挂单
// 被真实成交吃掉之后，系统账户余额耗尽、某一侧订单簿再也补不上"这个历史问题的核心行为
func TestMirror_PlaceOrderSucceedsRegardlessOfBalance(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	uid := service.UID
	if _, _, err := e.accounts.Create(ctx, uid); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := e.accounts.FreezeMargin(ctx, uid, decimalOf(t, "1000000")); err != nil {
			t.Fatalf("第%d次冻结不应该失败: %v", i+1, err)
		}
	}
	acc := e.account(t, uid)
	mustDec(t, acc.Balance, "0", "系统账户balance从来没有充值过")
	mustDec(t, acc.FrozenMargin, "5000000", "累计冻结的保证金远超balance，自由余额是深度负数")
}

// 系统账户不受强平约束：即使它名下的仓位浮亏巨大、权益远低于任何强平线，
// LiquidationService.RiskScanOnce也不会对它做任何事
func TestMirror_SystemAccountNeverLiquidated(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	uid := service.UID
	if _, _, err := e.accounts.Create(ctx, uid); err != nil {
		t.Fatal(err)
	}
	other := e.newAccount(t, 1, "10000")

	// 系统账户空头0.1@65000，标记价拉到极高，制造巨额浮亏。保证金走FreezeMargin对这个uid
	// 的无条件冻结路径(balance从来没充值过)
	freeze, err := e.accounts.FreezeMargin(ctx, uid, decimalOf(t, "650"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	sell := &model.Order{
		OrderID: e.id(), UID: uid, Symbol: testSymbol, Side: model.SideShort, Action: model.ActionOpen,
		Type: model.OrderTypeLimit, Price: decimalOf(t, "65000"), Amount: decimalOf(t, "0.1"),
		FrozenMargin: freeze.FromAvailable, FrozenCredit: freeze.FromCredit, Leverage: 10,
		Status: model.OrderStatusOpen, CreateTime: now, UpdateTime: now,
	}
	if err := e.orders.Insert(ctx, sell); err != nil {
		t.Fatal(err)
	}
	if err := e.engine.SubmitOrder(ctx, sell, 1); err != nil {
		t.Fatal(err)
	}
	buy := e.insertOrder(t, other, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "65000", amount: "0.1", margin: "650"})
	if err := e.engine.SubmitOrder(ctx, buy, 2); err != nil {
		t.Fatal(err)
	}
	e.setMark(t, testSymbol, "10000000")

	e.liq.RiskScanOnce(ctx)

	p := e.position(t, uid, model.SideShort)
	if p.Status != model.PositionStatusNormal || p.Volume.Sign() <= 0 {
		t.Fatalf("系统账户的仓位不应该被强平: %+v", p)
	}
	if n := len(e.liquidationOrders(t, uid)); n != 0 {
		t.Fatalf("系统账户不应该挂出强平单, got %d", n)
	}
}

// 盘口没变：第二轮不产生新的挂单/撤单(不会天天撤了重挂丢排队位置)
func TestMirror_UnchangedBookDoesNothing(t *testing.T) {
	e := newEngineEnv(t)
	m, bn, uid := newMirrorEnv(t, e, 10*time.Second)
	bn.set(testSymbol, [][2]string{{"65000", "1"}}, [][2]string{{"65010", "1"}})
	ctx := context.Background()
	m.Step(ctx)
	before, err := e.orders.FindActiveByUID(ctx, uid, testSymbol)
	if err != nil {
		t.Fatal(err)
	}
	m.Step(ctx)
	after, err := e.orders.FindActiveByUID(ctx, uid, testSymbol)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != len(after) || before[0].OrderID != after[0].OrderID {
		t.Fatalf("盘口没变不该有任何撤挂: before=%+v after=%+v", before, after)
	}
}

// 端到端：镜像挂出的卖单是真实挂单，用户的市价买单能正常吃到它、正常结算——这是这次重构最核心的
// 不变量：撮合、结算、持仓这些逻辑必须跟真实U本位合约完全一样，变的只是镜像挂单怎么进订单簿
func TestMirror_UserMarketOrderFillsAgainstMirroredLiquidity(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	m, bn, uid := newMirrorEnv(t, e, 10*time.Second)
	bn.set(testSymbol, [][2]string{{"65000", "1"}}, [][2]string{{"65010", "1"}})
	m.Step(ctx)

	e.setMark(t, testSymbol, "65000")
	taker := e.newAccount(t, 1, "10000")
	buy := e.insertOrder(t, taker, orderOpts{side: model.SideLong, action: model.ActionOpen, price: "65000", amount: "0.5", margin: "3250", market: true})
	if err := e.engine.SubmitOrder(ctx, buy, 1); err != nil {
		t.Fatal(err)
	}

	got := e.order(t, buy.OrderID)
	if got.Status != model.OrderStatusFilled {
		t.Fatalf("市价买单应该吃到镜像卖单全部成交, status=%s traded=%s", got.Status, got.TradedAmount)
	}
	mustDec(t, got.TradedAmount, "0.5", "成交数量")
	mustDec(t, got.AvgDealPrice, "65010", "成交均价")

	takerPos := e.position(t, taker, model.SideLong)
	mustDec(t, takerPos.Volume, "0.5", "用户仓位数量")
	mustDec(t, takerPos.PositionMargin, "3250.5", "用户仓位保证金=0.5*65010/10")

	mirrorPos := e.position(t, uid, model.SideShort)
	mustDec(t, mirrorPos.Volume, "0.5", "镜像账户被吃掉部分应该正常开出空头仓位，结算逻辑跟真实用户完全一样")
}

// 拿不到可信的币安盘口：StaleAfter以内不动已经挂着的镜像单，超过就撤光、暂停报价，恢复后重新挂。
// 用真实的短时长(50ms)+真实sleep，不用假时钟——这个组件不暴露时钟注入口子给测试(跟生产代码
// 一样按真实时间走)，其它涉及超时的集成测试(比如强平单排队超时)也是这个套路，见newEngineEnv
func TestMirror_StaleSourceCancelsAndResumes(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	m, bn, uid := newMirrorEnv(t, e, 50*time.Millisecond)
	bn.set(testSymbol, [][2]string{{"65000", "1"}}, [][2]string{{"65010", "1"}})
	m.Step(ctx)
	if orders, _ := e.orders.FindActiveByUID(ctx, uid, testSymbol); len(orders) != 2 {
		t.Fatalf("应该先挂好2笔, got %d", len(orders))
	}

	bn.setDown(true)
	m.Step(ctx)
	if orders, _ := e.orders.FindActiveByUID(ctx, uid, testSymbol); len(orders) != 2 {
		t.Fatal("还没超过StaleAfter，不能动镜像挂单")
	}

	time.Sleep(60 * time.Millisecond) // 超过StaleAfter(50ms)
	m.Step(ctx)
	if orders, _ := e.orders.FindActiveByUID(ctx, uid, testSymbol); len(orders) != 0 {
		t.Fatalf("超过StaleAfter应该撤光, 还剩%d笔", len(orders))
	}

	bn.setDown(false)
	m.Step(ctx)
	if orders, _ := e.orders.FindActiveByUID(ctx, uid, testSymbol); len(orders) != 2 {
		t.Fatalf("币安数据恢复后应该重新挂好, got %d", len(orders))
	}
}
