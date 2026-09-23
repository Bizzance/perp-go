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

// 起一个接到真实引擎的MirrorService，Levels/Interval/Leverage/Balance/StaleAfter用测试常用的值。
// 系统账户uid是固定值service.UID(不需要配置)，每个测试用自己独立的MySQL库，不会跟别的测试冲突
func newMirrorEnv(t *testing.T, e *engineEnv, staleAfter time.Duration) (*service.MirrorService, *fakeMirrorBinance, uint64) {
	t.Helper()
	bn := newFakeMirrorBinance(t)
	cfg := service.MirrorConfig{
		Symbols: []string{testSymbol}, Levels: 2, Interval: time.Second,
		Leverage: 5, Balance: "1000000", StaleAfter: staleAfter,
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

// 第一轮：建好系统账户并充值，币安盘口的前2档原样镜像成真实挂单(数量按合约精度3位取整)，
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
	if acc.Balance.Sign() <= 0 && acc.FrozenMargin.Sign() <= 0 {
		t.Fatalf("系统账户应该已经建好并充值: %+v", acc)
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
