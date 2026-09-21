//go:build integration

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
)

// POST /kline/sync：外部行情(币安)的K线整根覆盖写入，写完把变了的K线推给WebSocket订阅者

type fakeKlinePub struct {
	mu  sync.Mutex
	got []model.Kline
}

func (f *fakeKlinePub) PublishKline(_ context.Context, _ string, k model.Kline) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, k)
}

func (f *fakeKlinePub) take() []model.Kline {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.got
	f.got = nil
	return out
}

func (e *apiEnv) get(t *testing.T, path string) map[string]any {
	t.Helper()
	w := httptest.NewRecorder()
	e.handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	var m map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil {
		t.Fatalf("响应不是JSON: %v body=%s", err, w.Body.String())
	}
	return m
}

func newKlineEnv(t *testing.T) (*apiEnv, *fakeKlinePub) {
	t.Helper()
	e := newAPIEnv(t)
	pub := &fakeKlinePub{}
	e.srv.WithKlineSync(true, pub)
	return e, pub
}

// interval周期内、对齐好的n根K线的开盘时间(升序)，最后一根是当前这一根
func klineOpenTimes(interval model.KlineInterval, n int) []int64 {
	step := model.KlineIntervalMillis[interval]
	cur := time.Now().UnixMilli() / step * step
	out := make([]int64, n)
	for i := range out {
		out[i] = cur - int64(n-1-i)*step
	}
	return out
}

func candle(openTime int64, open, high, low, closeP, volume string, trades int) map[string]any {
	return map[string]any{"openTime": openTime, "open": open, "high": high, "low": low, "close": closeP, "volume": volume, "tradeCount": trades}
}

func syncBody(interval model.KlineInterval, candles ...map[string]any) map[string]any {
	return map[string]any{"symbol": testSymbol, "interval": string(interval), "candles": candles}
}

func klineRows(t *testing.T, e *apiEnv, interval model.KlineInterval) []map[string]any {
	t.Helper()
	resp := e.get(t, "/kline?symbol="+testSymbol+"&interval="+string(interval)+"&limit=2000")
	if resp["code"] != float64(200) {
		t.Fatalf("GET /kline失败: %v", resp)
	}
	raw, _ := resp["data"].([]any)
	out := make([]map[string]any, len(raw))
	for i, r := range raw {
		out[i] = r.(map[string]any)
	}
	return out
}

// 写入之后GET /kline读到的就是币安的数据(升序)；重复推送同一批结果不变，也不再推给订阅者；
// 币安上还没走完的那一根变了，是整根覆盖而不是合并(成交量、最高价可以变小)，只推变了的那一根
func TestKlineSync_WritesOverwritesAndPublishesOnlyChanges(t *testing.T) {
	e, pub := newKlineEnv(t)
	ts := klineOpenTimes(model.Kline1m, 3)
	first := syncBody(model.Kline1m,
		candle(ts[0], "100", "103", "99", "102", "10.5", 30),
		candle(ts[1], "102", "104", "101", "103", "8", 20),
		candle(ts[2], "103", "105", "102", "104", "3.25", 9))
	resp := e.post(t, "/kline/sync", first)
	if d := data(t, resp); d["written"] != float64(3) || d["changed"] != float64(3) {
		t.Fatalf("第一次: %v", d)
	}
	if got := pub.take(); len(got) != 3 {
		t.Fatalf("新K线都要推送, got %d", len(got))
	}
	rows := klineRows(t, e, model.Kline1m)
	if len(rows) != 3 || rows[0]["openTime"] != float64(ts[0]) || rows[2]["close"] != "104" || rows[0]["volume"] != "10.5" || rows[0]["tradeCount"] != float64(30) {
		t.Fatalf("读到的K线不对: %v", rows)
	}

	// 原样再推一次：不变，不推送
	if d := data(t, e.post(t, "/kline/sync", first)); d["changed"] != float64(0) {
		t.Fatalf("重复推送不该有变化: %v", d)
	}
	if got := pub.take(); len(got) != 0 {
		t.Fatalf("没变的K线不该再推, got %d", len(got))
	}

	// 当前这一根变了：最高价和成交量都比数据库里的小(整根覆盖，不是取GREATEST/累加)，只推这一根
	updated := syncBody(model.Kline1m,
		candle(ts[0], "100", "103", "99", "102", "10.5", 30),
		candle(ts[1], "102", "104", "101", "103", "8", 20),
		candle(ts[2], "103", "104.5", "102", "103.5", "2", 7))
	if d := data(t, e.post(t, "/kline/sync", updated)); d["changed"] != float64(1) {
		t.Fatalf("只有最后一根变了: %v", d)
	}
	got := pub.take()
	if len(got) != 1 || got[0].OpenTime != ts[2] || got[0].Interval != model.Kline1m || !got[0].Close.Equal(decimal.RequireFromString("103.5")) {
		t.Fatalf("只应该推最后一根: %+v", got)
	}
	last := klineRows(t, e, model.Kline1m)[2]
	if last["high"] != "104.5" || last["volume"] != "2" || last["tradeCount"] != float64(7) {
		t.Fatalf("应该是整根覆盖: %v", last)
	}
}

// 不同周期各自独立
func TestKlineSync_IntervalsAreIndependent(t *testing.T) {
	e, _ := newKlineEnv(t)
	t1 := klineOpenTimes(model.Kline1m, 1)[0]
	t2 := klineOpenTimes(model.Kline1h, 1)[0]
	data(t, e.post(t, "/kline/sync", syncBody(model.Kline1m, candle(t1, "1", "2", "1", "2", "1", 1))))
	data(t, e.post(t, "/kline/sync", syncBody(model.Kline1h, candle(t2, "5", "9", "4", "8", "7", 3))))
	if r := klineRows(t, e, model.Kline1m); len(r) != 1 || r[0]["open"] != "1" {
		t.Fatalf("1m: %v", r)
	}
	if r := klineRows(t, e, model.Kline1h); len(r) != 1 || r[0]["open"] != "5" {
		t.Fatalf("1h: %v", r)
	}
	if r := klineRows(t, e, model.Kline1d); len(r) != 0 {
		t.Fatalf("没同步的周期应该是空的: %v", r)
	}
}

// K线来源是我们自己的成交(默认)时，这个接口一律拒绝，一根都不写：不然两个来源的数据会混在一起
func TestKlineSync_RejectedWhenSourceIsTrades(t *testing.T) {
	e := newAPIEnv(t) // 没调WithKlineSync
	ts := klineOpenTimes(model.Kline1m, 1)[0]
	requireErr(t, e.post(t, "/kline/sync", syncBody(model.Kline1m, candle(ts, "1", "2", "1", "2", "1", 1))), 400, ErrKlineSourceNotExternal)
	if r := klineRows(t, e, model.Kline1m); len(r) != 0 {
		t.Fatalf("被拒绝的请求不能写入: %v", r)
	}
}

// 整批校验：任何一根不合法整批拒绝，一根都不写
func TestKlineSync_ValidationRejectsWholeBatch(t *testing.T) {
	e, pub := newKlineEnv(t)
	ts := klineOpenTimes(model.Kline1m, 3)
	ok0 := candle(ts[0], "100", "103", "99", "102", "10", 1)
	step := model.KlineIntervalMillis[model.Kline1m]
	cases := []struct {
		name string
		body map[string]any
		code string
	}{
		{"合约不存在", map[string]any{"symbol": "NOPEUSDT", "interval": "1m", "candles": []any{ok0}}, ErrSymbolNotFound},
		{"周期不合法", map[string]any{"symbol": testSymbol, "interval": "2m", "candles": []any{ok0}}, ErrInvalidParam},
		{"没有K线", syncBody(model.Kline1m), ErrInvalidParam},
		{"开盘时间没对齐到周期", syncBody(model.Kline1m, candle(ts[0]+1, "100", "103", "99", "102", "10", 1)), ErrInvalidParam},
		{"开盘时间不递增", syncBody(model.Kline1m, candle(ts[1], "1", "2", "1", "2", "1", 1), candle(ts[0], "1", "2", "1", "2", "1", 1)), ErrInvalidParam},
		{"开盘时间重复", syncBody(model.Kline1m, ok0, ok0), ErrInvalidParam},
		{"开盘时间在未来", syncBody(model.Kline1m, candle(ts[2]+5*step, "1", "2", "1", "2", "1", 1)), ErrInvalidParam},
		{"价格是0", syncBody(model.Kline1m, candle(ts[0], "0", "3", "1", "2", "1", 1)), ErrInvalidParam},
		{"最高价低于最低价", syncBody(model.Kline1m, candle(ts[0], "2", "1", "3", "2", "1", 1)), ErrInvalidParam},
		{"最高价低于收盘价", syncBody(model.Kline1m, candle(ts[0], "100", "101", "99", "102", "1", 1)), ErrInvalidParam},
		{"最高价低于开盘价", syncBody(model.Kline1m, candle(ts[0], "105", "103", "99", "102", "1", 1)), ErrInvalidParam},
		{"最低价高于开盘价", syncBody(model.Kline1m, candle(ts[0], "98", "103", "99", "102", "1", 1)), ErrInvalidParam},
		{"最低价高于收盘价", syncBody(model.Kline1m, candle(ts[0], "100", "103", "99.5", "99", "1", 1)), ErrInvalidParam},
		{"成交量是负数", syncBody(model.Kline1m, candle(ts[0], "100", "103", "99", "102", "-1", 1)), ErrInvalidParam},
		// 好的在前、坏的在后：前面那根也不能被写进去
		{"一批里有一根坏的", syncBody(model.Kline1m, ok0, candle(ts[1], "100", "99", "99", "102", "1", 1)), ErrInvalidParam},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			requireErr(t, e.post(t, "/kline/sync", c.body), 400, c.code)
		})
	}
	// 超过一次最多的根数
	many := make([]map[string]any, maxKlineSyncBatch+1)
	for i := range many {
		many[i] = ok0
	}
	requireErr(t, e.post(t, "/kline/sync", syncBody(model.Kline1m, many...)), 400, ErrInvalidParam)

	if r := klineRows(t, e, model.Kline1m); len(r) != 0 {
		t.Fatalf("被拒绝的批次一根都不能写入: %v", r)
	}
	if got := pub.take(); len(got) != 0 {
		t.Fatalf("被拒绝的批次不能推送: %d", len(got))
	}
}

// 一次最多1500根（币安K线接口的上限）刚好可以
func TestKlineSync_AcceptsMaxBatch(t *testing.T) {
	e, _ := newKlineEnv(t)
	ts := klineOpenTimes(model.Kline1m, maxKlineSyncBatch)
	cs := make([]map[string]any, len(ts))
	for i, tm := range ts {
		cs[i] = candle(tm, "100", "101", "99", "100", strconv.Itoa(i), 1)
	}
	if d := data(t, e.post(t, "/kline/sync", syncBody(model.Kline1m, cs...))); d["written"] != float64(maxKlineSyncBatch) {
		t.Fatalf("%v", d)
	}
	if r := klineRows(t, e, model.Kline1m); len(r) != maxKlineSyncBatch {
		t.Fatalf("读到%d根", len(r))
	}
}

// 最新价：K线来自币安(external)时用最近一根1分钟K线的收盘价，也就是币安的最新价——即使我们自己有成交也不用成交价
// (成交少的时候最后一笔成交价会停很久，跟币安差很多)；24h统计来自币安的1小时K线
func TestTicker_ExternalSourceLastPriceIsTheLatestKlineClose(t *testing.T) {
	e, _ := newKlineEnv(t)
	ticker := func() map[string]any { return data(t, e.get(t, "/market/ticker?symbol="+testSymbol)) }
	if got := ticker(); got["lastPrice"] != nil {
		t.Fatalf("什么数据都没有时最新价是null: %v", got)
	}

	m := klineOpenTimes(model.Kline1m, 2)
	data(t, e.post(t, "/kline/sync", syncBody(model.Kline1m,
		candle(m[0], "80000", "80010", "79990", "80005", "1", 1),
		candle(m[1], "80005", "80020", "80000", "80015.5", "2", 2))))
	h := klineOpenTimes(model.Kline1h, 2)
	data(t, e.post(t, "/kline/sync", syncBody(model.Kline1h,
		candle(h[0], "79000", "80500", "78800", "80000", "100.5", 900),
		candle(h[1], "80000", "80600", "79900", "80015.5", "50", 400))))

	got := ticker()
	if got["lastPrice"] != "80015.5" {
		t.Fatalf("最新价应该是最近一根1分钟K线的收盘价: %v", got)
	}
	// 24h: 开=第一根1h的开79000，高=80600，低=78800，成交量=100.5+50，涨跌=(80015.5-79000)/79000
	if got["open24h"] != "79000" || got["high24h"] != "80600" || got["low24h"] != "78800" || got["volume24h"] != "150.5" {
		t.Fatalf("24h统计应该来自币安的1小时K线: %v", got)
	}
	want := decimal.RequireFromString("80015.5").Sub(decimal.NewFromInt(79000)).Div(decimal.NewFromInt(79000))
	if c, _ := decimal.NewFromString(got["change24h"].(string)); c.Sub(want).Abs().GreaterThan(decimal.RequireFromString("0.0000000001")) {
		t.Fatalf("change24h = %v, want %s", got["change24h"], want)
	}

	// 我们自己有一笔成交(价格跟币安差了几美元)：最新价仍然是币安的
	if _, err := e.db.Exec(`INSERT INTO trades (trade_id, symbol, price, volume, buy_order_id, sell_order_id, buy_uid, sell_uid, maker_order_id, create_time)
		VALUES (?, ?, ?, 1, 1, 2, 3, 4, 2, ?)`, e.id(), testSymbol, "80012.3", time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if got := ticker(); got["lastPrice"] != "80015.5" {
		t.Fatalf("K线来自币安时最新价不能用我们自己的成交价: %v", got)
	}
}

// K线来自我们自己的成交(默认)：最新价是最后一笔成交价；还没有成交时退回最近一根K线的收盘价
func TestTicker_TradesModeLastPriceIsTheLastTradeWithKlineFallback(t *testing.T) {
	e := newAPIEnv(t) // 没调WithKlineSync：trades模式
	ticker := func() map[string]any { return data(t, e.get(t, "/market/ticker?symbol="+testSymbol)) }
	m := klineOpenTimes(model.Kline1m, 1)[0]
	if err := e.srv.klines.ReplaceBatch(context.Background(), testSymbol, model.Kline1m,
		[]model.Kline{{OpenTime: m, Open: decimal.NewFromInt(100), High: decimal.NewFromInt(102), Low: decimal.NewFromInt(99), Close: decimal.RequireFromString("101.5")}}, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if got := ticker(); got["lastPrice"] != "101.5" {
		t.Fatalf("没有成交时退回最近一根K线的收盘价: %v", got)
	}
	if _, err := e.db.Exec(`INSERT INTO trades (trade_id, symbol, price, volume, buy_order_id, sell_order_id, buy_uid, sell_uid, maker_order_id, create_time)
		VALUES (?, ?, ?, 1, 1, 2, 3, 4, 2, ?)`, e.id(), testSymbol, "100.7", time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if got := ticker(); got["lastPrice"] != "100.7" {
		t.Fatalf("有成交时最新价是最后一笔成交价: %v", got)
	}
}
