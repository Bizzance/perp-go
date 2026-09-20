package indexfeed

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/api"
)

// 假的交易所：按路径返回固定响应，记下收到的查询串。响应体是三家真实接口的格式(精简了无关字段)
type fakeExchange struct {
	*httptest.Server
	mu    sync.Mutex
	query string
}

func newFakeExchange(t *testing.T, status int, body string) *fakeExchange {
	t.Helper()
	fx := &fakeExchange{}
	fx.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fx.mu.Lock()
		fx.query = r.URL.Path + "?" + r.URL.RawQuery
		fx.mu.Unlock()
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(fx.Close)
	return fx
}

func (f *fakeExchange) gotQuery() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.query
}

const (
	binanceOK = `{"symbol":"BTCUSDT","markPrice":"81139.21150000","indexPrice":"81172.64521739","estimatedSettlePrice":"80936.15","lastFundingRate":"0.00007212","time":1789920912000}`
	okxOK     = `{"code":"0","data":[{"instId":"BTC-USDT","idxPx":"81177.7","high24h":"81910.3","ts":"1789920912565"}],"msg":""}`
	bybitOK   = `{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[{"symbol":"BTCUSDT","lastPrice":"81142.20","indexPrice":"81176.62","markPrice":"81142.63"}]}}`
)

func fetch(t *testing.T, s Source, symbol string) (decimal.Decimal, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return s.Fetch(ctx, symbol)
}

func TestSources_ParseRealResponseFormats(t *testing.T) {
	bn := newFakeExchange(t, 200, binanceOK)
	ok := newFakeExchange(t, 200, okxOK)
	by := newFakeExchange(t, 200, bybitOK)
	c := http.DefaultClient
	cases := []struct {
		src   Source
		fx    *fakeExchange
		want  string
		query string
	}{
		{&Binance{BaseURL: bn.URL, Client: c}, bn, "81172.64521739", "/fapi/v1/premiumIndex?symbol=BTCUSDT"},
		{&OKX{BaseURL: ok.URL, Client: c}, ok, "81177.7", "/api/v5/market/index-tickers?instId=BTC-USDT"},
		{&Bybit{BaseURL: by.URL, Client: c}, by, "81176.62", "/v5/market/tickers?category=linear&symbol=BTCUSDT"},
	}
	for _, tc := range cases {
		t.Run(tc.src.Name(), func(t *testing.T) {
			got, err := fetch(t, tc.src, "BTCUSDT")
			if err != nil {
				t.Fatal(err)
			}
			if !got.Equal(d(tc.want)) {
				t.Fatalf("price = %s, want %s", got, tc.want)
			}
			if q := tc.fx.gotQuery(); q != tc.query {
				t.Fatalf("请求 = %s, want %s", q, tc.query)
			}
		})
	}
}

func TestSources_RejectBadResponses(t *testing.T) {
	c := http.DefaultClient
	cases := []struct {
		name string
		mk   func(url string) Source
		code int
		body string
	}{
		{"币安非200", func(u string) Source { return &Binance{BaseURL: u, Client: c} }, 451, `{"msg":"restricted location"}`},
		{"币安不是JSON", func(u string) Source { return &Binance{BaseURL: u, Client: c} }, 200, `<html>`},
		{"币安没有indexPrice", func(u string) Source { return &Binance{BaseURL: u, Client: c} }, 200, `{"symbol":"BTCUSDT"}`},
		{"币安价格是0", func(u string) Source { return &Binance{BaseURL: u, Client: c} }, 200, `{"indexPrice":"0"}`},
		{"币安价格是负数", func(u string) Source { return &Binance{BaseURL: u, Client: c} }, 200, `{"indexPrice":"-5"}`},
		{"OKX业务错误", func(u string) Source { return &OKX{BaseURL: u, Client: c} }, 200, `{"code":"51001","msg":"Instrument ID does not exist","data":[]}`},
		{"OKX业务错误但带着数据", func(u string) Source { return &OKX{BaseURL: u, Client: c} }, 200, `{"code":"50011","msg":"Too Many Requests","data":[{"idxPx":"81000"}]}`},
		{"币安HTTP 500但响应体是合法价格", func(u string) Source { return &Binance{BaseURL: u, Client: c} }, 500, `{"indexPrice":"81000"}`},
		{"Bybit业务错误但带着数据", func(u string) Source { return &Bybit{BaseURL: u, Client: c} }, 200, `{"retCode":10006,"retMsg":"Too many visits","result":{"list":[{"indexPrice":"81000"}]}}`},
		{"OKX没有数据", func(u string) Source { return &OKX{BaseURL: u, Client: c} }, 200, `{"code":"0","data":[],"msg":""}`},
		{"OKX价格不是数字", func(u string) Source { return &OKX{BaseURL: u, Client: c} }, 200, `{"code":"0","data":[{"idxPx":"abc"}]}`},
		{"Bybit业务错误", func(u string) Source { return &Bybit{BaseURL: u, Client: c} }, 200, `{"retCode":10001,"retMsg":"params error","result":{}}`},
		{"Bybit列表为空", func(u string) Source { return &Bybit{BaseURL: u, Client: c} }, 200, `{"retCode":0,"result":{"list":[]}}`},
		{"Bybit指数价为空串", func(u string) Source { return &Bybit{BaseURL: u, Client: c} }, 200, `{"retCode":0,"result":{"list":[{"indexPrice":""}]}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFakeExchange(t, tc.code, tc.body)
			if p, err := fetch(t, tc.mk(fx.URL), "BTCUSDT"); err == nil {
				t.Fatalf("应该报错，却返回了价格%s", p)
			}
		})
	}
}

func TestOKXInstID(t *testing.T) {
	for in, want := range map[string]string{"BTCUSDT": "BTC-USDT", "ETHUSDT": "ETH-USDT", "1000PEPEUSDT": "1000PEPE-USDT"} {
		got, err := okxInstID(in)
		if err != nil || got != want {
			t.Errorf("okxInstID(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"BTCUSD", "USDT", "", "BTCBUSD"} {
		if got, err := okxInstID(bad); err == nil {
			t.Errorf("okxInstID(%q) 应该报错，got %q", bad, got)
		}
	}
}

func TestSource_ResponseTooLargeIsRejected(t *testing.T) {
	fx := newFakeExchange(t, 200, `{"indexPrice":"1","pad":"`+strings.Repeat("x", maxResponseBytes)+`"}`)
	_, err := fetch(t, &Binance{BaseURL: fx.URL, Client: http.DefaultClient}, "BTCUSDT")
	if err == nil || !strings.Contains(err.Error(), "超过") {
		t.Fatalf("超大响应应该被明确拒绝(错误里说明超过上限), got %v", err)
	}
}

// ---- 发布 ----

// 发布出去的请求：签名用生产的api.SignRequest重新算一遍，方法、路径、请求体都要对得上
func TestAPIPublisher_SignsExactlyWhatItSends(t *testing.T) {
	var got struct {
		method, path, rawQuery, key string
		body                        []byte
		sigOK                       bool
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got.method, got.path, got.rawQuery, got.body = r.Method, r.URL.EscapedPath(), r.URL.RawQuery, b
		got.key = r.Header.Get("X-Api-Key")
		want := api.SignRequest("s3cret-s3cret-s3cret", r.Header.Get("X-Timestamp"), r.Header.Get("X-Nonce"), r.Method, r.URL.EscapedPath(), r.URL.RawQuery, b)
		got.sigOK = r.Header.Get("X-Signature") == want && r.Header.Get("X-Nonce") != ""
		_, _ = io.WriteString(w, `{"code":200,"message":"success","data":null}`)
	}))
	defer srv.Close()

	p := &APIPublisher{BaseURL: srv.URL + "/", KeyID: "feeder", Secret: "s3cret-s3cret-s3cret", Client: srv.Client()}
	if err := p.Publish(context.Background(), "BTCUSDT", d("81172.5")); err != nil {
		t.Fatal(err)
	}
	if got.method != "POST" || got.path != "/index-price" || got.rawQuery != "" || got.key != "feeder" {
		t.Fatalf("请求不对: %+v", got)
	}
	if !got.sigOK {
		t.Fatal("签名对不上")
	}
	var body map[string]string
	if err := json.Unmarshal(got.body, &body); err != nil || body["symbol"] != "BTCUSDT" || body["price"] != "81172.5" {
		t.Fatalf("请求体不对: %s %v", got.body, err)
	}
}

// 业务错误也是HTTP 200，错误在响应体里，发布方必须识别出来，不能当成功
func TestAPIPublisher_BusinessErrorsAreErrors(t *testing.T) {
	cases := map[string]string{
		"权限不够":     `{"code":403,"errCode":"forbidden","message":"没有权限"}`,
		"参数不合法":    `{"code":400,"errCode":"invalid_param","message":"price参数不合法"}`,
		"响应不是JSON": `<html>bad gateway</html>`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) }))
			defer srv.Close()
			p := &APIPublisher{BaseURL: srv.URL, KeyID: "k", Secret: "s", Client: srv.Client()}
			if err := p.Publish(context.Background(), "BTCUSDT", d("1")); err == nil {
				t.Fatal("应该报错")
			}
		})
	}
}

// contract-api的服务端跳变保护拦下推送(errCode=index_price_jump)：要能用errors.Is认出来，
// 喂价器据此不把它当接口故障；其它业务错误不能被误认成它
func TestAPIPublisher_ServerJumpGuardIsRecognized(t *testing.T) {
	respond := func(body string) error {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, body) }))
		defer srv.Close()
		p := &APIPublisher{BaseURL: srv.URL, KeyID: "k", Secret: "s", Client: srv.Client()}
		return p.Publish(context.Background(), "BTCUSDT", d("70000"))
	}
	err := respond(`{"code":400,"errCode":"index_price_jump","message":"变动超过服务端阈值(已持续1.0秒)"}`)
	if !errors.Is(err, ErrJumpGuard) {
		t.Fatalf("应该认出是服务端跳变保护: %v", err)
	}
	if !strings.Contains(err.Error(), "已持续1.0秒") {
		t.Fatalf("服务端的说明要带上: %v", err)
	}
	for _, body := range []string{
		`{"code":400,"errCode":"invalid_param","message":"price参数不合法"}`,
		`{"code":403,"errCode":"forbidden","message":"没有权限"}`,
	} {
		if err := respond(body); err == nil || errors.Is(err, ErrJumpGuard) {
			t.Fatalf("%s 不是跳变保护: %v", body, err)
		}
	}
}
