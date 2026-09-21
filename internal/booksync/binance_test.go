package booksync

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func serveDepth(t *testing.T, status int, body string) (*Binance, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fapi/v1/depth" || r.URL.Query().Get("symbol") != "BTCUSDT" || r.URL.Query().Get("limit") != "50" {
			t.Errorf("请求不对: %s", r.URL)
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return &Binance{BaseURL: srv.URL, Client: srv.Client()}, srv
}

func TestBinanceDepth_ParsesLevelsAsIs(t *testing.T) {
	b, _ := serveDepth(t, 200, `{"lastUpdateId":1,"bids":[["81276.5","1.234"],["81276.4","0.001"],["81276.3","0"]],"asks":[["81276.6","2.5"],["81276.7","0.75"]]}`)
	bids, asks, err := b.Depth(context.Background(), "BTCUSDT", 50)
	if err != nil {
		t.Fatal(err)
	}
	// 数量为0的档位丢掉，其余原样(价格、数量的字符串精度都不动)
	if len(bids) != 2 || bids[0].Price.String() != "81276.5" || bids[0].Qty.String() != "1.234" || bids[1].Price.String() != "81276.4" {
		t.Fatalf("bids = %v", bids)
	}
	if len(asks) != 2 || asks[0].Price.String() != "81276.6" || asks[1].Qty.String() != "0.75" {
		t.Fatalf("asks = %v", asks)
	}
}

// 拿不到可信的盘口一律报错，不能带着半截数据往下走
func TestBinanceDepth_Errors(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"被地区屏蔽(451)：状态码带在错误里", 451, `{"msg":"blocked"}`, "451"},
		{"被限流(429)", 429, `{"msg":"too many"}`, "429"},
		{"响应不是JSON", 200, `<html>`, "不是JSON"},
		{"买盘是空的", 200, `{"bids":[],"asks":[["100","1"]]}`, "空的"},
		{"卖盘是空的", 200, `{"bids":[["100","1"]],"asks":[]}`, "空的"},
		{"买一等于卖一(交叉)", 200, `{"bids":[["100","1"]],"asks":[["100","1"]]}`, "交叉"},
		{"买一高于卖一(交叉)", 200, `{"bids":[["101","1"]],"asks":[["100","1"]]}`, "交叉"},
		{"价格不是数字", 200, `{"bids":[["abc","1"]],"asks":[["100","1"]]}`, "价格不合法"},
		{"数量不是数字", 200, `{"bids":[["99","x"]],"asks":[["100","1"]]}`, "数量不合法"},
		{"价格是0", 200, `{"bids":[["0","1"]],"asks":[["100","1"]]}`, "不是正数"},
		{"档位缺字段", 200, `{"bids":[["99"]],"asks":[["100","1"]]}`, "格式不对"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, _ := serveDepth(t, c.status, c.body)
			_, _, err := b.Depth(context.Background(), "BTCUSDT", 50)
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want 包含%q", err, c.want)
			}
		})
	}
}

func TestBinanceIndexPrice(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   string // 空=应该报错
	}{
		{"正常：原样", 200, `{"symbol":"BTCUSDT","markPrice":"81300.1","indexPrice":"81276.55","lastFundingRate":"0.0001"}`, "81276.55"},
		{"被地区屏蔽(451)", 451, `blocked`, ""},
		{"缺指数价字段", 200, `{"markPrice":"81300.1"}`, ""},
		{"指数价是0", 200, `{"indexPrice":"0"}`, ""},
		{"指数价不是数字", 200, `{"indexPrice":"abc"}`, ""},
		{"响应不是JSON", 200, `<html>`, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/fapi/v1/premiumIndex" || r.URL.Query().Get("symbol") != "BTCUSDT" {
					t.Errorf("请求不对: %s", r.URL)
				}
				w.WriteHeader(c.status)
				_, _ = io.WriteString(w, c.body)
			}))
			defer srv.Close()
			b := &Binance{BaseURL: srv.URL, Client: srv.Client()}
			p, err := b.IndexPrice(context.Background(), "BTCUSDT")
			if c.want == "" {
				if err == nil {
					t.Fatalf("应该报错, got %s", p)
				}
				return
			}
			if err != nil || p.String() != c.want {
				t.Fatalf("p=%s err=%v, want %s", p, err, c.want)
			}
		})
	}
}
