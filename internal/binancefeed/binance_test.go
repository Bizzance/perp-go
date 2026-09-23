package binancefeed

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

func TestBinanceKlines(t *testing.T) {
	t.Run("解析：开盘时间、开高低收、成交量、成交笔数原样", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/fapi/v1/klines" || r.URL.Query().Get("symbol") != "BTCUSDT" || r.URL.Query().Get("interval") != "1m" || r.URL.Query().Get("limit") != "2" {
				t.Errorf("请求不对: %s", r.URL)
			}
			_, _ = io.WriteString(w, `[[1700000000000,"81000.10","81010.50","80995.00","81005.25","12.345",1700000059999,"1000000.5",321,"6.1","500000.2","0"],
				[1700000060000,"81005.25","81020.00","81001.10","81015.70","0.5",1700000119999,"40000.1",7,"0.2","16000","0"]]`)
		}))
		defer srv.Close()
		b := &Binance{BaseURL: srv.URL, Client: srv.Client()}
		cs, err := b.Klines(context.Background(), "BTCUSDT", "1m", 2)
		if err != nil || len(cs) != 2 {
			t.Fatalf("cs=%v err=%v", cs, err)
		}
		c := cs[0]
		if c.OpenTime != 1700000000000 || c.Open.String() != "81000.1" || c.High.String() != "81010.5" || c.Low.String() != "80995" ||
			c.Close.String() != "81005.25" || c.Volume.String() != "12.345" || c.Trades != 321 {
			t.Fatalf("第一根解析不对: %+v", c)
		}
		if cs[1].OpenTime != 1700000060000 || cs[1].Trades != 7 {
			t.Fatalf("第二根解析不对: %+v", cs[1])
		}
	})
	bad := map[string]string{
		"被地区屏蔽(451)": "451",
		"不是JSON":     `<html>`,
		"不是数组":       `{"code":-1121}`,
		"列数不够":       `[[1700000000000,"1","2","3","4","5"]]`,
		"开盘时间不是数字":   `[["x","1","2","3","4","5",0,"0",1]]`,
		"开盘时间是0":     `[[0,"1","2","3","4","5",0,"0",1]]`,
		"价格不是字符串":    `[[1700000000000,1,"2","3","4","5",0,"0",1]]`,
		"价格解析不出来":    `[[1700000000000,"abc","2","3","4","5",0,"0",1]]`,
		"成交笔数不是数字":   `[[1700000000000,"1","2","3","4","5",0,"0","x"]]`,
	}
	for name, body := range bad {
		t.Run("异常："+name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if body == "451" {
					w.WriteHeader(451)
					return
				}
				_, _ = io.WriteString(w, body)
			}))
			defer srv.Close()
			b := &Binance{BaseURL: srv.URL, Client: srv.Client()}
			if _, err := b.Klines(context.Background(), "BTCUSDT", "1m", 2); err == nil {
				t.Fatal("应该报错")
			}
		})
	}
}
