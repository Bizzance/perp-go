//go:build integration

package api

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/service"
)

// POST /index-price的服务端跳变保护，验证HTTP层的响应：被拦下返回400+index_price_jump，
// 推价方(orderbook-sync)靠这个errCode认出"服务端在等确认"，不能把它当成别的错误。判断逻辑本身见
// service包的TestDecideIndexPush_*和TestPushIndexPrice_*
func TestSetIndexPrice_JumpGuardResponses(t *testing.T) {
	e := newAPIEnv(t)
	var clock atomic.Int64
	clock.Store(time.Now().UnixMilli())
	cfg := service.DefaultMarkPriceConfig()
	cfg.IndexMaxJump = decimal.RequireFromString("0.05")
	e.srv.markPrice.WithConfig(cfg).WithClock(clock.Load)

	index := func() string {
		p, ok := e.srv.markPrice.GetIndexPrice(context.Background(), testSymbol)
		if !ok {
			t.Fatal("应该有指数价")
		}
		return p.String()
	}

	data(t, e.post(t, "/index-price", map[string]any{"symbol": testSymbol, "price": "60000"}))
	data(t, e.post(t, "/index-price", map[string]any{"symbol": testSymbol, "price": "61000"})) // +1.7%，正常
	if index() != "61000" {
		t.Fatalf("指数价 = %s", index())
	}

	resp := e.post(t, "/index-price", map[string]any{"symbol": testSymbol, "price": "90000"})
	requireErr(t, resp, 400, ErrIndexPriceJump)
	msg, _ := resp["message"].(string)
	if !strings.Contains(msg, "61000") || !strings.Contains(msg, "0.0秒") {
		t.Fatalf("message要带上当前指数价和已持续时间: %q", msg)
	}
	if index() != "61000" {
		t.Fatal("被拦下的价格不能写进去")
	}

	clock.Add(2999)
	resp = e.post(t, "/index-price", map[string]any{"symbol": testSymbol, "price": "90000"})
	requireErr(t, resp, 400, ErrIndexPriceJump)
	if msg, _ := resp["message"].(string); !strings.Contains(msg, "3.0秒") && !strings.Contains(msg, "2.9秒") {
		t.Fatalf("已持续约3秒: %q", msg)
	}

	clock.Add(1) // 持续满3秒
	data(t, e.post(t, "/index-price", map[string]any{"symbol": testSymbol, "price": "90000"}))
	if index() != "90000" {
		t.Fatalf("持续3秒之后应该承认: %s", index())
	}
}

// 参数不合法仍然是原来的invalid_param，跟跳变保护无关
func TestSetIndexPrice_InvalidParamsUnchanged(t *testing.T) {
	e := newAPIEnv(t)
	cfg := service.DefaultMarkPriceConfig()
	cfg.IndexMaxJump = decimal.RequireFromString("0.05")
	e.srv.markPrice.WithConfig(cfg)

	requireErr(t, e.post(t, "/index-price", map[string]any{"symbol": testSymbol, "price": "0"}), 400, ErrInvalidParam)
	requireErr(t, e.post(t, "/index-price", map[string]any{"symbol": testSymbol, "price": "-1"}), 400, ErrInvalidParam)
	requireErr(t, e.post(t, "/index-price", map[string]any{"price": "60000"}), 400, ErrInvalidParam)
}

// 默认不开保护：一步大跳变照样写(模拟客户端的行情情景、手动喂价框依赖这个)
func TestSetIndexPrice_NoGuardByDefault(t *testing.T) {
	e := newAPIEnv(t)
	data(t, e.post(t, "/index-price", map[string]any{"symbol": testSymbol, "price": "60000"}))
	data(t, e.post(t, "/index-price", map[string]any{"symbol": testSymbol, "price": "120000"}))
	if p, _ := e.srv.markPrice.GetIndexPrice(context.Background(), testSymbol); !p.Equal(decimal.NewFromInt(120000)) {
		t.Fatalf("指数价 = %s", p)
	}
}
