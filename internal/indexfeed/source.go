// Package indexfeed 生产环境的指数价喂价：从多家交易所的公开行情取各自的指数价，取中位数，
// 校验之后通过POST /index-price推给contract-api。标记价靠这个价格做锚，见docs/mark-price.md。
package indexfeed

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/shopspring/decimal"
)

// 一个行情来源，返回它对某个合约算出来的指数价
type Source interface {
	Name() string
	// symbol是我们自己的合约名(BTCUSDT)，各来源自己转成它的格式
	Fetch(ctx context.Context, symbol string) (decimal.Decimal, error)
}

const maxResponseBytes = 1 << 20

// 请求一个公开接口，返回响应体。非200、响应过大都算失败
func httpGet(ctx context.Context, client *http.Client, rawURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxResponseBytes {
		return nil, fmt.Errorf("响应超过%d字节", maxResponseBytes)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(body), 200))
	}
	return body, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// 解析各家返回的价格字符串，必须是正数
func parsePrice(s string) (decimal.Decimal, error) {
	d, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Zero, fmt.Errorf("价格%q不是数字", s)
	}
	if d.Sign() <= 0 {
		return decimal.Zero, fmt.Errorf("价格%s不是正数", s)
	}
	return d, nil
}

// ---- 币安 ----

// 币安USDⓈ-M合约的指数价(币安自己按多家现货交易所加权算出来的)
type Binance struct {
	BaseURL string // 默认https://fapi.binance.com
	Client  *http.Client
}

func (b *Binance) Name() string { return "binance" }

func (b *Binance) Fetch(ctx context.Context, symbol string) (decimal.Decimal, error) {
	base := b.BaseURL
	if base == "" {
		base = "https://fapi.binance.com"
	}
	body, err := httpGet(ctx, b.Client, base+"/fapi/v1/premiumIndex?symbol="+url.QueryEscape(symbol))
	if err != nil {
		return decimal.Zero, err
	}
	var r struct {
		IndexPrice string `json:"indexPrice"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return decimal.Zero, fmt.Errorf("解析响应: %w", err)
	}
	return parsePrice(r.IndexPrice)
}

// ---- OKX ----

// OKX的指数价，合约名BTCUSDT对应它的instId BTC-USDT
type OKX struct {
	BaseURL string // 默认https://www.okx.com
	Client  *http.Client
}

func (o *OKX) Name() string { return "okx" }

func (o *OKX) Fetch(ctx context.Context, symbol string) (decimal.Decimal, error) {
	instID, err := okxInstID(symbol)
	if err != nil {
		return decimal.Zero, err
	}
	base := o.BaseURL
	if base == "" {
		base = "https://www.okx.com"
	}
	body, err := httpGet(ctx, o.Client, base+"/api/v5/market/index-tickers?instId="+url.QueryEscape(instID))
	if err != nil {
		return decimal.Zero, err
	}
	var r struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			IdxPx string `json:"idxPx"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return decimal.Zero, fmt.Errorf("解析响应: %w", err)
	}
	if r.Code != "0" {
		return decimal.Zero, fmt.Errorf("OKX返回错误 code=%s msg=%s", r.Code, r.Msg)
	}
	if len(r.Data) == 0 {
		return decimal.Zero, fmt.Errorf("OKX没有返回%s的指数价", instID)
	}
	return parsePrice(r.Data[0].IdxPx)
}

// BTCUSDT -> BTC-USDT。只支持USDT本位的合约名
func okxInstID(symbol string) (string, error) {
	base, ok := strings.CutSuffix(symbol, "USDT")
	if !ok || base == "" {
		return "", fmt.Errorf("合约名%q不是XXXUSDT的形式，没法转成OKX的instId", symbol)
	}
	return base + "-USDT", nil
}

// ---- Bybit ----

// Bybit USDT永续的指数价
type Bybit struct {
	BaseURL string // 默认https://api.bybit.com
	Client  *http.Client
}

func (b *Bybit) Name() string { return "bybit" }

func (b *Bybit) Fetch(ctx context.Context, symbol string) (decimal.Decimal, error) {
	base := b.BaseURL
	if base == "" {
		base = "https://api.bybit.com"
	}
	body, err := httpGet(ctx, b.Client, base+"/v5/market/tickers?category=linear&symbol="+url.QueryEscape(symbol))
	if err != nil {
		return decimal.Zero, err
	}
	var r struct {
		RetCode int    `json:"retCode"`
		RetMsg  string `json:"retMsg"`
		Result  struct {
			List []struct {
				IndexPrice string `json:"indexPrice"`
			} `json:"list"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return decimal.Zero, fmt.Errorf("解析响应: %w", err)
	}
	if r.RetCode != 0 {
		return decimal.Zero, fmt.Errorf("Bybit返回错误 retCode=%d msg=%s", r.RetCode, r.RetMsg)
	}
	if len(r.Result.List) == 0 {
		return decimal.Zero, fmt.Errorf("Bybit没有返回%s的行情", symbol)
	}
	return parsePrice(r.Result.List[0].IndexPrice)
}
