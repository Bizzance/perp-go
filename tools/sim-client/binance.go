package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/shopspring/decimal"
)

// 币安USDⓈ-M合约的公共行情(不需要密钥)：订单簿深度和标记价/指数价。用REST轮询而不是WebSocket：
// 做市每两秒刷一次就够用，REST没有断线重连、增量深度对账这些复杂度。

type binanceBook struct {
	bids  []level // 最优价在前
	asks  []level
	mark  decimal.Decimal
	index decimal.Decimal
	at    time.Time
}

func (b binanceBook) bestBid() decimal.Decimal {
	if len(b.bids) == 0 {
		return decimal.Zero
	}
	return b.bids[0].price
}

func (b binanceBook) bestAsk() decimal.Decimal {
	if len(b.asks) == 0 {
		return decimal.Zero
	}
	return b.asks[0].price
}

func (b binanceBook) mid() decimal.Decimal {
	if len(b.bids) == 0 || len(b.asks) == 0 {
		return decimal.Zero
	}
	return b.bestBid().Add(b.bestAsk()).Div(decimal.NewFromInt(2))
}

type binanceClient struct {
	base string
	http *http.Client
}

func newBinanceClient(base string) *binanceClient {
	return &binanceClient{base: base, http: &http.Client{Timeout: 8 * time.Second}}
}

func (c *binanceClient) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != http.StatusOK {
		// 429/418是被限流，451是所在地区被币安屏蔽——把状态码带出去，页面上能直接看到原因
		return fmt.Errorf("币安返回%d: %.200s", resp.StatusCode, body)
	}
	return json.Unmarshal(body, out)
}

func parseLevels(raw [][]string) ([]level, error) {
	out := make([]level, 0, len(raw))
	for _, r := range raw {
		if len(r) < 2 {
			continue
		}
		p, err := decimal.NewFromString(r[0])
		if err != nil {
			return nil, err
		}
		q, err := decimal.NewFromString(r[1])
		if err != nil {
			return nil, err
		}
		if q.Sign() > 0 {
			out = append(out, level{price: p, qty: q})
		}
	}
	return out, nil
}

// 拉一次这个合约的盘口和标记价/指数价。limit是深度档位数(币安允许5/10/20/50/100/500/1000)：
// 聚合成价格桶后要有足够的桶，原始档位太少的话只覆盖很小的价格范围
func (c *binanceClient) fetch(ctx context.Context, symbol string, limit int) (binanceBook, error) {
	var depth struct {
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
	}
	if err := c.getJSON(ctx, fmt.Sprintf("/fapi/v1/depth?symbol=%s&limit=%d", symbol, limit), &depth); err != nil {
		return binanceBook{}, fmt.Errorf("拉深度: %w", err)
	}
	var prem struct {
		Mark  string `json:"markPrice"`
		Index string `json:"indexPrice"`
	}
	if err := c.getJSON(ctx, "/fapi/v1/premiumIndex?symbol="+symbol, &prem); err != nil {
		return binanceBook{}, fmt.Errorf("拉标记价: %w", err)
	}
	bids, err := parseLevels(depth.Bids)
	if err != nil {
		return binanceBook{}, err
	}
	asks, err := parseLevels(depth.Asks)
	if err != nil {
		return binanceBook{}, err
	}
	mark, _ := decimal.NewFromString(prem.Mark)
	index, _ := decimal.NewFromString(prem.Index)
	return binanceBook{bids: bids, asks: asks, mark: mark, index: index, at: time.Now()}, nil
}
