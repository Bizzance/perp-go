// Package booksync 把币安的订单簿同步进我们自己的订单簿：定时拉币安USDⓈ-M合约的深度，用两个系统账户
// 在我们的订单簿里挂出一模一样的价格和数量，用户下单吃的就是这些挂单，对手方就是系统。
// 不做市商、不对冲、不报自己的价，我们的价格就是币安的价格，见docs/orderbook-sync.md。
package booksync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/shopspring/decimal"
)

// 订单簿的一档
type Level struct {
	Price decimal.Decimal
	Qty   decimal.Decimal
}

// 币安USDⓈ-M合约的公共行情，不需要密钥。用REST轮询而不是WebSocket：没有断线重连和增量深度对账的复杂度，
// 每秒一次的延迟对"同步盘口"够用
type Binance struct {
	BaseURL string // 形如https://fapi.binance.com，不带结尾的斜杠
	Client  *http.Client
}

// 币安深度接口允许的档位数，取值不在里面会被拒绝
var depthLimits = []int{5, 10, 20, 50, 100, 500, 1000}

// 不小于n的最小的合法档位数，n超过1000返回false
func depthLimitFor(n int) (int, bool) {
	for _, l := range depthLimits {
		if l >= n {
			return l, true
		}
	}
	return 0, false
}

// 拉这个合约的深度，两边都是最优价在前。返回错误的情况：请求失败(429/418是被限流，451是所在地区被币安屏蔽，
// 状态码会带在错误里)、任何一侧是空的、买一不低于卖一(交叉的盘口不可信)
func (b *Binance) Depth(ctx context.Context, symbol string, limit int) (bids, asks []Level, err error) {
	body, err := b.get(ctx, fmt.Sprintf("/fapi/v1/depth?symbol=%s&limit=%d", symbol, limit))
	if err != nil {
		return nil, nil, err
	}
	var depth struct {
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
	}
	if err := json.Unmarshal(body, &depth); err != nil {
		return nil, nil, fmt.Errorf("币安响应不是JSON: %w", err)
	}
	if bids, err = parseLevels(depth.Bids); err != nil {
		return nil, nil, err
	}
	if asks, err = parseLevels(depth.Asks); err != nil {
		return nil, nil, err
	}
	if len(bids) == 0 || len(asks) == 0 {
		return nil, nil, errors.New("币安返回的盘口有一侧是空的")
	}
	if !bids[0].Price.LessThan(asks[0].Price) {
		return nil, nil, fmt.Errorf("币安返回的盘口是交叉的: 买一%s 卖一%s", bids[0].Price, asks[0].Price)
	}
	return bids, asks, nil
}

// 币安算好的指数价(它自己按多家现货交易所加权得出，不是合约的最新成交价)。我们的标记价拿它当锚，
// 见docs/mark-price.md
func (b *Binance) IndexPrice(ctx context.Context, symbol string) (decimal.Decimal, error) {
	body, err := b.get(ctx, "/fapi/v1/premiumIndex?symbol="+symbol)
	if err != nil {
		return decimal.Zero, err
	}
	var prem struct {
		IndexPrice string `json:"indexPrice"`
	}
	if err := json.Unmarshal(body, &prem); err != nil {
		return decimal.Zero, fmt.Errorf("币安响应不是JSON: %w", err)
	}
	p, err := decimal.NewFromString(prem.IndexPrice)
	if err != nil || p.Sign() <= 0 {
		return decimal.Zero, fmt.Errorf("币安返回的指数价不合法: %q", prem.IndexPrice)
	}
	return p, nil
}

func (b *Binance) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := b.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("币安返回%d: %.200s", resp.StatusCode, body)
	}
	return body, nil
}

// 数量为0的档位丢掉；价格或数量解析不出来整体报错，不猜
func parseLevels(raw [][]string) ([]Level, error) {
	out := make([]Level, 0, len(raw))
	for _, r := range raw {
		if len(r) < 2 {
			return nil, fmt.Errorf("币安返回的档位格式不对: %v", r)
		}
		p, err := decimal.NewFromString(r[0])
		if err != nil {
			return nil, fmt.Errorf("币安返回的价格不合法: %q", r[0])
		}
		q, err := decimal.NewFromString(r[1])
		if err != nil {
			return nil, fmt.Errorf("币安返回的数量不合法: %q", r[1])
		}
		if p.Sign() <= 0 {
			return nil, fmt.Errorf("币安返回的价格不是正数: %q", r[0])
		}
		if q.Sign() > 0 {
			out = append(out, Level{Price: p, Qty: q})
		}
	}
	return out, nil
}
