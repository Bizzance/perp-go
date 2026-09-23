// Package binancefeed 是拉取币安USDⓈ-M合约公开行情的客户端(深度、指数价、K线)，以及"把一份目标
// 深度跟已有挂单做差异"的纯函数(plan.go)。不碰账户、不碰HTTP鉴权，两个使用方各自决定挂单的
// 方式：contract-engine内部直接调用撮合/账户服务镜像挂单(internal/service的mirror组件)，
// orderbook-sync进程调用contract-api的公开接口推送指数价和K线(internal/booksync)。
package binancefeed

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
var DepthLimits = []int{5, 10, 20, 50, 100, 500, 1000}

// 不小于n的最小的合法档位数，n超过1000返回false
func DepthLimitFor(n int) (int, bool) {
	for _, l := range DepthLimits {
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

// 币安的一根K线(开高低收、成交量、成交笔数都是币安全市场的，不是我们平台的)
type Candle struct {
	OpenTime int64 // 毫秒
	Open     decimal.Decimal
	High     decimal.Decimal
	Low      decimal.Decimal
	Close    decimal.Decimal
	Volume   decimal.Decimal
	Trades   int64
}

// 币安K线接口一次最多返回的根数
const MaxKlineLimit = 1500

// 拉这个合约某个周期最近limit根K线，按开盘时间升序，最后一根是还没走完的当前这一根。
// interval是币安的周期写法，跟我们的一样(1m/5m/15m/1h/4h/1d)
func (b *Binance) Klines(ctx context.Context, symbol, interval string, limit int) ([]Candle, error) {
	body, err := b.get(ctx, fmt.Sprintf("/fapi/v1/klines?symbol=%s&interval=%s&limit=%d", symbol, interval, limit))
	if err != nil {
		return nil, err
	}
	var rows [][]json.RawMessage
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("币安K线响应不是JSON数组: %w", err)
	}
	out := make([]Candle, 0, len(rows))
	for _, r := range rows {
		c, err := parseCandle(r)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

// 币安的一行K线是数组：[开盘时间, 开, 高, 低, 收, 成交量, 收盘时间, 成交额, 成交笔数, ...]，价格和数量是字符串
func parseCandle(r []json.RawMessage) (Candle, error) {
	if len(r) < 9 {
		return Candle{}, fmt.Errorf("币安返回的K线格式不对(只有%d列)", len(r))
	}
	var c Candle
	if err := json.Unmarshal(r[0], &c.OpenTime); err != nil || c.OpenTime <= 0 {
		return Candle{}, fmt.Errorf("币安返回的K线开盘时间不合法: %s", r[0])
	}
	for i, dst := range []*decimal.Decimal{&c.Open, &c.High, &c.Low, &c.Close, &c.Volume} {
		var s string
		if err := json.Unmarshal(r[i+1], &s); err != nil {
			return Candle{}, fmt.Errorf("币安返回的K线数值不是字符串: %s", r[i+1])
		}
		d, err := decimal.NewFromString(s)
		if err != nil {
			return Candle{}, fmt.Errorf("币安返回的K线数值不合法: %q", s)
		}
		*dst = d
	}
	if err := json.Unmarshal(r[8], &c.Trades); err != nil {
		return Candle{}, fmt.Errorf("币安返回的K线成交笔数不合法: %s", r[8])
	}
	return c, nil
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
