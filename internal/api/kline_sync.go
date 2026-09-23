package api

import (
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"

	"perp-go/internal/model"
)

// 一次最多同步多少根K线。受请求体大小限制(鉴权层的maxAuthBodyBytes是64KB)：一根K线的JSON最多约180字节，
// 200根约36KB，留够余量。补历史要多根的话调用方分批推
const maxKlineSyncBatch = 200

type syncKlineCandle struct {
	OpenTime   int64           `json:"openTime"`
	Open       decimal.Decimal `json:"open"`
	High       decimal.Decimal `json:"high"`
	Low        decimal.Decimal `json:"low"`
	Close      decimal.Decimal `json:"close"`
	Volume     decimal.Decimal `json:"volume"`
	TradeCount uint32          `json:"tradeCount"`
}

type syncKlinesRequest struct {
	Symbol   string              `json:"symbol" binding:"required"`
	Interval model.KlineInterval `json:"interval" binding:"required"`
	Candles  []syncKlineCandle   `json:"candles"`
}

// 外部行情源(orderbook-sync从币安拉)推送这个合约某个周期的一批K线，整根覆盖已有的同一根(重复推送结果不变)。
// 只在K线来源是外部行情时可用(PERP_KLINE_SOURCE=external)：否则我们自己的成交也在写K线，两边的数据会混在一起。
// 一批里任何一根不合法就整批拒绝，不写一部分：坏数据进了K线，合作方看到的图就是错的
func (s *Server) syncKlines(c *gin.Context) {
	var req syncKlinesRequest
	if !bindJSON(c, &req) {
		return
	}
	if !s.klineExternal {
		failC(c, 400, ErrKlineSourceNotExternal, "kline source is not external, this endpoint is unavailable (set PERP_KLINE_SOURCE=external)")
		return
	}
	ctx := c.Request.Context()
	coin, err := s.coins.FindBySymbol(ctx, req.Symbol)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	if coin == nil || !coin.Enable {
		failC(c, 400, ErrSymbolNotFound, "contract does not exist or is disabled")
		return
	}
	if !validKlineIntervals[req.Interval] {
		fail(c, 400, "interval is invalid")
		return
	}
	if len(req.Candles) == 0 || len(req.Candles) > maxKlineSyncBatch {
		fail(c, 400, fmt.Sprintf("candles must contain between 1 and %d items", maxKlineSyncBatch))
		return
	}
	candles, msg := validateKlineCandles(req.Symbol, req.Interval, req.Candles, time.Now().UnixMilli())
	if msg != "" {
		fail(c, 400, msg)
		return
	}

	openTimes := make([]int64, len(candles))
	for i, k := range candles {
		openTimes[i] = k.OpenTime
	}
	before, err := s.klines.FindByOpenTimes(ctx, req.Symbol, req.Interval, openTimes)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	old := make(map[int64]model.Kline, len(before))
	for _, k := range before {
		old[k.OpenTime] = k
	}

	now := time.Now().UnixMilli()
	if err := s.klines.ReplaceBatch(ctx, req.Symbol, req.Interval, candles, now); err != nil {
		fail(c, 500, err.Error())
		return
	}
	// 只推变了的：每几秒同步一次最近几根，绝大部分没变，全推的话订阅者每次都收到一堆重复数据
	changed := 0
	for _, k := range candles {
		if o, exists := old[k.OpenTime]; exists && sameCandle(o, k) {
			continue
		}
		changed++
		if s.klinePub != nil {
			k.UpdateTime = now
			s.klinePub.PublishKline(ctx, req.Symbol, k)
		}
	}
	ok(c, gin.H{"written": len(candles), "changed": changed})
}

func sameCandle(a, b model.Kline) bool {
	return a.Open.Equal(b.Open) && a.High.Equal(b.High) && a.Low.Equal(b.Low) &&
		a.Close.Equal(b.Close) && a.Volume.Equal(b.Volume) && a.TradeCount == b.TradeCount
}

// 逐根校验并转成model.Kline，第一个不合法的返回说明。nowMs是当前时间(毫秒)，用来拒绝远在未来的K线
func validateKlineCandles(symbol string, interval model.KlineInterval, in []syncKlineCandle, nowMs int64) ([]model.Kline, string) {
	step := model.KlineIntervalMillis[interval]
	out := make([]model.Kline, 0, len(in))
	var prev int64 = -1
	for i, k := range in {
		bad := func(why string) ([]model.Kline, string) { return nil, fmt.Sprintf("candles[%d]: %s", i, why) }
		switch {
		case k.OpenTime <= 0 || k.OpenTime%step != 0:
			return bad("openTime must be an aligned open time (ms) for this interval")
		case k.OpenTime <= prev:
			return bad("openTime must be strictly increasing")
		case k.OpenTime > nowMs+step:
			return bad("openTime is in the future")
		case k.Open.Sign() <= 0 || k.High.Sign() <= 0 || k.Low.Sign() <= 0 || k.Close.Sign() <= 0:
			return bad("open/high/low/close must be greater than 0")
		case k.High.LessThan(decimal.Max(k.Open, k.Close, k.Low)):
			return bad("high is lower than open, close, or low")
		case k.Low.GreaterThan(decimal.Min(k.Open, k.Close)):
			return bad("low is higher than open or close")
		case k.Volume.Sign() < 0:
			return bad("volume cannot be negative")
		}
		prev = k.OpenTime
		out = append(out, model.Kline{Symbol: symbol, Interval: interval, OpenTime: k.OpenTime,
			Open: k.Open, High: k.High, Low: k.Low, Close: k.Close, Volume: k.Volume, TradeCount: k.TradeCount})
	}
	return out, ""
}
