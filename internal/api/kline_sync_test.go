package api

import (
	"encoding/json"
	"testing"

	"github.com/shopspring/decimal"
)

// 一批最多maxKlineSyncBatch根、每根按最长的写法(8位小数、大成交量、大成交笔数)，序列化后必须远小于鉴权层的请求体上限，
// 否则补历史时请求会被"请求体太大"拒绝(真实系统里一次推500根，15m以上的周期就是这样被拒的)。
// 改maxKlineSyncBatch或maxAuthBodyBytes时这条会提醒两者要配套
func TestKlineSyncBatchFitsInTheAuthBodyLimit(t *testing.T) {
	c := syncKlineCandle{
		OpenTime: 1789961460000,
		Open:     decimal.RequireFromString("81464.20000000"), High: decimal.RequireFromString("81549.00000000"),
		Low: decimal.RequireFromString("81464.20000000"), Close: decimal.RequireFromString("81522.80000000"),
		Volume: decimal.RequireFromString("293583.123456789"), TradeCount: 5227123,
	}
	req := syncKlinesRequest{Symbol: "BTCUSDT", Interval: "1d", Candles: make([]syncKlineCandle, maxKlineSyncBatch)}
	for i := range req.Candles {
		req.Candles[i] = c
	}
	body, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) > maxAuthBodyBytes/2 {
		t.Fatalf("%d根K线序列化后%d字节，超过请求体上限%d的一半，没有余量", maxKlineSyncBatch, len(body), maxAuthBodyBytes)
	}
}
