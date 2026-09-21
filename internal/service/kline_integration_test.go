//go:build integration

package service_test

import (
	"context"
	"sync"
	"testing"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
	"perp-go/internal/repo"
	"perp-go/internal/service"
	"perp-go/internal/testutil"
)

// K线开盘时间=成交时间按周期向下取整。用一个对齐到UTC零点的基准时间，手算各周期的开盘时间

const (
	minuteMs = int64(60_000)
	hourMs   = 60 * minuteMs
	dayMs    = 24 * hourMs
	klineDay = int64(20000) * dayMs // UTC零点
	// 零点后3小时07分30秒：1m开盘=+3:07，5m=+3:05，15m=+3:00，1h=+3:00，4h=+0:00，1d=+0:00
	klineT0 = klineDay + 3*hourMs + 7*minuteMs + 30_000
)

func newKlineSvc(t *testing.T) (*service.KlineService, *repo.KlineRepo) {
	t.Helper()
	r := repo.NewKlineRepo(testutil.NewDB(t))
	return service.NewKlineService(r), r
}

func klineByInterval(t *testing.T, ks []model.Kline) map[model.KlineInterval]model.Kline {
	t.Helper()
	m := make(map[model.KlineInterval]model.Kline, len(ks))
	for _, k := range ks {
		m[k.Interval] = k
	}
	return m
}

func mustKline(t *testing.T, k model.Kline, open, high, low, closeP, volume string, count uint32) {
	t.Helper()
	ok := k.Open.Equal(decimal.RequireFromString(open)) && k.High.Equal(decimal.RequireFromString(high)) &&
		k.Low.Equal(decimal.RequireFromString(low)) && k.Close.Equal(decimal.RequireFromString(closeP)) &&
		k.Volume.Equal(decimal.RequireFromString(volume)) && k.TradeCount == count
	if !ok {
		t.Fatalf("%s K线不对: got O=%s H=%s L=%s C=%s V=%s N=%d, want O=%s H=%s L=%s C=%s V=%s N=%d",
			k.Interval, k.Open, k.High, k.Low, k.Close, k.Volume, k.TradeCount, open, high, low, closeP, volume, count)
	}
}

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

// 第一笔成交同时建出六个周期的K线，各自的开盘时间按周期对齐，开高低收都是这笔的价格
func TestKline_FirstTradeCreatesAllSixIntervals(t *testing.T) {
	ks, _ := newKlineSvc(t)

	rows := ks.RecordTrade(context.Background(), testSymbol, dec("65000"), dec("0.1"), klineT0)

	if len(rows) != 6 {
		t.Fatalf("应该返回6根K线(给WS推送用), got %d", len(rows))
	}
	got := klineByInterval(t, rows)
	wantOpen := map[model.KlineInterval]int64{
		model.Kline1m:  klineDay + 3*hourMs + 7*minuteMs,
		model.Kline5m:  klineDay + 3*hourMs + 5*minuteMs,
		model.Kline15m: klineDay + 3*hourMs,
		model.Kline1h:  klineDay + 3*hourMs,
		model.Kline4h:  klineDay,
		model.Kline1d:  klineDay,
	}
	for interval, want := range wantOpen {
		k, ok := got[interval]
		if !ok {
			t.Fatalf("缺少%s周期的K线", interval)
		}
		if k.OpenTime != want {
			t.Fatalf("%s开盘时间应该是%d, got %d", interval, want, k.OpenTime)
		}
		mustKline(t, k, "65000", "65000", "65000", "65000", "0.1", 1)
	}
}

// 同一根K线里的多笔成交：开盘价取第一笔、最高最低取极值、收盘取最后一笔、成交量和笔数累加
func TestKline_SameBucketAggregatesOHLCV(t *testing.T) {
	ks, _ := newKlineSvc(t)
	ctx := context.Background()
	trades := []struct{ price, vol string }{{"65000", "0.1"}, {"65100", "0.2"}, {"64900", "0.05"}, {"65050", "0.15"}}
	var rows []model.Kline
	for i, tr := range trades {
		rows = ks.RecordTrade(ctx, testSymbol, dec(tr.price), dec(tr.vol), klineT0+int64(i)*1000)
	}

	// 四笔都在同一分钟内，所以六个周期各自只有一根K线，内容一样
	for interval, k := range klineByInterval(t, rows) {
		_ = interval
		mustKline(t, k, "65000", "65100", "64900", "65050", "0.5", 4)
	}
}

// 下一分钟的成交：1分钟K线另起一根(开盘价是这笔的价格)，但5分钟及更大周期仍然落在同一根里继续累加
func TestKline_NextMinuteStartsNew1mBucketButSameLargerBuckets(t *testing.T) {
	ks, r := newKlineSvc(t)
	ctx := context.Background()
	ks.RecordTrade(ctx, testSymbol, dec("65000"), dec("0.1"), klineT0)
	rows := ks.RecordTrade(ctx, testSymbol, dec("65200"), dec("0.2"), klineT0+minuteMs)

	got := klineByInterval(t, rows)
	mustKline(t, got[model.Kline1m], "65200", "65200", "65200", "65200", "0.2", 1)
	if got[model.Kline1m].OpenTime != klineDay+3*hourMs+8*minuteMs {
		t.Fatalf("1m应该是下一分钟的K线, got %d", got[model.Kline1m].OpenTime)
	}
	// 3:07和3:08都在3:05-3:10这根5分钟K线里
	mustKline(t, got[model.Kline5m], "65000", "65200", "65000", "65200", "0.3", 2)
	mustKline(t, got[model.Kline1d], "65000", "65200", "65000", "65200", "0.3", 2)

	recent, err := r.FindRecent(ctx, testSymbol, model.Kline1m, 10)
	if err != nil || len(recent) != 2 {
		t.Fatalf("1m周期应该有2根: %d %v", len(recent), err)
	}
	if recent[0].OpenTime >= recent[1].OpenTime {
		t.Fatal("FindRecent应该按开盘时间升序(从旧到新)")
	}
}

// 成交跨过整点：1小时K线另起一根，4小时和日线还是同一根
func TestKline_CrossingHourBoundary(t *testing.T) {
	ks, _ := newKlineSvc(t)
	ctx := context.Background()
	ks.RecordTrade(ctx, testSymbol, dec("65000"), dec("0.1"), klineDay+3*hourMs+59*minuteMs)
	rows := ks.RecordTrade(ctx, testSymbol, dec("65300"), dec("0.1"), klineDay+4*hourMs+minuteMs)

	got := klineByInterval(t, rows)
	mustKline(t, got[model.Kline1h], "65300", "65300", "65300", "65300", "0.1", 1) // 4:00这根新的
	// 3:59在第一根4小时K线(0:00-4:00)里，4:01在第二根(4:00-8:00)里，所以4h也是新的
	mustKline(t, got[model.Kline4h], "65300", "65300", "65300", "65300", "0.1", 1)
	mustKline(t, got[model.Kline1d], "65000", "65300", "65000", "65300", "0.2", 2)
}

// 并发成交一笔都不能丢：50笔同时写同一根K线，成交量和笔数精确相加，最高最低是真实的极值
func TestKline_ConcurrentTradesLoseNothing(t *testing.T) {
	ks, r := newKlineSvc(t)
	ctx := context.Background()
	const n = 50
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ks.RecordTrade(ctx, testSymbol, decimal.NewFromInt(65000+int64(i)), dec("0.01"), klineT0)
		}(i)
	}
	close(start)
	wg.Wait()

	rows, err := r.FindRecent(ctx, testSymbol, model.Kline1m, 1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("应该只有1根1m K线: %d %v", len(rows), err)
	}
	k := rows[0]
	if !k.Volume.Equal(dec("0.5")) || k.TradeCount != n {
		t.Fatalf("50笔0.01应该累加成成交量0.5、笔数50, got V=%s N=%d", k.Volume, k.TradeCount)
	}
	if !k.High.Equal(dec("65049")) || !k.Low.Equal(dec("65000")) {
		t.Fatalf("最高最低应该是真实极值65049/65000, got H=%s L=%s", k.High, k.Low)
	}
}

// 不同symbol的K线互相隔离
func TestKline_SymbolsAreIsolated(t *testing.T) {
	ks, r := newKlineSvc(t)
	ctx := context.Background()
	ks.RecordTrade(ctx, "BTCUSDT", dec("65000"), dec("0.1"), klineT0)
	ks.RecordTrade(ctx, "ETHUSDT", dec("3000"), dec("1"), klineT0)

	btc, _ := r.FindRecent(ctx, "BTCUSDT", model.Kline1m, 10)
	eth, _ := r.FindRecent(ctx, "ETHUSDT", model.Kline1m, 10)
	if len(btc) != 1 || len(eth) != 1 {
		t.Fatalf("各自1根, got btc=%d eth=%d", len(btc), len(eth))
	}
	mustKline(t, btc[0], "65000", "65000", "65000", "65000", "0.1", 1)
	mustKline(t, eth[0], "3000", "3000", "3000", "3000", "1", 1)
}

// FindRecent只返回最近limit根，按时间升序
func TestKline_FindRecentReturnsLatestAscending(t *testing.T) {
	ks, r := newKlineSvc(t)
	ctx := context.Background()
	for i := int64(0); i < 5; i++ {
		ks.RecordTrade(ctx, testSymbol, decimal.NewFromInt(65000+i), dec("0.1"), klineT0+i*minuteMs)
	}

	rows, err := r.FindRecent(ctx, testSymbol, model.Kline1m, 3)
	if err != nil || len(rows) != 3 {
		t.Fatalf("应该返回3根: %d %v", len(rows), err)
	}
	for i, wantClose := range []string{"65002", "65003", "65004"} {
		if !rows[i].Close.Equal(dec(wantClose)) {
			t.Fatalf("第%d根应该是最近的第%d根(收盘%s), got %s", i, i+3, wantClose, rows[i].Close)
		}
	}
}

// 端到端：一笔真实成交经引擎结算之后，六个周期的K线都被更新
func TestKline_FillUpdatesKlinesThroughEngine(t *testing.T) {
	e := newEngineEnv(t)
	a := e.newAccount(t, 1, "10000")
	b := e.newAccount(t, 2, "10000")

	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")

	for _, interval := range model.AllKlineIntervals {
		var k model.Kline
		if err := e.db.Get(&k, "SELECT * FROM klines WHERE symbol = ? AND `interval` = ?", testSymbol, interval); err != nil {
			t.Fatalf("%s周期应该有K线: %v", interval, err)
		}
		mustKline(t, k, "65000", "65000", "65000", "65000", "0.1", 1)
	}
}

// K线来源是外部行情时，我们自己的成交不再更新K线：RecordTrade什么都不写、返回nil(没有可推送的数据)，
// 不然同一根K线会被我们的成交价和外部数据混在一起
func TestKline_ExternalSourceIgnoresOwnTrades(t *testing.T) {
	ks, r := newKlineSvc(t)
	ks.WithExternalSource(true)
	if rows := ks.RecordTrade(context.Background(), "BTCUSDT", decimal.RequireFromString("100"), decimal.RequireFromString("1"), klineT0); rows != nil {
		t.Fatalf("外部来源时RecordTrade应该返回nil, got %v", rows)
	}
	for _, iv := range model.AllKlineIntervals {
		got, err := r.FindRecent(context.Background(), "BTCUSDT", iv, 10)
		if err != nil || len(got) != 0 {
			t.Fatalf("%s: 外部来源时不该有我们自己成交生成的K线: %v %v", iv, got, err)
		}
	}
	// 切回trades又能写(默认行为不变)
	ks.WithExternalSource(false)
	if rows := ks.RecordTrade(context.Background(), "BTCUSDT", decimal.RequireFromString("100"), decimal.RequireFromString("1"), klineT0); len(rows) != len(model.AllKlineIntervals) {
		t.Fatalf("trades来源应该照常写六个周期, got %d", len(rows))
	}
}
