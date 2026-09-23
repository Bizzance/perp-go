package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"perp-go/internal/binancefeed"
	"perp-go/internal/booksync"
)

func main() {
	apiURL := envOr("BOOKSYNC_API_URL", "http://127.0.0.1:7001")
	keyID := os.Getenv("BOOKSYNC_API_KEY_ID")
	secret := os.Getenv("BOOKSYNC_API_SECRET")
	if keyID == "" || secret == "" {
		log.Fatal("必须设置BOOKSYNC_API_KEY_ID和BOOKSYNC_API_SECRET(只需要ops权限：指数价、K线同步都是运营接口)")
	}
	symbols := splitList(envOr("BOOKSYNC_SYMBOLS", "BTCUSDT,ETHUSDT"))
	cfg := booksync.Config{
		Symbols:  symbols,
		Interval: time.Duration(envInt("BOOKSYNC_INTERVAL_MS", 1000)) * time.Millisecond,
		// 币安K线：每2秒同步一次最近几根(只推变了的)，启动时补500根历史(币安接口上限1500)。
		// BOOKSYNC_KLINE_INTERVAL_SEC=0表示不同步K线(contract-api没设PERP_KLINE_SOURCE=external时用，否则每次同步都会被拒绝)
		KlineEvery:    time.Duration(envIntOrZero("BOOKSYNC_KLINE_INTERVAL_SEC", 2)) * time.Second,
		KlineBackfill: int(envInt("BOOKSYNC_KLINE_BACKFILL", 500)),
	}
	bn := &binancefeed.Binance{
		BaseURL: strings.TrimRight(envOr("BOOKSYNC_BINANCE_URL", "https://fapi.binance.com"), "/"),
		Client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
	s, err := booksync.New(cfg, apiURL, keyID, secret, bn)
	if err != nil {
		log.Fatalf("配置不合法: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Printf("orderbook-sync启动: api=%s symbols=%v 指数价间隔%s K线每%s同步一次(启动补%d根)",
		apiURL, symbols, cfg.Interval, cfg.KlineEvery, cfg.KlineBackfill)
	s.Run(ctx)
	log.Println("orderbook-sync退出")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// 读正整数环境变量，没设用默认值，设了但不合法直接退出——带着悄悄退回默认值的参数跑起来比起不来更糟
func envInt(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		log.Fatalf("%s不合法(需要正整数): %q", key, v)
	}
	return n
}

// 跟envInt一样，但0也是合法的值(表示关闭这个功能)
func envIntOrZero(key string, def int64) int64 {
	if os.Getenv(key) == "0" {
		return 0
	}
	return envInt(key, def)
}
