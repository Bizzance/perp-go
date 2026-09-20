// index-feeder：生产环境的指数价喂价进程。定时从币安、OKX、Bybit取各自的指数价，取中位数，
// 校验之后调contract-api的POST /index-price推进去。标记价靠它做锚，喂价断了强平会暂停，
// 见docs/mark-price.md和docs/index-feeder.md。
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

	"github.com/shopspring/decimal"

	"perp-go/internal/indexfeed"
)

func main() {
	apiURL := envOr("FEEDER_API_URL", "http://127.0.0.1:7001")
	keyID := os.Getenv("FEEDER_API_KEY_ID")
	secret := os.Getenv("FEEDER_API_SECRET")
	if keyID == "" || secret == "" {
		log.Fatal("必须设置FEEDER_API_KEY_ID和FEEDER_API_SECRET(需要ops权限的API密钥)")
	}
	symbols := splitList(envOr("FEEDER_SYMBOLS", "BTCUSDT,ETHUSDT"))
	if len(symbols) == 0 {
		log.Fatal("FEEDER_SYMBOLS不能为空")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	var sources []indexfeed.Source
	for _, name := range splitList(envOr("FEEDER_SOURCES", "binance,okx,bybit")) {
		switch name {
		case "binance":
			sources = append(sources, &indexfeed.Binance{Client: client})
		case "okx":
			sources = append(sources, &indexfeed.OKX{Client: client})
		case "bybit":
			sources = append(sources, &indexfeed.Bybit{Client: client})
		default:
			log.Fatalf("FEEDER_SOURCES里有不认识的来源%q，支持binance、okx、bybit", name)
		}
	}
	cfg := indexfeed.Config{
		Symbols:     symbols,
		Sources:     sources,
		MinSources:  int(envInt("FEEDER_MIN_SOURCES", 2)),
		Outlier:     decimal.NewFromFloat(envFloat("FEEDER_OUTLIER", 0.01)),
		MaxJump:     decimal.NewFromFloat(envFloat("FEEDER_MAX_JUMP", 0.03)),
		JumpConfirm: int(envInt("FEEDER_JUMP_CONFIRM", 3)),
	}
	if cfg.MinSources > len(sources) {
		log.Fatalf("FEEDER_MIN_SOURCES=%d大于来源数%d，永远凑不够、一个价格也发布不出去", cfg.MinSources, len(sources))
	}
	interval := time.Duration(envInt("FEEDER_INTERVAL_MS", 1000)) * time.Millisecond

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Printf("index-feeder启动: api=%s symbols=%v sources=%d家 最少%d家一致 间隔%s", apiURL, symbols, len(sources), cfg.MinSources, interval)
	f := indexfeed.New(cfg, &indexfeed.APIPublisher{BaseURL: apiURL, KeyID: keyID, Secret: secret, Client: client})
	f.Run(ctx, interval)
	log.Println("index-feeder退出")
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

func envFloat(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		log.Fatalf("%s不合法(需要大于0的数): %q", key, v)
	}
	return f
}
