// orderbook-sync：把币安的订单簿和行情同步进我们系统。定时拉币安合约的深度，用两个系统账户在我们的
// 订单簿里挂出一模一样的价格和数量，用户下单吃的就是这些挂单，对手方就是系统，没有做市商；
// 同时把币安的指数价推给contract-api，标记价靠它做锚。
// 币安数据拉不到超过一段时间，撤掉全部挂单、暂停报价，见docs/orderbook-sync.md。
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

	"perp-go/internal/booksync"
)

func main() {
	apiURL := envOr("BOOKSYNC_API_URL", "http://127.0.0.1:7001")
	keyID := os.Getenv("BOOKSYNC_API_KEY_ID")
	secret := os.Getenv("BOOKSYNC_API_SECRET")
	if keyID == "" || secret == "" {
		log.Fatal("必须设置BOOKSYNC_API_KEY_ID和BOOKSYNC_API_SECRET(需要trade和ops两种权限的API密钥)")
	}
	symbols := splitList(envOr("BOOKSYNC_SYMBOLS", "BTCUSDT,ETHUSDT"))
	cfg := booksync.Config{
		Symbols:    symbols,
		BaseUID:    uint64(envInt("BOOKSYNC_UID_BASE", 9000000)),
		Levels:     int(envInt("BOOKSYNC_LEVELS", 50)),
		Interval:   time.Duration(envInt("BOOKSYNC_INTERVAL_MS", 1000)) * time.Millisecond,
		Leverage:   int(envInt("BOOKSYNC_LEVERAGE", 5)),
		Balance:    envOr("BOOKSYNC_BALANCE", "1000000000"),
		StaleAfter: time.Duration(envInt("BOOKSYNC_STALE_SEC", 10)) * time.Second,
	}
	bn := &booksync.Binance{BaseURL: strings.TrimRight(envOr("BOOKSYNC_BINANCE_URL", "https://fapi.binance.com"), "/"), Client: &http.Client{Timeout: 5 * time.Second}}
	s, err := booksync.New(cfg, apiURL, keyID, secret, bn)
	if err != nil {
		log.Fatalf("配置不合法: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Printf("orderbook-sync启动: api=%s symbols=%v 每侧%d档 间隔%s 币安数据%s没更新就撤单 系统账户uid=%d/%d",
		apiURL, symbols, cfg.Levels, cfg.Interval, cfg.StaleAfter, cfg.BaseUID, cfg.BaseUID+1)
	s.Run(ctx)
	log.Println("orderbook-sync退出，系统挂单已撤")
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
