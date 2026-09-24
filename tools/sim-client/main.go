// 模拟客户端：内部测试用的交易页面，扮演"合作方的后端"接入我们的contract-api。
//
// 浏览器不能直接连我们的后端：鉴权是API Key + HMAC签名，密钥不能放进页面；而且浏览器的WebSocket
// 不能自定义请求头，/ws的握手签名也做不了。所以这里有一个薄的代理服务：页面只连它，它持有密钥、
// 对每个请求签名后转发，WebSocket也由它签名连上去再中继给页面。
//
// 这个代理持有ops权限的密钥(能充值、冻结、设指数价)，只是开发和演示工具，默认只绑127.0.0.1，
// 不进生产镜像。用法见docs/sim-client.md。
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type config struct {
	listen      string
	apiURL      string
	engineURL   string
	tradeKeyID  string // 交易类接口用的trade密钥，留空=所有请求都用keyID这把
	tradeSecret string
	keyID       string
	secret      string
	allowRemote bool
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func loadConfig() config {
	var c config
	flag.StringVar(&c.listen, "listen", envOr("SIM_LISTEN", "127.0.0.1:8088"), "页面和代理的监听地址")
	flag.StringVar(&c.apiURL, "api", envOr("SIM_API_URL", "http://127.0.0.1:7001"), "contract-api的地址")
	flag.StringVar(&c.engineURL, "engine", envOr("SIM_ENGINE_URL", "http://127.0.0.1:7002"), "contract-engine的地址")
	flag.StringVar(&c.keyID, "key-id", envOr("SIM_KEY_ID", ""), "API Key的id，留空=不签名(后端开了PERP_AUTH_DISABLED才行)")
	flag.StringVar(&c.secret, "key-secret", envOr("SIM_KEY_SECRET", ""), "API Key的secret")
	flag.StringVar(&c.tradeKeyID, "trade-key-id", envOr("SIM_TRADE_KEY_ID", ""), "只有trade权限的API Key的id：设了以后交易类请求用它签名、页面标了运营的请求用key-id那把(ops)，跟合作方的用法一致；留空=所有请求都用key-id那把")
	flag.StringVar(&c.tradeSecret, "trade-key-secret", envOr("SIM_TRADE_KEY_SECRET", ""), "trade密钥的secret")
	flag.BoolVar(&c.allowRemote, "allow-remote", false, "允许监听非回环地址(默认拒绝：这个代理持有ops密钥，谁连上来谁就能加钱扣钱)")
	flag.Parse()
	return c
}

// 监听地址必须是回环地址，除非显式允许
func checkListenAddr(addr string, allowRemote bool) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if allowRemote || host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("监听地址" + addr + "不是回环地址：这个代理持有ops密钥，暴露出去等于任何人都能加钱扣钱。确实需要的话加 -allow-remote")
	}
	return nil
}

func main() {
	cfg := loadConfig()
	if err := checkListenAddr(cfg.listen, cfg.allowRemote); err != nil {
		log.Fatal(err)
	}
	if (cfg.keyID == "") != (cfg.secret == "") {
		log.Fatal("SIM_KEY_ID和SIM_KEY_SECRET要么都设、要么都不设")
	}
	if cfg.keyID == "" {
		log.Printf("[WARN] 没有配置API密钥，请求不会签名，后端必须开着PERP_AUTH_DISABLED才能用")
	}
	if (cfg.tradeKeyID == "") != (cfg.tradeSecret == "") {
		log.Fatal("SIM_TRADE_KEY_ID和SIM_TRADE_KEY_SECRET要么都设、要么都不设")
	}
	if cfg.tradeKeyID != "" {
		log.Printf("用两把密钥：交易类请求用trade密钥(%s)，页面标了运营的请求用ops密钥(%s)，接口权限分错了会直接返回forbidden", cfg.tradeKeyID, cfg.keyID)
	} else if cfg.keyID != "" {
		log.Printf("只有一把密钥(%s)，所有请求都用它，测不出接口权限范围分错的问题；想跟合作方一样分开用，配置SIM_TRADE_KEY_ID/SIM_TRADE_KEY_SECRET", cfg.keyID)
	}

	s := newServer(cfg)
	srv := &http.Server{Addr: cfg.listen, Handler: s.routes(), ReadHeaderTimeout: 10 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Printf("模拟客户端已启动: http://%s  (contract-api=%s engine=%s)", cfg.listen, cfg.apiURL, cfg.engineURL)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}
