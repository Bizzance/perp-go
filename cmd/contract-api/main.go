// contract-api：对外HTTP服务(Gin)，负责校验参数+冻结保证金+落库+把下单/撤单事件发到Kafka
// 给contract-engine处理，查询类接口直接读MySQL——见plan文件"项目结构"一节。
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/api"
	"perp-go/internal/cache"
	"perp-go/internal/config"
	"perp-go/internal/db"
	"perp-go/internal/mq"
	"perp-go/internal/repo"
	"perp-go/internal/service"
	"perp-go/internal/ws"
)

func main() {
	cfg := config.Load(0) // contract-api默认node id=0，跟contract-engine(默认1)区分开
	if err := cfg.ValidateAuth(); err != nil {
		log.Fatal(err)
	}
	service.InitNodeID(cfg.NodeID)

	dbConn, err := db.Connect(cfg.MySQLDSN)
	if err != nil {
		log.Fatalf("connect mysql: %v", err)
	}
	defer dbConn.Close()

	rdb, err := cache.Connect(cfg.RedisAddr, cfg.RedisPass)
	if err != nil {
		log.Fatalf("connect redis: %v", err)
	}

	producer := mq.NewProducer(cfg.KafkaBrokers)
	defer producer.Close()

	accountRepo := repo.NewAccountRepo(dbConn)
	coinRepo := repo.NewCoinRepo(dbConn)
	orderRepo := repo.NewOrderRepo(dbConn)
	conditionalOrderRepo := repo.NewConditionalOrderRepo(dbConn)
	positionRepo := repo.NewPositionRepo(dbConn)
	tradeRepo := repo.NewTradeRepo(dbConn)
	txRepo := repo.NewTxRepo(dbConn)
	fundingRepo := repo.NewFundingRepo(dbConn)
	riskLimitRepo := repo.NewRiskLimitRepo(dbConn)
	klineRepo := repo.NewKlineRepo(dbConn)

	markPriceSvc := service.NewMarkPriceService(rdb).WithConfig(service.MarkPriceConfig{
		MaxIndexAge:  cfg.MarkPriceMaxIndexAge,
		MaxDeviation: decimal.NewFromFloat(cfg.MarkPriceMaxDeviation),
		BasisWindow:  cfg.MarkPriceBasisWindow,
		RequireIndex: cfg.MarkPriceRequireIndex,
		// 服务端跳变保护只有contract-api会用到(POST /index-price在这里)，engine不需要
		IndexMaxJump:     decimal.NewFromFloat(cfg.IndexMaxJump),
		IndexJumpConfirm: cfg.IndexJumpConfirm,
	})
	positionSvc := service.NewPositionService(positionRepo, riskLimitRepo, markPriceSvc)
	accountSvc := service.NewAccountService(accountRepo, positionSvc, txRepo)
	fundingSvc := service.NewFundingService(rdb, coinRepo, positionRepo, fundingRepo, accountSvc, txRepo, markPriceSvc)
	lockSvc := service.NewLockService(rdb)

	hub := ws.NewHub(rdb)

	auth := api.NewAuth(cfg.AuthDisabled, cfg.APIKeys, rdb)

	srv := api.NewServer(accountSvc, positionSvc, coinRepo, orderRepo, conditionalOrderRepo, tradeRepo, klineRepo, markPriceSvc, fundingSvc, producer, hub, lockSvc, txRepo, auth)
	// K线来源是外部行情(PERP_KLINE_SOURCE=external)时，POST /kline/sync写入后要把变了的K线推给WebSocket订阅者，
	// 走的是跟engine同一个Redis频道
	srv.WithKlineSync(cfg.KlineSource == "external", service.NewPushService(rdb, accountSvc, positionSvc, orderRepo))

	// 收到SIGTERM/SIGINT(docker stop、滚动发布都会发)先停止接收新连接、等在途请求处理完再退出，
	// 而不是被直接杀掉——下单请求可能正处在"已冻结保证金、还没落库/发Kafka"这一步。WebSocket
	// 连接是被hijack的，Shutdown不会等它们，进程退出时自然断开，客户端重连即可
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	httpSrv := &http.Server{Addr: cfg.APIAddr, Handler: srv.Router()}
	go func() {
		log.Printf("contract-api listening on %s", cfg.APIAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("contract-api shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[WARN] http server graceful shutdown: %v", err)
	}
}
