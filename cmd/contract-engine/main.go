// contract-engine：撮合+风控引擎进程。消费Kafka里的下单/撤单事件，维护每个symbol的内存
// 订单簿，定时扫描做全仓强平判断——见plan文件"项目结构"一节。
package main

import (
	"context"
	"encoding/json"
	"log"
	"os/signal"
	"syscall"
	"time"

	"perp-go/internal/cache"
	"perp-go/internal/config"
	"perp-go/internal/db"
	"perp-go/internal/events"
	"perp-go/internal/matching"
	"perp-go/internal/mq"
	"perp-go/internal/repo"
	"perp-go/internal/service"
)

func main() {
	cfg := config.Load()

	conn, err := db.Connect(cfg.MySQLDSN)
	if err != nil {
		log.Fatalf("connect mysql: %v", err)
	}
	defer conn.Close()

	rdb, err := cache.Connect(cfg.RedisAddr, cfg.RedisPass)
	if err != nil {
		log.Fatalf("connect redis: %v", err)
	}

	accountRepo := repo.NewAccountRepo(conn)
	coinRepo := repo.NewCoinRepo(conn)
	orderRepo := repo.NewOrderRepo(conn)
	positionRepo := repo.NewPositionRepo(conn)
	tradeRepo := repo.NewTradeRepo(conn)
	txRepo := repo.NewTxRepo(conn)
	fundRepo := repo.NewInsuranceFundRepo(conn)

	markPriceSvc := service.NewMarkPriceService(rdb)
	positionSvc := service.NewPositionService(positionRepo, coinRepo, markPriceSvc)
	accountSvc := service.NewAccountService(accountRepo, positionSvc, txRepo)
	settlementSvc := service.NewSettlementService(accountSvc, positionRepo, coinRepo, txRepo)
	fundSvc := service.NewInsuranceFundService(fundRepo)

	matchingEngine := matching.NewEngine()
	engineSvc := service.NewEngineService(matchingEngine, orderRepo, tradeRepo, accountSvc, positionSvc, settlementSvc, markPriceSvc, fundSvc)
	liquidationSvc := service.NewLiquidationService(engineSvc, orderRepo, positionRepo, positionSvc, coinRepo, markPriceSvc, accountSvc, fundSvc, cfg.LiquidationOrderTimeoutMs)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	submitConsumer := mq.NewConsumer(cfg.KafkaBrokers, events.TopicOrderSubmit, "contract-engine")
	defer submitConsumer.Close()
	go submitConsumer.Consume(ctx, func(_, value []byte) error {
		var evt events.OrderSubmitEvent
		if err := json.Unmarshal(value, &evt); err != nil {
			return err
		}
		o, err := orderRepo.FindByOrderID(ctx, evt.OrderID)
		if err != nil || o == nil {
			log.Printf("[ERROR] order %d not found for submit event", evt.OrderID)
			return err
		}
		return engineSvc.SubmitOrder(ctx, o, time.Now().UnixNano())
	})

	cancelConsumer := mq.NewConsumer(cfg.KafkaBrokers, events.TopicOrderCancel, "contract-engine")
	defer cancelConsumer.Close()
	go cancelConsumer.Consume(ctx, func(_, value []byte) error {
		var evt events.OrderCancelEvent
		if err := json.Unmarshal(value, &evt); err != nil {
			return err
		}
		o, err := orderRepo.FindByOrderID(ctx, evt.OrderID)
		if err != nil || o == nil {
			return err
		}
		return engineSvc.CancelOrder(ctx, o)
	})

	go func() {
		ticker := time.NewTicker(time.Duration(cfg.RiskScanIntervalMs) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				liquidationSvc.RiskScanOnce(ctx)
			}
		}
	}()

	log.Println("contract-engine started")
	<-ctx.Done()
	log.Println("contract-engine shutting down")
}
