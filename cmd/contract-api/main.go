// contract-api：对外HTTP服务(Gin)，负责校验参数+冻结保证金+落库+把下单/撤单事件发到Kafka
// 给contract-engine处理，查询类接口直接读MySQL——见plan文件"项目结构"一节。
package main

import (
	"log"

	"perp-go/internal/api"
	"perp-go/internal/cache"
	"perp-go/internal/config"
	"perp-go/internal/db"
	"perp-go/internal/mq"
	"perp-go/internal/repo"
	"perp-go/internal/service"
)

func main() {
	cfg := config.Load()

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
	positionRepo := repo.NewPositionRepo(dbConn)
	tradeRepo := repo.NewTradeRepo(dbConn)
	txRepo := repo.NewTxRepo(dbConn)
	fundingRepo := repo.NewFundingRepo(dbConn)

	markPriceSvc := service.NewMarkPriceService(rdb)
	positionSvc := service.NewPositionService(positionRepo, coinRepo, markPriceSvc)
	accountSvc := service.NewAccountService(accountRepo, positionSvc, txRepo)
	fundingSvc := service.NewFundingService(rdb, coinRepo, positionRepo, fundingRepo, accountSvc, txRepo, markPriceSvc)

	srv := api.NewServer(accountSvc, positionSvc, coinRepo, orderRepo, tradeRepo, markPriceSvc, fundingSvc, producer)
	log.Printf("contract-api listening on %s", cfg.APIAddr)
	if err := srv.Router().Run(cfg.APIAddr); err != nil {
		log.Fatalf("http server error: %v", err)
	}
}
