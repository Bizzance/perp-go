// contract-engine：撮合+风控引擎进程。消费Kafka里的下单/撤单事件，维护每个symbol的内存
// 订单簿，定时扫描做全仓强平判断，定时采样+结算资金费率——见plan文件"项目结构"一节。
package main

import (
	"context"
	"encoding/json"
	"log"
	"os/signal"
	"syscall"
	"time"

	"perp-go/internal/api"
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
	cfg := config.Load(1) // contract-engine默认node id=1，跟contract-api(默认0)区分开
	service.InitNodeID(cfg.NodeID)

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
	conditionalOrderRepo := repo.NewConditionalOrderRepo(conn)
	positionRepo := repo.NewPositionRepo(conn)
	tradeRepo := repo.NewTradeRepo(conn)
	txRepo := repo.NewTxRepo(conn)
	fundRepo := repo.NewInsuranceFundRepo(conn)
	fundingRepo := repo.NewFundingRepo(conn)
	riskLimitRepo := repo.NewRiskLimitRepo(conn)
	klineRepo := repo.NewKlineRepo(conn)
	processedMsgRepo := repo.NewProcessedMessageRepo(conn)

	markPriceSvc := service.NewMarkPriceService(rdb)
	positionSvc := service.NewPositionService(positionRepo, riskLimitRepo, markPriceSvc)
	accountSvc := service.NewAccountService(accountRepo, positionSvc, txRepo)
	settlementSvc := service.NewSettlementService(accountSvc, positionRepo, coinRepo, txRepo)
	fundSvc := service.NewInsuranceFundService(fundRepo)
	fundingSvc := service.NewFundingService(rdb, coinRepo, positionRepo, fundingRepo, accountSvc, txRepo, markPriceSvc)
	klineSvc := service.NewKlineService(klineRepo)
	pushSvc := service.NewPushService(rdb, accountSvc, positionSvc, orderRepo)

	matchingEngine := matching.NewEngine()
	engineSvc := service.NewEngineService(matchingEngine, orderRepo, conditionalOrderRepo, tradeRepo, accountSvc, positionSvc, settlementSvc, markPriceSvc, fundSvc, klineSvc, pushSvc)
	liquidationSvc := service.NewLiquidationService(engineSvc, orderRepo, positionRepo, positionSvc, markPriceSvc, accountSvc, fundSvc, cfg.LiquidationOrderTimeoutMs)
	conditionalOrderSvc := service.NewConditionalOrderService(conditionalOrderRepo, orderRepo, markPriceSvc, engineSvc)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 订单簿是纯内存结构，重启会丢——启动时先从MySQL里还在排队的委托记录重建，必须在下面
	// 的Kafka消费者开始处理新消息之前跑完，不然新委托可能撮合到一个还没恢复完整的半成品
	// 订单簿上，见docs/order-book-recovery.md。失败直接退出而不是带着一个不完整/空的订单簿
	// 硬起来——那样后续撮合会悄悄产出经济上错误的结果(该撮合到的历史挂单凭空消失)，
	// 比进程起不来更糟
	if err := engineSvc.RecoverOrderBook(ctx); err != nil {
		log.Fatalf("恢复订单簿失败: %v", err)
	}

	submitConsumer := mq.NewConsumer(cfg.KafkaBrokers, events.TopicOrderSubmit, "contract-engine")
	defer submitConsumer.Close()
	go submitConsumer.Consume(ctx, mq.WithDedup(ctx, processedMsgRepo, func(msg mq.Message) error {
		var evt events.OrderSubmitEvent
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			return err
		}
		o, err := orderRepo.FindByOrderID(ctx, evt.OrderID)
		if err != nil || o == nil {
			log.Printf("[ERROR] order %d not found for submit event", evt.OrderID)
			return err
		}
		return engineSvc.SubmitOrder(ctx, o, time.Now().UnixNano())
	}))

	cancelConsumer := mq.NewConsumer(cfg.KafkaBrokers, events.TopicOrderCancel, "contract-engine")
	defer cancelConsumer.Close()
	go cancelConsumer.Consume(ctx, mq.WithDedup(ctx, processedMsgRepo, func(msg mq.Message) error {
		var evt events.OrderCancelEvent
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			return err
		}
		o, err := orderRepo.FindByOrderID(ctx, evt.OrderID)
		if err != nil || o == nil {
			return err
		}
		return engineSvc.CancelOrder(ctx, o)
	}))

	// 用独立的group id，不要跟下面的submit/cancel共用"contract-engine"——同一个group id挂
	// 多个订阅不同topic的member，Kafka的分区分配在这种异构订阅场景下不可靠(实测过：3个
	// member共用一个group id时，broker端分配阶段完成了，但每个member实际收不到任何分区，
	// 消费彻底卡住，连已有的submit/cancel两个topic也一起被拖挂)，每个独立的消费职责必须用
	// 自己独立的group id
	roundCloseConsumer := mq.NewConsumer(cfg.KafkaBrokers, events.TopicRoundClose, "contract-engine-round-close")
	defer roundCloseConsumer.Close()
	go roundCloseConsumer.Consume(ctx, mq.WithDedup(ctx, processedMsgRepo, func(msg mq.Message) error {
		var evt events.RoundCloseEvent
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			return err
		}
		return engineSvc.CloseRound(ctx, evt.UID)
	}))

	go func() {
		ticker := time.NewTicker(time.Duration(cfg.DedupCleanupIntervalMs) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cutoff := time.Now().Add(-time.Duration(cfg.DedupRetentionHours) * time.Hour)
				if n, err := processedMsgRepo.DeleteOlderThan(ctx, cutoff); err != nil {
					log.Printf("[ERROR] 清理消息去重记录失败: %v", err)
				} else if n > 0 {
					log.Printf("清理了%d条过期的消息去重记录(早于%s)", n, cutoff.Format(time.RFC3339))
				}
			}
		}
	}()

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

	go func() {
		ticker := time.NewTicker(time.Duration(cfg.FundingSampleIntervalMs) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fundingSvc.SampleOnce(ctx)
				fundingSvc.SettleIfDue(ctx, time.Now().UnixMilli())
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(time.Duration(cfg.ConditionalScanIntervalMs) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				conditionalOrderSvc.ScanOnce(ctx)
			}
		}
	}()

	engineSrv := api.NewEngineServer(matchingEngine, coinRepo)
	engineSrv.RefreshSymbols(ctx) // 启动时先同步刷一次，不然/depth接口刚起来那段时间缓存是空的、全部请求都会被当成"合约不存在"拒绝
	go func() {
		ticker := time.NewTicker(time.Duration(cfg.SymbolCacheRefreshMs) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				engineSrv.RefreshSymbols(ctx)
			}
		}
	}()
	go func() {
		log.Printf("contract-engine http(深度查询等) listening on %s", cfg.EngineHTTPAddr)
		if err := engineSrv.Router().Run(cfg.EngineHTTPAddr); err != nil {
			log.Fatalf("contract-engine http server error: %v", err)
		}
	}()

	log.Println("contract-engine started")
	<-ctx.Done()
	log.Println("contract-engine shutting down")
}
