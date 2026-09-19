// contract-engine：撮合+风控引擎进程。消费Kafka里的下单/撤单事件，维护每个symbol的内存
// 订单簿，定时扫描做全仓强平判断，定时采样+结算资金费率——见plan文件"项目结构"一节。
package main

import (
	"context"
	"encoding/json"
	"fmt"
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

// 未开启分片(PERP_ENGINE_SYMBOLS没配)时沿用原有的固定group id，兼容
// 现有单实例部署、零迁移成本；开启分片后按NodeID给每个实例分配独立的group id，让每个
// 实例都能独立拿到topic的完整消息流(fan-out)，不依赖Kafka原生的分区负载均衡，见
// docs/engine-sharding.md
func consumerGroupID(base string, cfg config.Config) string {
	if len(cfg.EngineSymbols) == 0 {
		return base
	}
	return fmt.Sprintf("%s-%d", base, cfg.NodeID)
}

func main() {
	cfg := config.Load(1) // contract-engine默认node id=1，跟contract-api(默认0)区分开
	service.InitNodeID(cfg.NodeID)

	// 分片模式下每个实例的consumer group id按NodeID拼(见下面consumerGroupID)，如果运维
	// 开了PERP_ENGINE_SYMBOLS却忘了给每个实例分别设不同的PERP_NODE_ID，多个实例会用同一个
	// 默认NodeID、拼出完全相同的group id，实际效果等同于回退到"同一个group id挂多个订阅
	// 不同topic的member"——这正是known-limitations.md记录过的、已经实测复现过的Kafka
	// 分区分配失效故障模式。这里没法校验"真的全局唯一"(单进程看不到其它实例)，但至少能
	// 拦住"根本没设、还在用默认值"这种最容易犯的错误配置，快速失败比启动后悄悄消费不了
	// 强得多
	if len(cfg.EngineSymbols) > 0 && !cfg.NodeIDExplicit {
		log.Fatalf("开启engine分片(PERP_ENGINE_SYMBOLS)时必须显式设置PERP_NODE_ID，且每个" +
			"实例的值必须互不相同——否则多个实例会用相同的Kafka consumer group id，导致" +
			"分区分配失效，见docs/engine-sharding.md")
	}

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
	roundCloseProgressRepo := repo.NewRoundCloseProgressRepo(conn)

	markPriceSvc := service.NewMarkPriceService(rdb)
	positionSvc := service.NewPositionService(positionRepo, riskLimitRepo, markPriceSvc)
	accountSvc := service.NewAccountService(accountRepo, positionSvc, txRepo)
	settlementSvc := service.NewSettlementService(accountSvc, positionRepo, coinRepo, txRepo)
	fundSvc := service.NewInsuranceFundService(fundRepo)
	fundingSvc := service.NewFundingService(rdb, coinRepo, positionRepo, fundingRepo, accountSvc, txRepo, markPriceSvc)
	klineSvc := service.NewKlineService(klineRepo)
	pushSvc := service.NewPushService(rdb, accountSvc, positionSvc, orderRepo)
	lockSvc := service.NewLockService(rdb)

	if len(cfg.EngineSymbols) > 0 {
		log.Printf("engine分片模式：这个实例负责的symbol=%v，见docs/engine-sharding.md", cfg.EngineSymbols)
	}

	matchingEngine := matching.NewEngine()
	engineSvc := service.NewEngineService(matchingEngine, orderRepo, conditionalOrderRepo, tradeRepo, accountSvc, positionSvc, settlementSvc, markPriceSvc, fundSvc, klineSvc, pushSvc, roundCloseProgressRepo, lockSvc, cfg.EngineSymbols)
	liquidationSvc := service.NewLiquidationService(engineSvc, orderRepo, positionRepo, positionSvc, markPriceSvc, accountSvc, fundSvc, coinRepo, cfg.LiquidationOrderTimeoutMs)
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

	submitGroupID := consumerGroupID("contract-engine", cfg)
	submitConsumer := mq.NewConsumer(cfg.KafkaBrokers, events.TopicOrderSubmit, submitGroupID)
	defer submitConsumer.Close()
	go submitConsumer.Consume(ctx, mq.WithDedup(ctx, submitGroupID, processedMsgRepo, func(msg mq.Message) error {
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

	// 跟submit共用同一个group id(未分片时都是"contract-engine")——这是已经实测验证过能
	// 正常工作的2-member同group形状，见下面round-close的注释
	cancelGroupID := consumerGroupID("contract-engine", cfg)
	cancelConsumer := mq.NewConsumer(cfg.KafkaBrokers, events.TopicOrderCancel, cancelGroupID)
	defer cancelConsumer.Close()
	go cancelConsumer.Consume(ctx, mq.WithDedup(ctx, cancelGroupID, processedMsgRepo, func(msg mq.Message) error {
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

	// 用独立的group id，不要跟上面的submit/cancel共用——同一个group id挂多个订阅不同topic的
	// member，Kafka的分区分配在这种异构订阅场景下不可靠(实测过：3个member共用一个group id时，
	// broker端分配阶段完成了，但每个member实际收不到任何分区，消费彻底卡住，连已有的
	// submit/cancel两个topic也一起被拖挂)，每个独立的消费职责必须用自己独立的group id。
	// 分片部署下round.close事件要fan-out给每个实例(每个实例只处理自己拥有的symbol那部分，
	// 见EngineService.CloseRound)，所以这里也要按实例分配独立group id，不能让Kafka把它当
	// 普通消费者组去做分区负载均衡(那样一个uid的round.close只会被随机分配到的某一个实例
	// 处理到，其它symbol永远没人处理，进度永远凑不齐)
	roundCloseGroupID := consumerGroupID("contract-engine-round-close", cfg)
	roundCloseConsumer := mq.NewConsumer(cfg.KafkaBrokers, events.TopicRoundClose, roundCloseGroupID)
	defer roundCloseConsumer.Close()
	go roundCloseConsumer.Consume(ctx, mq.WithDedup(ctx, roundCloseGroupID, processedMsgRepo, func(msg mq.Message) error {
		var evt events.RoundCloseEvent
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			return err
		}
		return engineSvc.CloseRound(ctx, evt.UID, evt.Round)
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

	engineSrv := api.NewEngineServer(matchingEngine, coinRepo, engineSvc.OwnsSymbol)
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
