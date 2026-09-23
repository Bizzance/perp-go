package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/api"
	"perp-go/internal/binancefeed"
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
	// 鉴权配置不对要在最前面就失败：放到订单簿恢复、消费者启动之后才发现的话，重启策略会让它
	// 反复"恢复订单簿+消费一批消息+崩溃"
	if err := cfg.ValidateAuth(); err != nil {
		log.Fatal(err)
	}

	// 分片模式下每个实例的consumer group id按NodeID拼(见下面consumerGroupID)，如果运维
	// 开了PERP_ENGINE_SYMBOLS却忘了给每个实例分别设不同的PERP_NODE_ID，多个实例会用同一个
	// 默认NodeID、拼出完全相同的group id，实际效果等同于回退到"同一个group id挂多个订阅
	// 不同topic的member"——这正是known-limitations.md记录过的、已经实测复现过的Kafka
	// 分区分配失效故障模式。这里没法校验"真的全局唯一"(单进程看不到其它实例)，但至少能
	// 拦住"根本没设、还在用默认值"这种最容易犯的错误配置，快速失败比启动后悄悄消费不了
	// 强得多
	if len(cfg.EngineSymbols) > 0 && !cfg.NodeIDExplicit {
		log.Fatalf("开启engine分片时必须显式设置PERP_NODE_ID，且每个实例的值必须互不相同")
	}

	dbConn, err := db.Connect(cfg.MySQLDSN)
	if err != nil {
		log.Fatalf("connect mysql: %v", err)
	}
	defer dbConn.Close()

	rdb, err := cache.Connect(cfg.RedisAddr, cfg.RedisPass)
	if err != nil {
		log.Fatalf("connect redis: %v", err)
	}

	accountRepo := repo.NewAccountRepo(dbConn)
	coinRepo := repo.NewCoinRepo(dbConn)
	orderRepo := repo.NewOrderRepo(dbConn)
	conditionalOrderRepo := repo.NewConditionalOrderRepo(dbConn)
	positionRepo := repo.NewPositionRepo(dbConn)
	tradeRepo := repo.NewTradeRepo(dbConn)
	txRepo := repo.NewTxRepo(dbConn)
	fundRepo := repo.NewInsuranceFundRepo(dbConn)
	fundingRepo := repo.NewFundingRepo(dbConn)
	riskLimitRepo := repo.NewRiskLimitRepo(dbConn)
	klineRepo := repo.NewKlineRepo(dbConn)
	processedMsgRepo := repo.NewProcessedMessageRepo(dbConn)
	roundCloseProgressRepo := repo.NewRoundCloseProgressRepo(dbConn)

	markPriceSvc := service.NewMarkPriceService(rdb).WithConfig(service.MarkPriceConfig{
		MaxIndexAge:  cfg.MarkPriceMaxIndexAge,
		MaxDeviation: decimal.NewFromFloat(cfg.MarkPriceMaxDeviation),
		BasisWindow:  cfg.MarkPriceBasisWindow,
		RequireIndex: cfg.MarkPriceRequireIndex,
	})
	positionSvc := service.NewPositionService(positionRepo, riskLimitRepo, markPriceSvc)
	accountSvc := service.NewAccountService(accountRepo, positionSvc, txRepo)
	settlementSvc := service.NewSettlementService(accountSvc, positionRepo, coinRepo, txRepo)
	fundSvc := service.NewInsuranceFundService(fundRepo)
	fundingSvc := service.NewFundingService(rdb, coinRepo, positionRepo, fundingRepo, accountSvc, txRepo, markPriceSvc)
	klineSvc := service.NewKlineService(klineRepo).WithExternalSource(cfg.KlineSource == "external")
	if cfg.KlineSource == "external" {
		log.Printf("K线来源是外部行情(PERP_KLINE_SOURCE=external)：我们自己的成交不再更新K线")
	}
	pushSvc := service.NewPushService(rdb, accountSvc, positionSvc, orderRepo)
	lockSvc := service.NewLockService(rdb)

	if len(cfg.EngineSymbols) > 0 {
		log.Printf("engine分片模式：这个实例负责的symbol=%v", cfg.EngineSymbols)
	}

	matchingEngine := matching.NewEngine()
	engineSvc := service.NewEngineService(
		matchingEngine,
		orderRepo,
		conditionalOrderRepo,
		tradeRepo,
		accountSvc,
		positionSvc,
		settlementSvc,
		markPriceSvc,
		fundSvc,
		klineSvc,
		pushSvc,
		roundCloseProgressRepo,
		lockSvc,
		coinRepo,
		cfg.EngineSymbols)
	// 资金费率采样读订单簿：只有拥有这个symbol的实例才采样，用冲击价格算溢价
	fundingSvc.WithBook(
		func(symbol string) bool {
			return engineSvc.OwnsSymbol(symbol)
		},
		func(symbol string, notional decimal.Decimal) (decimal.Decimal, decimal.Decimal, bool) {
			return matchingEngine.BookFor(symbol).ImpactPrices(notional)
		})
	liquidationSvc := service.NewLiquidationService(
		engineSvc,
		orderRepo,
		positionRepo,
		positionSvc,
		markPriceSvc,
		accountSvc,
		fundSvc,
		coinRepo,
		cfg.LiquidationOrderTimeoutMs)
	conditionalOrderSvc := service.NewConditionalOrderService(conditionalOrderRepo, orderRepo, markPriceSvc, engineSvc)

	// 订单簿镜像：把币安的订单簿直接镜像成系统账户(service.UID，固定值不需要配置)在撮合引擎里的
	// 真实挂单，进程内直接调用，不经HTTP/Kafka/分布式锁。
	// MirrorSymbols为空(没配PERP_MIRROR_SYMBOLS)表示不开启，本地开发和大多数集成测试不需要
	var mirrorSvc *service.MirrorService
	if len(cfg.MirrorSymbols) > 0 {
		mirrorBinance := &binancefeed.Binance{BaseURL: strings.TrimRight(cfg.MirrorBinanceURL, "/"), Client: &http.Client{Timeout: 5 * time.Second}}
		mirrorSvc, err = service.NewMirrorService(service.MirrorConfig{
			Symbols: cfg.MirrorSymbols, Levels: cfg.MirrorLevels, Interval: cfg.MirrorInterval,
			Leverage: cfg.MirrorLeverage, StaleAfter: cfg.MirrorStaleAfter,
		}, mirrorBinance, accountSvc, orderRepo, coinRepo, engineSvc)
		if err != nil {
			log.Fatalf("订单簿镜像配置不合法: %v", err)
		}
		log.Printf("订单簿镜像已开启: uid=%d symbols=%v 每侧%d档 间隔%s 币安数据%s没更新就撤单",
			service.UID, cfg.MirrorSymbols, cfg.MirrorLevels, cfg.MirrorInterval, cfg.MirrorStaleAfter)
	} else {
		log.Printf("[WARN] 订单簿镜像没有开启(PERP_MIRROR_SYMBOLS没配)：没有其它挂单来源时，订单簿是空的，用户下单没有对手方。" +
			"只能用于本地开发和测试，生产环境必须配置")
	}

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

	// 订单簿恢复完之后再开始镜像：镜像的diff逻辑要先知道订单簿里已经有哪些挂单(重启前留下的)，
	// 不然会把它们当成"不存在"重复挂一遍
	if mirrorSvc != nil {
		go mirrorSvc.Run(ctx)
	}

	submitGroupID := consumerGroupID("contract-engine", cfg)
	submitConsumer := mq.NewConsumer(cfg.KafkaBrokers, events.TopicOrderSubmit, submitGroupID)
	defer submitConsumer.Close()
	go submitConsumer.Consume(ctx, mq.WithDedup(ctx, submitGroupID, processedMsgRepo, func(msg mq.Message) error {
		return engineSvc.HandleOrderSubmit(ctx, msg)
	}))

	// 跟submit共用同一个group id(未分片时都是"contract-engine")——这是已经实测验证过能
	// 正常工作的2-member同group形状，见下面round-close的注释
	cancelGroupID := consumerGroupID("contract-engine", cfg)
	cancelConsumer := mq.NewConsumer(cfg.KafkaBrokers, events.TopicOrderCancel, cancelGroupID)
	defer cancelConsumer.Close()
	go cancelConsumer.Consume(ctx, mq.WithDedup(ctx, cancelGroupID, processedMsgRepo, func(msg mq.Message) error {
		return engineSvc.HandleOrderCancel(ctx, msg)
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
		return engineSvc.HandleRoundClose(ctx, msg)
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

	if !cfg.MarkPriceRequireIndex {
		log.Printf("[WARN] PERP_MARK_REQUIRE_INDEX没有开启：没喂过指数价的合约，标记价会退回最新成交价，可以被自成交操纵。" +
			"只能用于本地开发和测试，生产环境必须开启并持续喂指数价")
	}
	go func() {
		ticker := time.NewTicker(time.Duration(cfg.MarkPriceRefreshMs) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				engineSvc.RefreshMarkPrices(ctx)
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

	// 引擎的HTTP端口(深度查询)同样要求鉴权，规则和contract-api一致
	engineAuth := api.NewAuth(cfg.AuthDisabled, cfg.APIKeys, rdb)
	engineSrv := api.NewEngineServer(matchingEngine, coinRepo, engineSvc.OwnsSymbol, engineAuth)
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
