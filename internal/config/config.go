package config

import (
	"log"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	MySQLDSN     string // 形如 user:pass@tcp(host:port)/dbname?parseTime=true
	RedisAddr    string
	RedisPass    string
	KafkaBrokers []string

	APIAddr        string // contract-api 监听地址
	EngineHTTPAddr string // contract-engine自己的轻量HTTP服务监听地址(目前只有订单簿深度查询)
	NodeID         uint64 // service.NextID用的雪花算法node id，不同进程/实例必须不同
	NodeIDExplicit bool   // PERP_NODE_ID是不是显式设置的(不是走的defaultNodeID)——engine分片
	// 模式下main.go要用这个做启动时校验，见parseEngineSymbols旁边的说明

	// 撮合/风控相关的可调参数，先用固定默认值
	LiquidationOrderTimeoutMs int64 // 强平单挂单排队超时兜底阈值
	RiskScanIntervalMs        int64 // 强平扫描周期
	MarkPriceEmaAlpha         float64
	FundingSampleIntervalMs   int64 // 资金费率溢价采样周期，采样越密集TWAP越准
	ConditionalScanIntervalMs int64 // 条件单(止盈止损)触发扫描周期
	SymbolCacheRefreshMs      int64 // /depth接口symbol合法性校验用的内存缓存刷新周期

	DedupRetentionHours    int64 // Kafka消息去重记录(processed_messages)保留多久，早于这个时长的清掉
	DedupCleanupIntervalMs int64 // 去重记录清理任务的扫描周期

	// EngineSymbols 这个contract-engine实例负责撮合的symbol列表，来自PERP_ENGINE_SYMBOLS
	// (逗号分隔，如"BTCUSDT,ETHUSDT")。nil(没设这个环境变量)=负责全部symbol，这是单实例
	// 部署的默认行为，不需要额外配置。见docs/engine-sharding.md
	EngineSymbols []string
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// defaultNodeID是这个进程类型没设PERP_NODE_ID环境变量时用的默认node id——单实例
// 部署时contract-api/contract-engine各自传一个固定值(见各自main.go)，不需要额外配置就能
// 保证两边不撞。要横向扩展(同一进程类型跑多个实例)必须显式设PERP_NODE_ID区分，不能指望
// 默认值——多个实例传同一个defaultNodeID会导致NextID理论上生成重复ID
func Load(defaultNodeID uint64) Config {
	nodeID := defaultNodeID
	nodeIDExplicit := false
	if v := os.Getenv("PERP_NODE_ID"); v != "" {
		parsed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			log.Fatalf("PERP_NODE_ID不合法: %v", err)
		}
		nodeID = parsed
		nodeIDExplicit = true
	}
	return Config{
		MySQLDSN:                  envOr("PERP_MYSQL_DSN", "perpgo:local123@tcp(127.0.0.1:3306)/perpgo?parseTime=true&loc=Local"),
		RedisAddr:                 envOr("PERP_REDIS_ADDR", "127.0.0.1:6379"),
		RedisPass:                 envOr("PERP_REDIS_PASS", "local123"),
		KafkaBrokers:              []string{envOr("PERP_KAFKA_BROKER", "127.0.0.1:9092")},
		APIAddr:                   envOr("PERP_API_ADDR", ":7001"),
		EngineHTTPAddr:            envOr("PERP_ENGINE_HTTP_ADDR", ":7002"),
		NodeID:                    nodeID,
		NodeIDExplicit:            nodeIDExplicit,
		LiquidationOrderTimeoutMs: 10_000,
		RiskScanIntervalMs:        2_000,
		MarkPriceEmaAlpha:         1.0, // 标记价=最新成交价，不做平滑
		FundingSampleIntervalMs:   60_000,
		ConditionalScanIntervalMs: 2_000,
		SymbolCacheRefreshMs:      30_000,
		DedupRetentionHours:       168,       // 7天，跟Kafka topic的常见默认retention对齐
		DedupCleanupIntervalMs:    3_600_000, // 1小时扫一次，清理任务本身很轻量，不需要跑得更勤
		EngineSymbols:             parseEngineSymbols(os.Getenv("PERP_ENGINE_SYMBOLS")),
	}
}

func parseEngineSymbols(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	symbols := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			symbols = append(symbols, p)
		}
	}
	if len(symbols) == 0 {
		return nil
	}
	return symbols
}
