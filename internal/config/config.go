package config

import "os"

type Config struct {
	MySQLDSN     string // 形如 user:pass@tcp(host:port)/dbname?parseTime=true
	RedisAddr    string
	RedisPass    string
	KafkaBrokers []string

	APIAddr string // contract-api 监听地址

	// 撮合/风控相关的可调参数，先用固定默认值
	LiquidationOrderTimeoutMs int64 // 强平单挂单排队超时兜底阈值
	RiskScanIntervalMs        int64 // 强平扫描周期
	MarkPriceEmaAlpha         float64
	FundingSampleIntervalMs   int64 // 资金费率溢价采样周期，采样越密集TWAP越准
	ConditionalScanIntervalMs int64 // 条件单(止盈止损)触发扫描周期
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func Load() Config {
	return Config{
		MySQLDSN:                  envOr("PERP_MYSQL_DSN", "perpgo:local123@tcp(127.0.0.1:3306)/perpgo?parseTime=true&loc=Local"),
		RedisAddr:                 envOr("PERP_REDIS_ADDR", "127.0.0.1:6379"),
		RedisPass:                 envOr("PERP_REDIS_PASS", "local123"),
		KafkaBrokers:              []string{envOr("PERP_KAFKA_BROKER", "127.0.0.1:9092")},
		APIAddr:                   envOr("PERP_API_ADDR", ":7001"),
		LiquidationOrderTimeoutMs: 10_000,
		RiskScanIntervalMs:        2_000,
		MarkPriceEmaAlpha:         1.0, // 标记价=最新成交价，不做平滑
		FundingSampleIntervalMs:   60_000,
		ConditionalScanIntervalMs: 2_000,
	}
}
