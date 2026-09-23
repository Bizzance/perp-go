package config

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
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
	MarkPriceRefreshMs        int64 // 标记价定时刷新周期(采盘口基差、按最新指数价重算)
	FundingSampleIntervalMs   int64 // 资金费率溢价采样周期，采样越密集TWAP越准
	ConditionalScanIntervalMs int64 // 条件单(止盈止损)触发扫描周期
	SymbolCacheRefreshMs      int64 // /depth接口symbol合法性校验用的内存缓存刷新周期

	DedupRetentionHours    int64 // Kafka消息去重记录(processed_messages)保留多久，早于这个时长的清掉
	DedupCleanupIntervalMs int64 // 去重记录清理任务的扫描周期

	// 标记价，见docs/mark-price.md和service.MarkPriceConfig
	MarkPriceMaxIndexAge  time.Duration // PERP_MARK_MAX_INDEX_AGE_SEC，指数价多久没更新算断供
	MarkPriceMaxDeviation float64       // PERP_MARK_MAX_DEVIATION，标记价相对指数价的最大偏离比例
	MarkPriceBasisWindow  time.Duration // PERP_MARK_BASIS_WINDOW_SEC，盘口基差取多长窗口的平均
	MarkPriceRequireIndex bool          // PERP_MARK_REQUIRE_INDEX=true，没有指数价就不产生标记价，生产环境必须开
	// POST /index-price的服务端跳变保护，见service.MarkPriceService.PushIndexPrice。
	// 0=不校验(没设这个环境变量就是这样)，生产环境设成0.05
	IndexMaxJump     float64       // PERP_INDEX_MAX_JUMP，一次推送相对当前指数价的变动超过这个比例就要等确认
	IndexJumpConfirm time.Duration // PERP_INDEX_JUMP_CONFIRM_SEC，新价位要持续多久才承认

	// K线的来源，PERP_KLINE_SOURCE：trades=用我们自己的成交更新K线(默认)；external=K线只来自外部行情
	// (orderbook-sync从币安同步，POST /kline/sync)，我们自己的成交不再写K线，见docs/kline.md。
	// contract-api和contract-engine必须配成一样的
	KlineSource string

	// EngineSymbols 这个contract-engine实例负责撮合的symbol列表，来自PERP_ENGINE_SYMBOLS
	// (逗号分隔，如"BTCUSDT,ETHUSDT")。nil(没设这个环境变量)=负责全部symbol，这是单实例
	// 部署的默认行为，不需要额外配置。见docs/engine-sharding.md
	EngineSymbols []string

	// 订单簿镜像(service.MirrorService，contract-engine进程内部直接调用账户/撮合服务把币安
	// 订单簿镜像成系统账户的真实挂单，不经HTTP/Kafka/分布式锁)，见docs/orderbook-sync.md。
	// 系统账户uid是固定值(service.UID)，不需要配置，也不需要配置/维护它的余额——它的
	// FreezeMargin永远无条件成功，见AccountService.FreezeMargin。MirrorSymbols为空
	// (没设PERP_MIRROR_SYMBOLS)表示不开启，本地开发/大多数集成测试不需要；生产环境必须配置
	MirrorSymbols    []string
	MirrorLevels     int
	MirrorInterval   time.Duration
	MirrorLeverage   int
	MirrorStaleAfter time.Duration
	MirrorBinanceURL string

	// AuthDisabled 关闭接口鉴权，只给本地开发用(PERP_AUTH_DISABLED=true)。默认开启：开启但一个
	// 密钥都没配置时进程拒绝启动，不会悄悄退化成"没有鉴权"，见docs/auth-design.md
	AuthDisabled bool
	// APIKeys 合作方的API密钥，来自PERP_API_KEYS，格式见ParseAPIKeys
	APIKeys []APIKey
}

// 一个合作方的API凭证。Secret只有双方知道，永远不随请求发送，用来算请求签名
type APIKey struct {
	ID     string
	Secret string
	Scopes []string // trade=交易和查询类接口，ops=运营类接口(加钱扣钱、发额度、设投保、喂指数价)
}

// 合法的权限范围。trade和ops分开授权：合作方业务后端需要的和运营/行情源需要的差别很大，
// 一把只喂指数价的密钥不应该能下单，一把交易密钥不应该能给账户加钱
var validScopes = map[string]bool{"trade": true, "ops": true}

// 密钥最短长度：太短的secret扛不住暴力破解HMAC
const minSecretLen = 16

// 启动时检查鉴权配置，两个进程在config.Load之后立刻调用(在连数据库、恢复订单簿、启动消费者之前)：
// 默认开启鉴权时必须至少有一把密钥，否则拒绝启动，不能悄悄退化成"没有鉴权"。放在配置层统一检查，
// 是为了两个进程用同一条规则、同一句报错，也不会等到订单簿恢复、开始消费消息之后才发现配置不对
func (c Config) ValidateAuth() error {
	if !c.AuthDisabled && len(c.APIKeys) == 0 {
		return fmt.Errorf("接口鉴权默认开启，必须通过PERP_API_KEYS配置至少一把密钥(格式 id:secret:trade|ops)；" +
			"本地开发可以设PERP_AUTH_DISABLED=true显式关闭，见docs/auth-design.md")
	}
	return nil
}

// 解析PERP_API_KEYS：逗号分隔多把密钥，每把是`id:secret:scope|scope`，比如
// `partner-a:0123456789abcdef0123:trade|ops,booksync:abcdef0123456789abcd:trade|ops`。选这个格式而不是
// JSON，是因为它放进.env文件和环境变量里不用处理引号转义。id和secret里不能出现`:`、`,`、`|`
func ParseAPIKeys(raw string) ([]APIKey, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	seen := make(map[string]bool)
	var keys []APIKey
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts := strings.Split(item, ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("密钥格式不对(应该是 id:secret:scope|scope)，注意不要在id/secret里带冒号或逗号")
		}
		id, secret := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		if id == "" {
			return nil, fmt.Errorf("密钥的id不能为空")
		}
		if seen[id] {
			return nil, fmt.Errorf("密钥id重复: %s", id)
		}
		seen[id] = true
		if len(secret) < minSecretLen {
			return nil, fmt.Errorf("密钥%s的secret太短(至少%d位)", id, minSecretLen)
		}
		// 环境变量模板里的占位值(CHANGE_ME...)如果原样带进生产，会让服务用一个写在仓库里、
		// 谁都看得到的密钥启动。占位值可能被补长过第16位的长度检查，所以单独拒绝
		if strings.Contains(strings.ToLower(secret), "change_me") {
			return nil, fmt.Errorf("密钥%s的secret还是模板里的占位值，请换成随机串(openssl rand -hex 24)", id)
		}
		var scopes []string
		for _, sc := range strings.Split(parts[2], "|") {
			sc = strings.TrimSpace(sc)
			if sc == "" {
				continue
			}
			if !validScopes[sc] {
				return nil, fmt.Errorf("密钥%s的权限范围不合法: %s(只能是trade或ops)", id, sc)
			}
			scopes = append(scopes, sc)
		}
		if len(scopes) == 0 {
			return nil, fmt.Errorf("密钥%s至少要有一个权限范围", id)
		}
		keys = append(keys, APIKey{ID: id, Secret: secret, Scopes: scopes})
	}
	return keys, nil
}

// 读整数环境变量，没设用默认值，设了但不合法直接退出——带着一个悄悄退回默认值的风控参数跑起来，比起不来更糟
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

// 读浮点环境变量，规则同envInt，取值必须大于0
func envFloat(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 {
		log.Fatalf("%s不合法(需要大于0的数): %q", key, v)
	}
	return f
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
	apiKeys, err := ParseAPIKeys(os.Getenv("PERP_API_KEYS"))
	if err != nil {
		log.Fatalf("PERP_API_KEYS不合法: %v", err)
	}
	return Config{
		AuthDisabled:              os.Getenv("PERP_AUTH_DISABLED") == "true",
		APIKeys:                   apiKeys,
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
		MarkPriceRefreshMs:        1_000,
		FundingSampleIntervalMs:   60_000,
		ConditionalScanIntervalMs: 2_000,
		SymbolCacheRefreshMs:      30_000,
		DedupRetentionHours:       168,       // 7天，跟Kafka topic的常见默认retention对齐
		DedupCleanupIntervalMs:    3_600_000, // 1小时扫一次，清理任务本身很轻量，不需要跑得更勤
		MarkPriceMaxIndexAge:      time.Duration(envInt("PERP_MARK_MAX_INDEX_AGE_SEC", 30)) * time.Second,
		MarkPriceMaxDeviation:     envFloat("PERP_MARK_MAX_DEVIATION", 0.01),
		MarkPriceBasisWindow:      time.Duration(envInt("PERP_MARK_BASIS_WINDOW_SEC", 60)) * time.Second,
		MarkPriceRequireIndex:     os.Getenv("PERP_MARK_REQUIRE_INDEX") == "true",
		IndexMaxJump:              envFloat("PERP_INDEX_MAX_JUMP", 0),
		IndexJumpConfirm:          time.Duration(envInt("PERP_INDEX_JUMP_CONFIRM_SEC", 3)) * time.Second,
		EngineSymbols:             parseEngineSymbols(os.Getenv("PERP_ENGINE_SYMBOLS")),
		KlineSource:               klineSource(),
		MirrorSymbols:             parseEngineSymbols(os.Getenv("PERP_MIRROR_SYMBOLS")),
		MirrorLevels:              int(envInt("PERP_MIRROR_LEVELS", 50)),
		MirrorInterval:            time.Duration(envInt("PERP_MIRROR_INTERVAL_MS", 1000)) * time.Millisecond,
		MirrorLeverage:            int(envInt("PERP_MIRROR_LEVERAGE", 5)),
		MirrorStaleAfter:          time.Duration(envInt("PERP_MIRROR_STALE_SEC", 10)) * time.Second,
		MirrorBinanceURL:          envOr("PERP_MIRROR_BINANCE_URL", "https://fapi.binance.com"),
	}
}

// 读PERP_KLINE_SOURCE，没设是trades；写错了直接退出——悄悄退回默认值会让K线来源和预期不一样
func klineSource() string {
	v := envOr("PERP_KLINE_SOURCE", "trades")
	if v != "trades" && v != "external" {
		log.Fatalf("PERP_KLINE_SOURCE不合法(只能是trades或external): %q", v)
	}
	return v
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
