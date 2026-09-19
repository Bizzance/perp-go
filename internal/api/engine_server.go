package api

import (
	"context"
	"log"
	"sync/atomic"

	"github.com/gin-gonic/gin"

	"perp-go/internal/matching"
	"perp-go/internal/repo"
)

// 是contract-engine进程自己的轻量HTTP服务，目前只提供订单簿深度查询——
// contract-engine是唯一持有真实内存订单簿(matching.Engine)状态的进程，直接在这里暴露
// 查询接口最自然，不用把订单簿状态同步到contract-api那边(那样会引入跨进程状态一致性问题)。
// 跟contract-api的Server是两个完全独立的HTTP服务/端口，复用同一套fail/ok响应helper
type EngineServer struct {
	matchingEngine *matching.Engine
	coins          *repo.CoinRepo
	ownsSymbol     func(symbol string) bool        // 见docs/engine-sharding.md，nil或恒真=单实例部署
	enabledSymbols atomic.Pointer[map[string]bool] // 见RefreshSymbols
	auth           *Auth
}

func NewEngineServer(matchingEngine *matching.Engine, coins *repo.CoinRepo, ownsSymbol func(symbol string) bool, auth *Auth) *EngineServer {
	s := &EngineServer{matchingEngine: matchingEngine, coins: coins, ownsSymbol: ownsSymbol, auth: auth}
	empty := map[string]bool{}
	s.enabledSymbols.Store(&empty)
	return s
}

// 刷新一次"启用的合约symbol"缓存——/depth是这个进程唯一暴露的高频查询
// 接口，Book.mu特意换成读写锁就是为了让多个并发的深度查询不用互相排队，如果每次请求还要
// 再查一次MySQL校验symbol合法性，等于把这个优化的意义抵消掉大半。改成内存缓存+定时刷新，
// 新增/停用合约有几十秒的生效延迟——这类配置变更本来就是低频的运营操作，能接受这个延迟
// 换查询接口的性能。main.go负责在启动时调一次、再起个ticker定时刷新
func (s *EngineServer) RefreshSymbols(ctx context.Context) {
	coins, err := s.coins.FindAllEnabled(ctx)
	if err != nil {
		log.Printf("[ERROR] 刷新symbol缓存失败: %v", err)
		return
	}
	m := make(map[string]bool, len(coins))
	for _, c := range coins {
		m[c.Symbol] = true
	}
	s.enabledSymbols.Store(&m)
}

func (s *EngineServer) Router() *gin.Engine {
	r := gin.Default()
	// 深度查询虽然是公开行情，也要求带有效签名：引擎的端口不应该有任何免鉴权的入口(/health除外)
	r.Use(s.auth.Middleware())
	r.GET("/health", s.health)
	s.auth.Route(r, "GET", "/depth", ScopeTrade, s.depth)
	return r
}

// 存活探针，容器编排用。能响应说明进程已经启动完成——订单簿恢复在HTTP服务启动之前就跑完了
// (恢复失败进程会直接退出)，所以探针通过就代表订单簿是完整的
func (s *EngineServer) health(c *gin.Context) {
	ok(c, gin.H{"status": "ok"})
}

func (s *EngineServer) depth(c *gin.Context) {
	symbol := c.Query("symbol")
	if symbol == "" {
		fail(c, 400, "symbol参数必填")
		return
	}
	// 必须校验symbol存在——matching.Engine.BookFor对任何没见过的symbol字符串都会创建
	// 一个新的空Book并永久存进map里，不会自动清理。这个接口没有鉴权，任由调用方传任意
	// 字符串会变成一个无限增长的内存占用点(每个不同symbol=一个永久Book)，等于一个开放的
	// 内存膨胀入口，必须先挡掉不存在的symbol——用内存缓存校验(见RefreshSymbols)，不查DB
	if !(*s.enabledSymbols.Load())[symbol] {
		fail(c, 400, "合约不存在或已下架")
		return
	}
	// 分片部署下这个实例可能根本不负责这个symbol——它的本地Book要么是空的、要么(重启
	// 恢复时已经按ownedSymbols过滤过)压根没有这个symbol的条目，返回一个看起来"合法但是
	// 空"的深度会误导调用方，不如直接明确拒绝，见docs/engine-sharding.md
	if s.ownsSymbol != nil && !s.ownsSymbol(symbol) {
		fail(c, 400, "这个实例不负责该symbol的撮合，请求路由到正确的分片")
		return
	}
	levels, msg := parsePositiveIntQuery(c, "levels", matching.DefaultDepthLevels)
	if msg != "" {
		fail(c, 400, msg)
		return
	}
	book := s.matchingEngine.BookFor(symbol)
	ok(c, book.Depth(levels))
}
