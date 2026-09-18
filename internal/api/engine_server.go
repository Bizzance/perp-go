package api

import (
	"context"
	"log"
	"sync/atomic"

	"github.com/gin-gonic/gin"

	"perp-go/internal/matching"
	"perp-go/internal/repo"
)

// EngineServer 是contract-engine进程自己的轻量HTTP服务，目前只提供订单簿深度查询——
// contract-engine是唯一持有真实内存订单簿(matching.Engine)状态的进程，直接在这里暴露
// 查询接口最自然，不用把订单簿状态同步到contract-api那边(那样会引入跨进程状态一致性问题)。
// 跟contract-api的Server是两个完全独立的HTTP服务/端口，复用同一套fail/ok响应helper
type EngineServer struct {
	matchingEngine *matching.Engine
	coins          *repo.CoinRepo
	enabledSymbols atomic.Pointer[map[string]bool] // 见RefreshSymbols
}

func NewEngineServer(matchingEngine *matching.Engine, coins *repo.CoinRepo) *EngineServer {
	s := &EngineServer{matchingEngine: matchingEngine, coins: coins}
	empty := map[string]bool{}
	s.enabledSymbols.Store(&empty)
	return s
}

// RefreshSymbols 刷新一次"启用的合约symbol"缓存——/depth是这个进程唯一暴露的高频查询
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
	r.GET("/depth", s.depth)
	return r
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
	levels, msg := parsePositiveIntQuery(c, "levels", matching.DefaultDepthLevels)
	if msg != "" {
		fail(c, 400, msg)
		return
	}
	book := s.matchingEngine.BookFor(symbol)
	ok(c, book.Depth(levels))
}
