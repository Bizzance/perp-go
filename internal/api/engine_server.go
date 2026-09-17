package api

import (
	"strconv"

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
}

func NewEngineServer(matchingEngine *matching.Engine, coins *repo.CoinRepo) *EngineServer {
	return &EngineServer{matchingEngine: matchingEngine, coins: coins}
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
	// 内存膨胀入口，必须先挡掉不存在的symbol
	coin, err := s.coins.FindBySymbol(c.Request.Context(), symbol)
	if err != nil || coin == nil || !coin.Enable {
		fail(c, 400, "合约不存在或已下架")
		return
	}
	levels := 20
	if v := c.Query("levels"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			fail(c, 400, "levels参数不合法")
			return
		}
		levels = n
	}
	book := s.matchingEngine.BookFor(symbol)
	ok(c, book.Depth(levels))
}
