package api

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"perp-go/internal/ws"
)

// upgrader没有做Origin校验(CheckOrigin恒true)——这个系统的鉴权本来就是MVP占位(明文uid，
// 不校验token/来源)，见docs/api.md，等换成真实鉴权中间件时WS这边一起换，不是这次的范围
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// ws升级handler：握手成功后创建一个ws.Client、注册进Hub、阻塞跑读写循环直到连接结束。
// 订阅协议(subscribe/unsubscribe控制消息、channel命名)见docs/websocket.md
func (s *Server) ws(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("[ERROR] WS升级失败: %v", err)
		return
	}
	client := ws.NewClient(conn, s.hub)
	client.Run()
}
