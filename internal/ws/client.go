package ws

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10 // 必须小于pongWait，才能在对方判定超时之前把下一个ping发出去
	maxMessageSize = 4096                // 客户端只会发订阅/取消订阅这种小控制消息，没必要放开上限
	sendBufferSize = 256                 // 单个连接的待发送消息队列，见Client.trySend满了就丢的策略
)

// 客户端发来的订阅/取消订阅控制消息
type controlMessage struct {
	Op       string   `json:"op"` // "subscribe" / "unsubscribe"
	Channels []string `json:"channels"`
}

// 一个WS连接，标准gorilla/websocket读写两个goroutine的模式：读goroutine(readPump)
// 处理客户端发来的订阅/取消订阅控制消息、检测断连；写goroutine(writePump)把Hub转发过来的
// 消息写给客户端，附带定时ping心跳。两个goroutine之间用send这个channel传递待发送消息，
// 不直接共享*websocket.Conn的写操作(gorilla/websocket不允许并发写同一个连接)
type Client struct {
	conn       *websocket.Conn
	hub        *Hub
	send       chan []byte
	subscribed map[string]bool // 这个连接当前订阅了哪些channel，断连时按这个清理，只有Hub的run()goroutine会读写
}

func NewClient(conn *websocket.Conn, hub *Hub) *Client {
	return &Client{
		conn:       conn,
		hub:        hub,
		send:       make(chan []byte, sendBufferSize),
		subscribed: make(map[string]bool),
	}
}

// 非阻塞投递——发送队列满了说明这个客户端处理不过来(网络慢/客户端卡住)，直接丢弃
// 这条消息，不能阻塞Hub的广播循环让一个慢客户端拖慢所有人。深度/成交这类高频公开频道丢一条
// 影响不大，下一条很快就来；账户快照丢一条相对麻烦，但这属于网络异常情况，客户端本来就该
// 自己定期用REST接口(GET /account/info等)校准，不能假设WS推送绝对不丢
func (c *Client) trySend(msg []byte) {
	select {
	case c.send <- msg:
	default:
		log.Printf("[WARN] WS客户端发送队列已满，丢弃一条消息")
	}
}

// 启动这个连接的读写循环，阻塞到连接结束——由HTTP升级handler在处理完握手之后调用，
// 通常放在一个新goroutine里跑(读写各自还会再起一个goroutine，这个Run本身只是负责收尾)
func (c *Client) Run() {
	go c.writePump()
	c.readPump() // 阻塞在这里，直到连接断开
}

func (c *Client) readPump() {
	defer func() {
		c.hub.RemoveClient(c)
		c.conn.Close()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return // 连接关闭/异常，defer里会清理订阅
		}
		var msg controlMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue // 不合法的控制消息直接忽略，不断开连接——容忍客户端偶尔发错格式
		}
		switch msg.Op {
		case "subscribe":
			c.hub.Subscribe(c, msg.Channels)
		case "unsubscribe":
			c.hub.Unsubscribe(c, msg.Channels)
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()
	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// Hub那边没有主动关闭send channel的路径(RemoveClient不关channel，靠
				// readPump检测到连接错误退出、连带writePump也退出)，这个分支理论上不会走到，
				// 保留是gorilla/websocket官方示例的标准写法，防御性处理
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
