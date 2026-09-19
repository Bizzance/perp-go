package ws

import (
	"context"
	"encoding/json"
	"log"

	"perp-go/internal/cache"
	"perp-go/internal/pubsub"
)

// 管理"Redis channel -> 订阅了它的本地WS连接集合"，用引用计数做懒订阅：第一个客户端
// 订阅某个channel时才真正去Redis SUBSCRIBE，最后一个客户端退订/断开时才UNSUBSCRIBE，不会
// 一次性订阅全部symbol×interval的组合。contract-api进程里只有一个Hub实例，被全部WS连接
// 共用，见docs/websocket.md
//
// 内部用一个单goroutine(run)串行处理全部订阅/退订/客户端移除/广播操作，不是拿锁保护
// channels/cancels这两个map——从Redis收到消息的relay goroutine不直接读map做广播，而是
// 把"广播"也封装成一个操作丢进同一个incoming队列，这样"谁订阅了什么"和"该给谁广播"
// 全部由run()这一个goroutine串行处理，天然没有数据竞争，不需要额外加锁
type Hub struct {
	cache    *cache.Cache
	incoming chan hubOp
	channels map[string]map[*Client]struct{} // channel -> 订阅了它的客户端集合
	cancels  map[string]context.CancelFunc   // channel -> 对应Redis中继goroutine的取消函数
}

type opKind int

const (
	opSubscribe opKind = iota
	opUnsubscribe
	opRemoveClient
	opBroadcast
)

type hubOp struct {
	kind     opKind
	client   *Client
	channels []string // subscribe/unsubscribe用；broadcast时只用channels[0]
	payload  []byte   // broadcast用，已经封装好的、直接发给客户端的完整消息
}

func NewHub(cache *cache.Cache) *Hub {
	h := &Hub{
		cache:    cache,
		incoming: make(chan hubOp, 1024),
		channels: make(map[string]map[*Client]struct{}),
		cancels:  make(map[string]context.CancelFunc),
	}
	go h.run()
	return h
}

func (h *Hub) run() {
	for op := range h.incoming {
		switch op.kind {
		case opSubscribe:
			for _, ch := range op.channels {
				h.subscribe(op.client, ch)
			}
		case opUnsubscribe:
			for _, ch := range op.channels {
				h.unsubscribe(op.client, ch)
			}
		case opRemoveClient:
			for ch := range op.client.subscribed {
				h.unsubscribe(op.client, ch)
			}
		case opBroadcast:
			h.broadcast(op.channels[0], op.payload)
		}
	}
}

func (h *Hub) subscribe(c *Client, channel string) {
	if c.subscribed[channel] {
		return
	}
	if h.channels[channel] == nil {
		h.channels[channel] = make(map[*Client]struct{})
	}
	h.channels[channel][c] = struct{}{}
	c.subscribed[channel] = true
	if _, exists := h.cancels[channel]; !exists {
		h.startRelay(channel)
	}
}

func (h *Hub) unsubscribe(c *Client, channel string) {
	if !c.subscribed[channel] {
		return
	}
	delete(c.subscribed, channel)
	set := h.channels[channel]
	delete(set, c)
	if len(set) == 0 {
		delete(h.channels, channel)
		if cancel, ok := h.cancels[channel]; ok {
			cancel()
			delete(h.cancels, channel)
		}
	}
}

func (h *Hub) broadcast(channel string, payload []byte) {
	for c := range h.channels[channel] {
		c.trySend(payload)
	}
}

// 起一个goroutine SUBSCRIBE这个Redis channel，收到消息就转成一条opBroadcast
// 操作丢回h.incoming——这样广播的实际执行(查订阅者列表、写各个客户端的send channel)
// 也在run这个唯一goroutine里串行完成，不会跟subscribe/unsubscribe操作竞态，也不需要
// 在Hub结构体上加mutex。channel参数是客户端订阅时用的名字(比如"depth:BTCUSDT")，
// h.channels/h.cancels的key也统一用这个"客户端视角"的名字记账，只有真正调Redis SUBSCRIBE
// 这一步才用internal/pubsub包转换成带前缀的真实channel名(比如"perpgo:ws:depth:BTCUSDT")——
// 发布端(internal/service/push.go)和订阅端共用同一个pubsub包算前缀，不是两边各自维护
// 一份前缀常量，那样一旦有一边改了忘了改另一边，Hub会订阅到错误的channel、表现为"连上了
// 但永远收不到推送"且没有任何报错，这正是开发过程中第一次联调时实测踩到的bug
func (h *Hub) startRelay(channel string) {
	ctx, cancel := context.WithCancel(context.Background())
	h.cancels[channel] = cancel
	sub := h.cache.Subscribe(ctx, pubsub.Prefix+channel)
	go func() {
		defer sub.Close()
		ch := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				envelope, err := json.Marshal(struct {
					Channel string          `json:"channel"`
					Data    json.RawMessage `json:"data"`
				}{Channel: channel, Data: json.RawMessage(msg.Payload)})
				if err != nil {
					log.Printf("[ERROR] WS消息封装失败, channel=%s: %v", channel, err)
					continue
				}
				h.incoming <- hubOp{kind: opBroadcast, channels: []string{channel}, payload: envelope}
			}
		}
	}()
}

func (h *Hub) Subscribe(c *Client, channels []string) {
	h.incoming <- hubOp{kind: opSubscribe, client: c, channels: channels}
}

func (h *Hub) Unsubscribe(c *Client, channels []string) {
	h.incoming <- hubOp{kind: opUnsubscribe, client: c, channels: channels}
}

func (h *Hub) RemoveClient(c *Client) {
	h.incoming <- hubOp{kind: opRemoveClient, client: c}
}
