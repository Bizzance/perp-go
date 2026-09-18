package service

import (
	"context"
	"encoding/json"
	"log"

	"github.com/shopspring/decimal"

	"perp-go/internal/cache"
	"perp-go/internal/matching"
	"perp-go/internal/model"
	"perp-go/internal/pubsub"
	"perp-go/internal/repo"
)

// PushService 通过Redis Pub/Sub往WS网关(contract-api的internal/ws.Hub)发布实时事件——
// 公开频道(深度/成交/K线/标记价格)+私有频道(某个uid的账户/持仓/挂单快照)。
// contract-engine只管发布，不知道、也不需要知道有没有人在订阅，两个进程完全解耦，
// 见docs/websocket.md
type PushService struct {
	cache     *cache.Cache
	accounts  *AccountService
	positions *PositionService
	orders    *repo.OrderRepo
}

func NewPushService(cache *cache.Cache, accounts *AccountService, positions *PositionService, orders *repo.OrderRepo) *PushService {
	return &PushService{cache: cache, accounts: accounts, positions: positions, orders: orders}
}

// publish 把payload序列化成JSON发到指定channel，失败只记日志——推送是锦上添花的实时通知，
// 不是交易正确性的一部分，不能因为Redis抖动之类的问题影响调用方(撮合/结算)的主流程，
// 跟KlineService.RecordTrade同样的取舍
func (p *PushService) publish(ctx context.Context, channel string, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[ERROR] 推送消息序列化失败, channel=%s: %v", channel, err)
		return
	}
	if err := p.cache.Publish(ctx, channel, string(data)); err != nil {
		log.Printf("[ERROR] 推送消息失败, channel=%s: %v", channel, err)
	}
}

func (p *PushService) PublishDepth(ctx context.Context, symbol string, depth matching.DepthSnapshot) {
	p.publish(ctx, pubsub.DepthChannel(symbol), depth)
}

func (p *PushService) PublishTrade(ctx context.Context, trade *model.Trade) {
	p.publish(ctx, pubsub.TradeChannel(trade.Symbol), trade)
}

func (p *PushService) PublishMarkPrice(ctx context.Context, symbol string, price decimal.Decimal) {
	p.publish(ctx, pubsub.MarkPriceChannel(symbol), map[string]any{"symbol": symbol, "price": price})
}

func (p *PushService) PublishKline(ctx context.Context, symbol string, k model.Kline) {
	p.publish(ctx, pubsub.KlineChannel(symbol, string(k.Interval)), k)
}

// UserSnapshot 私有频道user:{uid}推送的内容——账户/持仓/当前挂单三合一，不是增量diff。
// 跟"重新调一次GET /account/info + GET /position/current + GET /order/current"效果
// 一样，只是主动推给客户端，复用同样的查询逻辑，不用担心增量计算算错
type UserSnapshot struct {
	Account      *AccountView   `json:"account"`
	Positions    []PositionView `json:"positions"`
	ActiveOrders []model.Order  `json:"activeOrders"`
}

// PublishUserSnapshot 重新查一遍这个uid当前的account/positions/active orders，整体推送。
// 调用方是几个顶层编排函数(SubmitOrder结算完、CancelOrder、CloseRound、强平相关)，不是
// 每一个底层资金操作方法——那样改动面太大、容易漏，这几个顶层函数本身就是"一次业务动作
// 的完整收尾点"，收尾之后查一次最新状态推送，语义上最清楚。Positions用
// PositionService.Views(带markPrice/unrealizedPnl/roe/liquidationPrice这些计算字段)，
// 不是裸的model.Position——要跟GET /position/current拿到的字段对等，不能让WS推送的
// 数据比REST查询更"缺胳膊少腿"
func (p *PushService) PublishUserSnapshot(ctx context.Context, uid uint64) {
	account, err := p.accounts.View(ctx, uid)
	if err != nil {
		log.Printf("[ERROR] 推送账户快照查询account失败, uid=%d: %v", uid, err)
		return
	}
	positions, err := p.positions.Views(ctx, uid)
	if err != nil {
		log.Printf("[ERROR] 推送账户快照查询positions失败, uid=%d: %v", uid, err)
		return
	}
	orders, err := p.orders.FindActiveByUID(ctx, uid, "")
	if err != nil {
		log.Printf("[ERROR] 推送账户快照查询orders失败, uid=%d: %v", uid, err)
		return
	}
	p.publish(ctx, pubsub.UserChannel(uid), UserSnapshot{Account: account, Positions: positions, ActiveOrders: orders})
}
