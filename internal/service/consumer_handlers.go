package service

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"perp-go/internal/events"
	"perp-go/internal/mq"
)

// 下面三个是contract-engine消费Kafka事件的业务处理，从main里提出来是为了能不起Kafka、直接拿消息
// 驱动做集成测试。返回的error只会被消费者记日志、不会重投，见mq.Consumer.Consume；
// 消息级去重(mq.WithDedup)由调用方包在外面

// 处理下单事件：按事件里的orderId从库里取出委托交给撮合。contract-api在发事件之前已经把委托落库并
// 冻结了保证金，所以事件里只用orderId，委托的其余字段以数据库为准。找不到委托只记日志、不算错误
func (e *EngineService) HandleOrderSubmit(ctx context.Context, msg mq.Message) error {
	var evt events.OrderSubmitEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		return err
	}
	o, err := e.orders.FindByOrderID(ctx, evt.OrderID)
	if err != nil || o == nil {
		log.Printf("[ERROR] order %d not found for submit event", evt.OrderID)
		return err
	}
	return e.SubmitOrder(ctx, o, time.Now().UnixNano())
}

// 处理撤单事件：找到委托就从订单簿摘掉并退回冻结保证金，找不到(已经被清理、或者事件早于落库)直接忽略
func (e *EngineService) HandleOrderCancel(ctx context.Context, msg mq.Message) error {
	var evt events.OrderCancelEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		return err
	}
	o, err := e.orders.FindByOrderID(ctx, evt.OrderID)
	if err != nil || o == nil {
		return err
	}
	return e.CancelOrder(ctx, o)
}

// 处理结束本轮事件：事件里的round是要结束的那一轮，跟账户当前轮数不一致会被CloseRound忽略
func (e *EngineService) HandleRoundClose(ctx context.Context, msg mq.Message) error {
	var evt events.RoundCloseEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		return err
	}
	return e.CloseRound(ctx, evt.UID, evt.Round)
}
