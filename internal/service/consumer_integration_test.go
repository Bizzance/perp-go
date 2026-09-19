//go:build integration

package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"perp-go/internal/events"
	"perp-go/internal/model"
	"perp-go/internal/mq"
)

// 用消息直接驱动contract-engine的三个Kafka事件处理函数，不起Kafka——业务处理(解析、查库、交给撮合)
// 才是要验证的，Kafka本身的分区分配、消费位移是基础设施层的事

func msgOf(t *testing.T, topic string, offset int64, evt any) mq.Message {
	t.Helper()
	raw, err := json.Marshal(evt)
	if err != nil {
		t.Fatal(err)
	}
	return mq.Message{Topic: topic, Partition: 0, Offset: offset, Value: raw}
}

// 下单事件：委托已经落库，事件到了之后进订单簿
func TestConsumer_SubmitEventPutsOrderOnBook(t *testing.T) {
	e := newEngineEnv(t)
	uid := e.newAccount(t, 1, "10000")
	o := e.insertOrder(t, uid, openLong)
	msg := msgOf(t, events.TopicOrderSubmit, 1, events.OrderSubmitEvent{
		OrderID: o.OrderID, UID: uid, Symbol: testSymbol, Side: "long", Action: "open", Type: "limit",
		Price: "64000", Amount: "0.1", Leverage: 10,
	})

	if err := e.engine.HandleOrderSubmit(context.Background(), msg); err != nil {
		t.Fatal(err)
	}

	if !e.book.BookFor(testSymbol).Contains(o.OrderID) {
		t.Fatal("下单事件处理之后委托应该在订单簿里")
	}
	if got := e.order(t, o.OrderID).Status; got != model.OrderStatusOpen {
		t.Fatalf("委托应该是open, got %s", got)
	}
}

// 事件里的字段不可信：委托的价格、数量以数据库为准，事件里写什么都不影响
func TestConsumer_SubmitEventIgnoresOrderFieldsInPayload(t *testing.T) {
	e := newEngineEnv(t)
	uid := e.newAccount(t, 1, "10000")
	o := e.insertOrder(t, uid, openLong) // 库里是64000买0.1
	msg := msgOf(t, events.TopicOrderSubmit, 1, events.OrderSubmitEvent{
		OrderID: o.OrderID, UID: uid + 999, Symbol: "ETHUSDT", Side: "short", Action: "close", Type: "market",
		Price: "1", Amount: "999", Leverage: 1,
	})

	if err := e.engine.HandleOrderSubmit(context.Background(), msg); err != nil {
		t.Fatal(err)
	}

	if !e.book.BookFor(testSymbol).Contains(o.OrderID) {
		t.Fatal("应该按库里的BTCUSDT委托进BTCUSDT的订单簿")
	}
	if e.book.BookFor("ETHUSDT").Contains(o.OrderID) {
		t.Fatal("事件里写的symbol不能影响委托进哪个订单簿")
	}
	if got := e.order(t, o.OrderID); got.UID != uid || !got.Price.Equal(decimalOf(t, "64000")) {
		t.Fatalf("委托本身不应该被事件改动: %+v", got)
	}
}

// 找不到委托只记日志、不算错误，也不能碰订单簿；损坏的消息返回错误
func TestConsumer_SubmitEventEdgeCases(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()

	notFound := msgOf(t, events.TopicOrderSubmit, 1, events.OrderSubmitEvent{OrderID: 424242})
	if err := e.engine.HandleOrderSubmit(ctx, notFound); err != nil {
		t.Fatalf("找不到委托不应该返回错误, got %v", err)
	}
	if e.book.BookFor(testSymbol).Contains(424242) {
		t.Fatal("找不到的委托不能进订单簿")
	}

	for _, bad := range []string{"", "not json", `{"orderId": "abc"}`} {
		msg := mq.Message{Topic: events.TopicOrderSubmit, Value: []byte(bad)}
		if err := e.engine.HandleOrderSubmit(ctx, msg); err == nil {
			t.Fatalf("损坏的消息%q应该返回错误", bad)
		}
	}
}

// 撤单事件：委托在订单簿里，事件到了之后摘掉并退回冻结保证金
func TestConsumer_CancelEventCancelsAndRefunds(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	uid := e.newAccount(t, 1, "10000")
	o := e.insertOrder(t, uid, openLong)
	if err := e.engine.SubmitOrder(ctx, o, 1); err != nil {
		t.Fatal(err)
	}
	mustDec(t, e.account(t, uid).FrozenMargin, "640", "挂单期间冻结")
	msg := msgOf(t, events.TopicOrderCancel, 1, events.OrderCancelEvent{OrderID: o.OrderID, UID: uid, Symbol: testSymbol})

	if err := e.engine.HandleOrderCancel(ctx, msg); err != nil {
		t.Fatal(err)
	}

	if got := e.order(t, o.OrderID).Status; got != model.OrderStatusCanceled {
		t.Fatalf("应该是canceled, got %s", got)
	}
	if e.book.BookFor(testSymbol).Contains(o.OrderID) {
		t.Fatal("应该已经摘出订单簿")
	}
	acc := e.account(t, uid)
	mustDec(t, acc.FrozenMargin, "0", "冻结保证金退回")
	mustDec(t, acc.Available, "10000", "可用余额恢复")

	// 同一个撤单事件再来一次(不同的消息偏移，绕过消息级去重)：不能再退一遍
	again := msgOf(t, events.TopicOrderCancel, 2, events.OrderCancelEvent{OrderID: o.OrderID, UID: uid, Symbol: testSymbol})
	if err := e.engine.HandleOrderCancel(ctx, again); err != nil {
		t.Fatal(err)
	}
	mustDec(t, e.account(t, uid).Available, "10000", "重复撤单不能多退")
}

func TestConsumer_CancelEventEdgeCases(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()

	notFound := msgOf(t, events.TopicOrderCancel, 1, events.OrderCancelEvent{OrderID: 424242})
	if err := e.engine.HandleOrderCancel(ctx, notFound); err != nil {
		t.Fatalf("找不到委托不应该返回错误, got %v", err)
	}
	if err := e.engine.HandleOrderCancel(ctx, mq.Message{Value: []byte("garbage")}); err == nil {
		t.Fatal("损坏的消息应该返回错误")
	}
}

// 结束本轮事件：round字段的JSON名字要跟contract-api发出来的一致，账户轮数推进
func TestConsumer_RoundCloseEventAdvancesRound(t *testing.T) {
	e := newEngineEnv(t)
	ctx := context.Background()
	uid := e.newAccount(t, 1, "10000")
	e.setCredit(t, uid, "500", true)

	if err := e.engine.HandleRoundClose(ctx, msgOf(t, events.TopicRoundClose, 1, events.RoundCloseEvent{UID: uid, Round: 0})); err != nil {
		t.Fatal(err)
	}
	if round, insured := e.roundState(t, uid); round != 1 || insured {
		t.Fatalf("round应该推进到1且投保复位, got round=%d insured=%v", round, insured)
	}
	mustDec(t, e.account(t, uid).Credit, "0", "信用额度清零")

	// 过期的round(重复的结束请求)被忽略，不动新一轮
	e.setCredit(t, uid, "200", true)
	if err := e.engine.HandleRoundClose(ctx, msgOf(t, events.TopicRoundClose, 2, events.RoundCloseEvent{UID: uid, Round: 0})); err != nil {
		t.Fatal(err)
	}
	if round, _ := e.roundState(t, uid); round != 1 {
		t.Fatalf("过期的round应该被忽略, got round=%d", round)
	}
	mustDec(t, e.account(t, uid).Credit, "200", "新一轮的信用额度不能被清")

	if err := e.engine.HandleRoundClose(ctx, mq.Message{Value: []byte("{")}); err == nil {
		t.Fatal("损坏的消息应该返回错误")
	}
}
