// Package events 定义 contract-api 和 contract-engine 之间通过Kafka传递的消息结构——
// 对应Java版"API进程收单、engine进程撮合"这套拆分里两边靠Kafka解耦的思路。
package events

const (
	TopicOrderSubmit = "perpgo.order.submit"
	TopicOrderCancel = "perpgo.order.cancel"
)

// OrderSubmitEvent 下单事件：contract-api校验参数+冻结保证金+落库(status=NEW)之后发出，
// engine消费到之后把这笔委托放进对应symbol的订单簿参与撮合。冻结保证金这一步故意放在
// contract-api同步完成(而不是留给engine异步处理)，这样"余额不足"能在HTTP响应里立刻
// 告诉调用方，不用等一趟Kafka往返，照抄Java版ContractOrderController.addOrder的设计
type OrderSubmitEvent struct {
	OrderID    uint64 `json:"orderId"`
	UID        uint64 `json:"uid"`
	Symbol     string `json:"symbol"`
	Side       string `json:"side"`
	Action     string `json:"action"`
	Type       string `json:"type"`
	Price      string `json:"price"` // decimal序列化成字符串，避免JSON数字精度问题
	Amount     string `json:"amount"`
	Leverage   uint32 `json:"leverage"`
	ReduceOnly bool   `json:"reduceOnly"`
}

// OrderCancelEvent 撤单事件
type OrderCancelEvent struct {
	OrderID uint64 `json:"orderId"`
	UID     uint64 `json:"uid"`
	Symbol  string `json:"symbol"`
}
