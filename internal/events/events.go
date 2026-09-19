package events

const (
	TopicOrderSubmit = "perpgo.order.submit"
	TopicOrderCancel = "perpgo.order.cancel"
	TopicRoundClose  = "perpgo.round.close"
)

// 下单事件：contract-api校验参数+冻结保证金+落库(status=NEW)之后发出，
// engine消费到之后把这笔委托放进对应symbol的订单簿参与撮合。冻结保证金这一步故意放在
// contract-api同步完成(而不是留给engine异步处理)，这样"余额不足"能在HTTP响应里立刻
// 告诉调用方，不用等一趟Kafka往返
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

// 撤单事件
type OrderCancelEvent struct {
	OrderID uint64 `json:"orderId"`
	UID     uint64 `json:"uid"`
	Symbol  string `json:"symbol"`
}

// 结束本轮事件：contract-api校验完(账户存在等)之后发出，engine消费到之后撤掉这个uid全部
// 排队中的委托、按标记价强平全部仓位、清算credit/round——撤单要摘掉engine内存里的订单簿，
// 只能走这条路由过去，不能在contract-api那边直接做
type RoundCloseEvent struct {
	UID uint64 `json:"uid"`
	// Round 要结束的那一轮。引擎处理时账户当前的round不等于它就直接忽略——这是"结束本轮"
	// 的幂等键：合作方重试同一个round永远安全，不会把已经推进到下一轮的账户又结束一次
	Round uint64 `json:"round"`
}
