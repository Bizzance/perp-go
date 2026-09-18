# 订单簿恢复

## 问题：订单簿是纯内存结构，进程重启会丢

`internal/matching.Book`（见 [order-book.md](order-book.md)）从设计上就是纯内存的
价格档位数组+链表，没有任何自己的持久化——`contract-engine`进程重启（部署更新、崩溃、
运维操作）会丢失全部挂单排队状态。这不等于丢数据：委托本身早就落库了（`contract-api`
下单时同步落库+冻结保证金，见 [architecture.md](architecture.md)"为什么保证金冻结在
contract-api同步完成"），丢的只是"这笔委托当前还在排队"这个事实在内存订单簿里的体现——
MySQL的`orders`表里，这笔委托的`status`依然正确地是`open`/`partially_filled`，只是
`contract-engine`重启后已经不知道该把它放回订单簿的哪个位置了。

## 恢复：启动时从MySQL重建（`EngineService.RecoverOrderBook`）

`orders`表本身就包含了重建订单簿需要的全部信息——`status ∈ {open, partially_filled}`
说明这笔委托重启前还在排队，`RemainingAmount()`（`Amount - TradedAmount`）就是它当前
挂着的剩余量。`contract-engine`启动时（`cmd/contract-engine/main.go`，在Kafka消费者
开始处理新消息**之前**同步跑完）：

1. `OrderRepo.FindActiveLimitOrders`查全部`status ∈ {open, partially_filled}`且
   `type = 'limit'`的委托，按`create_time ASC, order_id ASC`排序——`order_id`是雪花
   算法生成、时间单调递增，在`create_time`（毫秒精度）不够细分同一毫秒内多笔委托的
   相对先后时兜底提供更细的顺序
2. 依次直接调用`Book.Rest(...)`把每一笔按顺序插回对应symbol的订单簿，**不经过
   `Book.Match`**

只查`type = 'limit'`：MARKET单不管成交与否都从不挂在订单簿上（缺对手盘的剩余量直接
终止在`open`/`partially_filled`状态、不排队，见
[known-limitations.md](known-limitations.md)里"MARKET单缺对手盘会永久停在未成交状态"
一节），恢复时如果不加这个过滤，会把这些本来就不该在簿子上的委托误挂上去。

### 为什么不能走`SubmitOrder`那条"先`Match`再`Rest`"的路径

`SubmitOrder`处理新进来的委托时，逻辑是"先尝试撮合、剩余部分再挂簿"——这对**新委托**
是对的，但恢复时要重建的是**已经存在、彼此之间没有成交关系的历史挂单**：重启前这些
委托各自静静挂在订单簿的不同价位上，互相之间早就确认过"暂时碰不上"（不然在重启之前
就已经被撮合掉了）。如果恢复时重新跑一遍`Match`，会把两笔本来就没有成交关系的历史挂单
错误地撮合出一笔真实世界从未发生过的成交——这是恢复逻辑必须绕开`Match`、直接`Rest`
的根本原因，不是可以偷懒省略的细节。

### `EntryTime`的量纲换算

`RestingOrder.EntryTime`在正常下单路径里是纳秒时间戳（`time.Now().UnixNano()`，见
`cmd/contract-engine/main.go`调用`SubmitOrder`的地方），但`orders`表的`create_time`
是毫秒时间戳（`service.NowMillis()`）。恢复时不能直接把毫秒数值当纳秒数值用——数值上
毫秒时间戳远小于纳秒时间戳，如果不换算，恢复的挂单在时间优先级上会"恰好"排在重启后
新提交的委托之前，看似结果对了，但这是两种时间戳量纲偶然不同数量级的巧合，不是有意为之
的保证，一旦哪天改了时间戳精度就会静默失效。`RecoverOrderBook`显式做了换算
（`o.CreateTime * int64(time.Millisecond)`），让恢复的挂单和运行时新提交的委托的
`EntryTime`处在同一个量纲（纳秒）里可比较，这样"恢复的挂单排在重启后新委托之前"是这次
换算保证的结果，不是数量级碰巧的副作用。

## 失败处理：启动直接退出，不带着半成品订单簿硬起来

`RecoverOrderBook`失败（比如恢复期间MySQL不可用）会让`contract-engine`直接
`log.Fatalf`退出，不是打个警告日志、带着一个空的/不完整的订单簿继续启动——如果放过
这种失败，后续真实的撮合会用一个残缺的订单簿跑，该撮合到的历史挂单凭空消失，产出
经济上错误的结果（比如一笔新的吃单本该跟某个历史挂单成交，因为那笔历史挂单没有被恢复
回订单簿，吃单会拿不到该有的对手盘，多余部分继续按新单处理），这比进程暂时起不来、
运维介入排查MySQL问题严重得多。

## 仍然没做的：横向扩展（按symbol分片到多个engine实例）

恢复机制解决的是"单实例重启后状态不丢"，不是"能跑多个engine实例分摊负载"。
`service.NextID`已经支持多实例部署时用`PERP_NODE_ID`区分不同实例（见
[known-limitations.md](known-limitations.md)"已经修复的历史问题"），但订单簿本身
按symbol分片到多个`contract-engine`实例、以及分片之间怎么路由Kafka消息的编排逻辑还
没做——横向扩展依然需要额外的工作，不是配一个环境变量就能启用，这次的范围只是"单实例
重启不丢状态"，不包含"多实例分摊负载"。
