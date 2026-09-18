# WebSocket实时推送

## 架构：Redis Pub/Sub解耦`contract-engine`（数据源）和`contract-api`（WS网关）

`contract-engine`持有全部会实时变化的状态（订单簿、成交、K线、标记价格，以及资金/持仓/
委托变化的结果），但WS客户端连的是`contract-api`。两者之间用Redis Pub/Sub解耦：
`contract-engine`状态变化时`PUBLISH`到约定好的channel（`internal/service/push.go`的
`PushService`），`contract-api`的`internal/ws.Hub`按需`SUBSCRIBE`这些channel、转发给
订阅了对应频道的WS客户端。

选这个架构而不是让`contract-api`自己也维护一份订单簿/账户状态，理由跟`GET /depth`当初
放在`contract-engine`是同一个道理——不引入跨进程状态同步的复杂度，`contract-engine`
已经是唯一权威数据源，只是多一步"顺手发布"。`github.com/redis/go-redis/v9`是现有依赖，
`internal/cache.Cache`新增了`Publish`/`Subscribe`两个方法。

## 事件发布点：少数几个顶层编排函数，不是每个底层资金操作

不是在`AccountRepo`/`PositionRepo`几十个底层方法里各自插入发布调用（那样blast radius
太大、容易漏、以后每加一个新的资金操作都要记得补），而是在**顶层编排函数执行完之后**
发布——这些函数本身就是"一次业务动作的完整收尾点"：

| 顶层函数 | 文件 | 发布内容 |
|---|---|---|
| `EngineService.SubmitOrder`（收尾时统一推，见下） | engine.go | 该symbol的深度快照(订单簿确实发生变化才推) + 涉及到的每个uid各一份账户快照 |
| `EngineService.CancelOrder`（真的从订单簿摘掉了东西才推深度） | engine.go | 深度快照 |
| `EngineService.finalizeOrderCancel`（`CancelOrder`和自成交保护共用的收尾函数） | engine.go | 该uid的账户快照 |
| `EngineService.CloseRound`（撤单/强平全部完成、credit清零round+1之后） | engine.go | 该uid的账户快照 |
| `LiquidationService.queueLiquidation`（`MarkLiquidating`成功/`ClearLiquidating`回退都推） | liquidation.go | 该uid的账户快照 |
| `LiquidationService.settleTimeoutFallback` | liquidation.go | 深度快照（摘掉剩余部分时）+ 该uid的账户快照(经`EngineService.SubmitOrder`间接推送) |

`SubmitOrder`的账户快照推送在函数末尾统一做，不是每笔成交各推一次：一笔大额市价单可能
一口气吃掉好几档、产生好几笔fill，中间几次快照都会被最后一次覆盖，白白多查DB多发Redis，
所以用一个`touchedUIDs`集合收集这次提交涉及到的全部uid（包括maker、taker、提交者
自己），成交结算完毕后每个uid只推一次。**提交者自己的uid也必须包含在内**，即使这笔
委托一笔成交都没吃到、只是静静挂在簿子上——它的出现本身就是提交者`activeOrders`列表的
变化，不能只在有成交时才推（早期实现有这个遗漏，一笔纯挂单不成交的委托，提交者的WS
私有频道永远收不到通知，直到某个不相关的事件恰好触发一次快照才会看到，已经用实测验证
修复：单独订阅`user:{uid}`、只挂一笔不吃到任何东西的限价单，确认能收到包含这笔新增
挂单的快照）。

强平相关的`ClearLiquidating`（判定不满足强平条件、把仓位状态从`liquidating`撤回
`normal`）之前只改DB、没有对应的推送——客户端已经收到过`liquidating`状态的快照，
服务端撤回之后不告诉客户端，会让客户端一直卡在一个已经不成立的风险状态上。现在
`ClearLiquidating`的两个调用点后面都补了一次`PublishUserSnapshot`。

账户快照（`PushService.PublishUserSnapshot(ctx, uid)`）是"重新查一遍这个uid当前的
account/positions/active orders、整体推送"，**不是增量diff**——推送快照职责单一、
不容易算错，复用现成的`AccountService.View`/`PositionService.Views`/
`OrderRepo.FindActiveByUID`，效果上等于"服务端主动帮你调用了一次GET /account/info +
GET /position/current + GET /order/current"。`positions`字段用的是
`PositionService.Views`（带`markPrice`/`unrealizedPnl`/`roe`/`notionalValue`/
`liquidationPrice`这些计算字段的`PositionView`），不是裸的`model.Position`——这个视图
类型现在收在`PositionService`里，REST的`GET /position/current`和WS推送共用同一份
计算逻辑，不会出现两处字段不对等的情况（早期实现WS这边直接用了裸`model.Position`，
比REST查询"缺胳膊少腿"，已经修复）。

## Kafka at-least-once重复投递的防护

完整的消息级去重（按Kafka的`(topic, partition, offset)`记一张"已处理"表，
`internal/mq.WithDedup`统一包一层，下单/撤单/结束本轮三个消费者都覆盖）见
[message-dedup.md](message-dedup.md)。`EngineService.SubmitOrder`入口另外还保留了
两层业务层面的针对性防护（委托状态检查+`Book.Contains`检查），两套机制互补：消息级
去重挡的是"同一条Kafka消息物理上被投递两次"，`SubmitOrder`这两层挡的是即使消息级去重
万一没生效（比如去重表本身故障降级放行），业务逻辑自己也不会对一笔已经终结/已经在排队
的委托重复处理。

条件单（止盈止损/条件开仓）的创建/撤销发生在`contract-api`，那边目前没有接入
`PushService`——冻结/释放保证金会影响下一次账户快照的内容，但不会主动触发一次推送，
是明确的、还没覆盖的范围（不是遗漏，是这次没有扩展到`contract-api`那一侧）。

## Redis channel命名 vs 客户端订阅名：有一层前缀转换

`contract-engine`发布到Redis的channel名字统一带`perpgo:ws:`前缀（比如
`perpgo:ws:depth:BTCUSDT`）——这个前缀是为了在共享的同一个Redis实例上跟其它系统的
pub/sub channel做命名隔离（这个项目历史上跟一个Java版本共用过Redis，见`contract:*`
那些遗留key）。客户端订阅时**不带这个前缀**（比如`depth:BTCUSDT`），`internal/ws.Hub`
在真正调Redis SUBSCRIBE时才拼上前缀——这一层转换必须做对，两个名字弄混会导致Hub订阅了
错误的Redis channel、看起来"连上了但永远收不到推送"，且没有任何报错（这正是开发过程中
第一次实测就踩到的bug：Hub直接把客户端给的名字当Redis channel名用，没有加前缀，导致
contract-engine发布到`perpgo:ws:depth:BTCUSDT`、Hub却在监听`depth:BTCUSDT`，两边对不上，
已经修复并用两个并发WS客户端+`PUBSUB NUMSUB`验证过）。

前缀+命名规则收在独立的`internal/pubsub`包里（`Prefix`常量+`DepthChannel`/`TradeChannel`/
`KlineChannel`/`MarkPriceChannel`/`UserChannel`几个命名函数），发布端(`push.go`)和
订阅端(`hub.go`)都调用这一份实现——不是两边各自维护一份前缀常量/拼接逻辑。这不是过度
设计：上面那个bug的根因正是"两边各自维护一份、其中一边漏了前缀"，把命名规则收进唯一的
一份实现之后，这类不一致在结构上不再可能发生（除非两边都改错成同一个错误的样子，概率
上跟"两处独立实现刚好各自都错"完全不是一回事）。

```
depth:{symbol}          公开，DepthSnapshot（内部实际频道 perpgo:ws:depth:{symbol}）
trade:{symbol}          公开，Trade
kline:{symbol}:{interval}  公开，Kline（六个周期各自独立的频道）
markprice:{symbol}      公开，{symbol, price}
user:{uid}              私有，{account, positions, activeOrders}三合一快照
```

## 订阅协议

```
GET /ws  (contract-api，默认端口:7001)
```

连上之后发JSON控制消息订阅/取消订阅：

```json
{"op": "subscribe", "channels": ["depth:BTCUSDT", "trade:BTCUSDT", "kline:BTCUSDT:1m", "user:10001"]}
{"op": "unsubscribe", "channels": ["depth:BTCUSDT"]}
```

推送给客户端的消息统一包一层：

```json
{"channel": "depth:BTCUSDT", "data": {...}}
```

私有频道`user:{uid}`延续现有REST接口"明文uid占位鉴权"的既定约定（`docs/api.md`已经
写明"MVP阶段鉴权用明文uid参数占位"）——订阅时直接给uid，不做token校验，任何人理论上
都能订阅任何uid的私有频道，后续统一换鉴权中间件时和REST接口一起换，不是这次的范围。

不认识的控制消息（`op`既不是`subscribe`也不是`unsubscribe`，或者JSON格式不对）直接
忽略，不会断开连接——容忍客户端偶尔发错格式。

## Hub的懒订阅（引用计数）

`internal/ws.Hub`管理"channel -> 订阅了它的本地WS连接集合"，用引用计数做懒订阅：第一个
客户端订阅某个channel时才真正去Redis SUBSCRIBE，最后一个客户端退订/断开时才
UNSUBSCRIBE——不会一次性订阅全部symbol×interval的组合，也不会因为多个本地WS客户端订阅
同一个channel就开多个Redis连接（已经用两个并发客户端订阅同一个depth频道验证过，
`PUBSUB NUMSUB`显示`contract-api`只对这个channel开了1个Redis订阅，推送消息正确广播给
两个客户端）。

Hub内部只有一个`run()` goroutine串行处理全部订阅/退订/客户端移除/广播操作，不是拿锁
保护`channels`/`cancels`这两个map——从Redis收到消息的中继goroutine不直接读map做广播，
而是把"广播"也封装成一个操作丢进同一个`incoming`队列，这样"谁订阅了什么"和"该给谁
广播"全部由`run()`这一个goroutine串行处理，天然没有数据竞争，不需要额外加锁。

## 连接管理（`internal/ws/client.go`）

标准`gorilla/websocket`读写两个goroutine模式：读goroutine(`readPump`)处理客户端发来的
订阅/取消订阅控制消息、检测断连；写goroutine(`writePump`)把Hub转发过来的消息写给客户端，
带定时ping心跳（`pingPeriod`=54秒，小于`pongWait`=60秒，保证在对方判定超时之前把下一个
ping发出去）。单个连接的发送队列（`sendBufferSize`=256）满了就丢弃新消息，不阻塞
Hub的广播循环——一个处理不过来的慢客户端不能拖慢所有人，深度/成交这类高频公开频道丢一条
影响不大，账户快照丢一条相对麻烦，但客户端本来就该定期用REST接口校准，不能假设WS推送
绝对不丢。

`upgrader.CheckOrigin`恒返回true，没有做Origin校验——这个系统的鉴权本来就是MVP占位
（明文uid，不校验token/来源），等换成真实鉴权中间件时WS这边一起换。
