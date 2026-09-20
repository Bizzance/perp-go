# 代码阅读指南

这份文档不描述"当前代码的行为"（那是其它每个文档各自的职责），而是回答一个更实际的
问题： **第一次看这个代码库，从哪进去、按什么顺序看、看到什么该联想到哪篇文档**。目标
是帮你在开始测试之前，对整个系统的关键设计有一张可以在脑子里调用的地图。

## 建议阅读顺序

不建议按目录结构从上到下顺序看（`internal/`下十几个包互相调用，孤立看一个文件很难
建立全局感觉）。建议按下面几步来：

1. **先看一遍 [architecture.md](architecture.md)**：两个进程、为什么这么分、目录结构
   总览。这是唯一一篇"看完就该有全局图"的文档，其它文档都是往这张图上补细节。
2. **看`internal/model/model.go`**：全部核心数据结构（`Account`/`Position`/`Order`/
   `ConditionalOrder`/`Coin`/`RiskLimitTier`/`Trade`/`Kline`/`InsuranceFund`/
   `FundingRateRecord`）加上各种枚举（`Side`/`OrderAction`/`OrderStatus`/
   `PositionStatus`/`TriggerDirection`/`ConditionalOrderStatus`）全部收在这一个文件里，
   200多行，十分钟能看完。后面看任何业务逻辑，字段名对不上就回来查这里，不用满仓库
   搜索。
3. **跟着"下单→撮合→结算→推送"这一条主线走一遍代码**（见下面"一次下单的完整代码路径"），
   不追求看懂每一行，先建立"一个请求会依次经过哪些文件"的感觉。
4. **按下面"贯穿全代码库的设计模式"逐个理解**——这些模式在多个文件里反复出现，理解了
   模式本身，看具体某个repo方法就不会觉得"为什么这么写、这个UPDATE语句在干嘛"。
5. **需要深入某个子系统时，查下面"文档-代码对照表"，直接跳到对应的repo/service文件**，
   不用整个`internal/`重新翻一遍。
6. **最后看 [known-limitations.md](known-limitations.md)**：哪些"看起来像bug"的东西
   其实是明确的设计取舍（比如全仓不支持逐仓、`0=不限制`的约定、`uid`是独立参数而不是从鉴权里取），
   免得测试时把已知的MVP简化当成新发现的问题。

## 代码分层一览

```
cmd/
  contract-api/main.go      contract-api进程入口：接线路由、repo、service，起Gin+WS Hub
  contract-engine/main.go   contract-engine进程入口：接线撮合引擎、消费Kafka、起3个定时任务

internal/
  api/          HTTP handler层。router.go是核心交易链路（下单/撤单/条件单/杠杆/账户）的
                实现，共享同一套校验辅助函数；extra.go是合作方对接用的查询类/批量类接口
                （合约信息、行情、单笔委托查询、批量撤单、资金流水、强平记录）；errors.go
                是错误码常量和统一的失败响应；params.go是分页参数和requestId校验；
                engine_server.go是contract-engine自己暴露的/depth查询接口；ws_server.go是
                WS升级入口
  service/      业务逻辑层，每个文件对应一个子系统（见下面对照表），这是整个代码库的
                核心，也是本次全量review改动最集中的地方
  repo/         数据访问层，一个repo对应一张表，只做SQL、不含业务判断——业务规则在
                service层，repo层的方法名应该都是"做一件具体的原子数据库操作"
  matching/     订单簿撮合引擎（book.go），不碰数据库、不碰账户，纯内存数据结构+撮合算法
  model/        领域模型struct，全部子系统共用
  ws/           contract-api侧WS网关：hub.go管"频道-连接"路由，client.go管单个连接的
                读写goroutine
  pubsub/       WS推送用的Redis channel命名规则，发布端(service/push.go)和订阅端
                (ws/hub.go)共用同一份，见websocket.md解释过的"两边各自维护一份前缀
                容易出bug"教训
  cache/        Redis封装：标记价格、指数价格、资金费率采样累加器、WS推送的Pub/Sub
  mq/           Kafka生产者/消费者封装，含消息级去重（WithDedup）
  config/       环境变量加载，全部可调参数（扫描间隔、超时阈值、node id等）集中在这里
  db/           MySQL连接
  events/       Kafka消息体struct（下单/撤单/结束本轮三种事件）

deploy/         容器化部署：Dockerfile、docker-compose.yml(+deps叠加层)、环境变量模板，见deployment.md
```

## 一次下单的完整代码路径

以`POST /order/add`一笔LIMIT开仓单为例，从HTTP进来到最终推送给WS客户端：

1. `internal/api/router.go`的`addOrder`：校验账户存在（`loadAccount`）、参数、`requestId`幂等预检
   （`idempotencyConflict`识别同一个键带了不同参数）、查`coins`表拿合约配置、算价格保护带、
   按保守参考价估算`requiredMargin`（SHORT+OPEN用`max(委托价,标记价)`，见
   [matching-and-settlement.md](matching-and-settlement.md)"冻结保证金的保守估计"）
2. 保证金分档校验：`LockService.WithLock`按`uid+symbol+side`拿分布式锁，锁内读现有
   仓位/挂单算已占用名义价值、`PositionService.TierFor`查分档、校验杠杆上限——这段
   临界区为什么要锁见 [risk-limit-tiers.md](risk-limit-tiers.md)
3. `AccountService.FreezeMargin`：四级路径冻结保证金（见下面"四级冻结路径"），返回
   `FromAvailable`/`FromCredit`两部分
4. `OrderRepo.Insert`落库，`status='open'`，带`request_id`/`request_hash`；撞`(uid, request_id)`
   唯一索引说明是并发的重复请求，退回刚冻结的保证金、按重复请求处理
5. `contract-api`通过`mq.Producer`发一条事件到Kafka（`perpgo.order.submit`）
6. `contract-engine`的消费循环（`cmd/contract-engine/main.go`）用`mq.WithDedup`包一层
   去重，调用`EngineService.HandleOrderSubmit`（`internal/service/consumer_handlers.go`，
   解析事件、按`orderId`查库、交给`SubmitOrder`；撤单和结束本轮同理是`HandleOrderCancel`、
   `HandleRoundClose`，提成方法是为了能不起Kafka直接拿消息做集成测试）
7. `SubmitOrder`：从`matching.Engine.BookFor(symbol)`拿到这个symbol的订单簿，
   `Book.Match`尝试撮合，产生`[]Fill`；没吃完的部分`Book.Rest`挂回簿子
8. 每笔`Fill`调用`settleOneFill`：maker/taker分别`OrderRepo.ApplyFill`更新委托状态、
   `SettlementService.SettleFill`结算保证金/已实现盈亏/手续费（"多退少补"逻辑见
   [matching-and-settlement.md](matching-and-settlement.md)），两边独立处理、一方失败
   不连累另一方
9. `PushService`把这次涉及到的深度快照、成交、K线、标记价格、每个touched uid的账户
   快照发布到Redis Pub/Sub（`internal/pubsub`定的channel名）
10. `contract-api`的`internal/ws.Hub`如果有本地连接订阅了对应频道，转发给WS客户端
    （见 [websocket.md](websocket.md)）

强平（`LiquidationService`）、条件单触发（`ConditionalOrderService`）走的是同一条
`SubmitOrder`撮合路径，只是委托是系统自己生成、提交时机由风控扫描/条件单扫描决定，
不是用户直接调`POST /order/add`。

## 贯穿全代码库的设计模式

这几个模式不是某一个文件独有的，理解了模式本身，看任何一处具体实现都会更快：

### 1. 全仓保证金四级冻结路径

`AccountService.FreezeMargin`（account.go）：`available`够→不够就`available+credit`
（`FreezeSpillToCredit`）→不够就`available+credit+全部持仓未实现盈亏`（
`FreezeForceIntoNegative`，允许`available`变负）→都不够就拒绝。详见
[account-and-margin.md](account-and-margin.md)。

### 2. CAS原子更新，不用悲观锁/事务

账户余额、持仓、委托的并发写保护统一用"`UPDATE ... WHERE 字段=读到的旧值`，
`RowsAffected()==0`就重读重试"这一套，不用`SELECT ... FOR UPDATE`或应用层mutex。
代表实现：`PositionRepo.ApplyOpenFill`/`ApplyCloseFill`、`OrderRepo.ApplyFill`、
`AccountRepo.FreezeForceIntoNegative`、`AccountRepo.CloseRoundIfRound`。看这类函数时
留意它们的通用形状：`for { 读 -> 算新值 -> UPDATE...WHERE带旧值守卫 -> 0行就continue重试 }`。

### 3. MySQL会话变量捕获技巧

同一条`UPDATE`语句里既要写新值、又要拿到写之前的旧值时（比如`FreezeSpillToCredit`要
知道这次冻结分别从`available`/`credit`各扣了多少、`CloseRoundIfRound`要知道清零前
`credit`是多少），用`col = (@var := 旧值表达式) - ...`这种写法在一条UPDATE里同时完成
计算和捕获，紧接着`SELECT @var`读出来。 **必须用同一条`sql.Conn`执行UPDATE和SELECT**——
会话变量是连接级别状态，连接池里随便拿一条连接执行SELECT可能读到别的会话的值。
代表实现：`AccountRepo.FreezeSpillToCredit`、`AccountRepo.CloseRoundIfRound`。

### 4. 分布式锁保护"读多个状态再决策"的复合临界区

CAS解决的是单次UPDATE的原子性，但"读现有仓位+挂单→算分档→决定冻结多少"这种跨多次
读写的复合决策没法只靠CAS。`LockService.WithLock`（lock.go，Redis`SETNX`+TTL）按
`uid+symbol+side`序列化这段临界区——用分布式锁而不是进程内mutex，是因为`contract-api`
无状态、可以多实例部署，进程内锁挡不住跨实例的并发。详见
[risk-limit-tiers.md](risk-limit-tiers.md)。

### 5. 订单簿：价格档位数组二分 + 档位内FIFO链表 + orderID→节点的map

`matching/book.go`的`Book`结构：`bids`/`asks`各自是按价格排序的`[]*priceLevel`切片
（二分查找定位档位），每个`priceLevel`内部是时间优先的双向链表（`orderNode`），另外
维护一个`orderID -> *orderNode`的map做O (1)撤单。`Match`包含自成交保护：撞上同一个uid
的挂单会把那笔挂单摘掉当撤单处理，不会真的成交。详见 [order-book.md](order-book.md)。

### 6. 强平：原子guard + 异步分批 + 超时兜底 + ADL兜底

`LiquidationService.queueLiquidation`用`MarkLiquidating`的`WHERE status='normal'`
做一次性原子guard，成功后丢给独立goroutine`liquidateInClips`异步循环处理，每一批不
超过`coin.MaxVolume`，挂保护价单排队，超时（`LiquidationOrderTimeoutMs`）还没成交完
就直接按标记价结算兜底。结算后穿仓由保险基金垫付，基金不够时`runADL`（adl.go）强制
减仓对手方最赚钱的仓位补缺口。详见 [liquidation.md](liquidation.md)——这也是本次
review发现最严重bug（`ApplyCloseFill`覆盖`liquidating`标记）的地方，读的时候留意
`positions.status`在整个强平流程期间必须保持不变这个不变量。

### 7. 资金费率：先落审计记录再转账 + Lua脚本原子累加

`FundingService.SampleOnce`定时把溢价率累加进Redis（`AccumulateFundingSample`用Lua
脚本把`INCRBYFLOAT`+`INCR`包成一次原子操作，多个engine分片实例并发采样也不丢样本）；
`SettleIfDue`结算时 **先**插入`funding_rate_history`审计记录、 **再**转账，划转到一半
失败也不会导致重复结算。详见 [funding-rate.md](funding-rate.md)。

### 8. Kafka消息级去重

`internal/mq.WithDedup`按消息的`(topic, partition, offset)`坐标查/写
`processed_messages`表，包在下单/撤单/结束本轮三个消费者外面，跟业务字段无关、对
at-least-once语义下的重复投递统一兜底。详见 [message-dedup.md](message-dedup.md)。

### 9. engine横向扩展：静态分片 + fan-out消费

`PERP_ENGINE_SYMBOLS`环境变量配置每个实例负责哪些symbol，`EngineService.OwnsSymbol`
在处理每个事件前判断"这个symbol归不归我管"。不依赖Kafka原生分区负载均衡（历史上在
同一个group id挂多个异构订阅上踩过分区分配失效的坑，见
[known-limitations.md](known-limitations.md)），而是每个实例用自己独立的consumer
group id做fan-out、应用层按symbol过滤。详见 [engine-sharding.md](engine-sharding.md)。

### 10. WebSocket推送：Redis Pub/Sub解耦 + Hub懒订阅

`contract-engine`是唯一权威数据源，状态变化时`PushService`（push.go）往Redis
`PUBLISH`；`contract-api`的`ws.Hub`按需`SUBSCRIBE`（引用计数懒订阅：第一个客户端
订阅某频道才真正去Redis订阅，最后一个退订才取消），转发给本地WS连接。发布点是少数
几个顶层编排函数（`SubmitOrder`/`CancelOrder`/`CloseRound`/`queueLiquidation`等），
不是几十个底层repo方法各自插入。详见 [websocket.md](websocket.md)。

### 11. 接口幂等：`requestId`唯一索引 + 请求指纹 + 事务

会改变资金或状态的写接口靠客户端传的幂等键防重复：`(uid, request_id)`唯一索引占位，`request_hash`
（`service.RequestFingerprint`）识别"同一个键带了不同参数"，资金类接口把"写流水+改余额"放进同一个事务
（`AccountRepo.ApplyFundOp`）。结束本轮用`round`当天然的幂等键，引擎侧还有权威判断。详见
[idempotency.md](idempotency.md)。

### 12. 雪花算法ID生成

`idgen.go`的`NextID`：41位时间戳+10位node id+12位序列号，`PERP_NODE_ID`区分实例。
`contract-api`/`contract-engine`默认node id分别是0/1。这个文件本身踩过几次并发/时钟
回拨的坑，注释里记录得比较详细，值得完整看一遍。

## 文档-代码对照表

| 想看哪个子系统            | 先看这篇文档                                                           | 核心代码                                                                                                     |
|---------------------------|------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|
| 账户/保证金/信用额度/轮次 | [account-and-margin.md](account-and-margin.md)                         | `service/account.go`, `repo/account_repo.go`                                                                 |
| 下单校验/撮合/结算        | [matching-and-settlement.md](matching-and-settlement.md)               | `service/engine.go`, `service/settlement.go`, `api/router.go`的`addOrder`                                    |
| 订单簿数据结构            | [order-book.md](order-book.md)                                         | `matching/book.go`                                                                                           |
| 进程重启恢复挂单          | [order-book-recovery.md](order-book-recovery.md)                       | `service/engine.go`的`RecoverOrderBook`                                                                      |
| 持仓/杠杆/保证金分档      | [risk-limit-tiers.md](risk-limit-tiers.md), [leverage.md](leverage.md) | `service/position.go`, `api/router.go`的`setLeverage`                                                        |
| 条件单（止盈止损）        | [conditional-orders.md](conditional-orders.md)                         | `service/conditional_order.go`, `repo/conditional_order_repo.go`                                             |
| 强平/保险基金/ADL         | [liquidation.md](liquidation.md)                                       | `service/liquidation.go`, `service/adl.go`, `service/insurancefund.go`                                       |
| 资金费率                  | [funding-rate.md](funding-rate.md)                                     | `service/funding.go`                                                                                         |
| K线                       | [kline.md](kline.md)                                                   | `service/kline.go`, `repo/kline_repo.go`                                                                     |
| engine分片                | [engine-sharding.md](engine-sharding.md)                               | `cmd/contract-engine/main.go`, `service/engine.go`的`OwnsSymbol`                                             |
| Kafka消息去重             | [message-dedup.md](message-dedup.md)                                   | `internal/mq/mq.go`                                                                                          |
| WebSocket推送             | [websocket.md](websocket.md)                                           | `service/push.go`, `internal/ws/`, `internal/pubsub/`                                                        |
| HTTP接口清单（对接文档）  | [api.md](api.md)                                                       | `internal/api/router.go`, `internal/api/extra.go`, `internal/api/errors.go`, `internal/api/engine_server.go` |
| 接口鉴权(API Key+HMAC)    | [auth-design.md](auth-design.md)                                       | （无对应代码）                                                                                               |
| 已知限制/明确排除项       | [known-limitations.md](known-limitations.md)                           | （文档性质，无对应代码）                                                                                     |

## 看代码时容易疑惑、但其实是既定设计的几个点

- **`uid`是独立的请求参数，不是从鉴权里取的**：鉴权（`internal/api/auth.go`）只证明"请求来自哪把密钥、
  有没有权限"，不告诉我们`uid`是谁。合作方是服务端，终端用户的身份由它自己负责，我们信任它传来的`uid`。
  所以handler里都是自己解析`uid`再`requireAccount`/`loadAccount`，不要去找"当前登录用户"。
- **账户必须先创建，API层不会自动建**：`router.go`里的`requireAccount`/`parseAccountUID`统一校验，
  没创建返回`account_not_found`。但service/repo内部仍有`GetOrCreate`——那是给成交结算、强平这类
  "账户一定存在"的内部流程用的，不要在新的对外接口里用它。
- **账户冻结（`status=frozen`）拦新增风险、不拦降低风险**：API层`rejectIfFrozen`拦下单/条件单/改杠杆，
  引擎层`submitOrder`撮合前再兜底一次（撤单退款）。别跟挂单的"冻结保证金"`frozen_margin`搞混，见
  [account-and-margin.md](account-and-margin.md)"账户状态"。
- **错误响应有`code`和`errCode`两层**：`code`是粗粒度的400/429/500，`errCode`是稳定的机器
  可读小写下划线错误码（`insufficient_margin`），定义在`internal/api/errors.go`。新增有业务
  含义的失败要用`failC`指定`errCode`，只有没有专门含义的才用`fail`走兜底。
- **只有全仓，没有逐仓**：`CLAUDE.md`项目说明里的范围决策，不是缺功能。
- **很多`coins`表字段"0=不限制"**：`max_volume`/`price_tick`/`volume_step`/
  `funding_rate_cap`都是这个约定，看到代码里"等于0就跳过这个校验"不要误判成bug。
- **`repo`层方法名字看起来像在做业务判断（比如`FreezeSpillToCredit`）**：这是因为
  这个系统的原子性保证下沉到了SQL语句本身（模式2），repo方法不是薄封装，方法名对应
  的是"一个具体的原子资金动作"，业务层（service）只负责按顺序调用、判断返回值。
- **多处"这一步失败就把状态撤回原样、交给下一轮定时任务重试"**：强平、条件单触发、
  结束本轮都是这个思路，不是漏了错误处理——重试同一批大概率还是失败，交给下一个
  独立的扫描周期用全新状态重新判断，比在当前调用栈里死循环重试更稳妥。
