# engine横向扩展（按symbol静态分片）

## 背景：为什么不能简单地"多起几个进程"

`contract-engine`的核心状态——内存订单簿（`matching.Engine`里每个symbol一个`Book`）——是
进程私有的，多个进程之间没有共享。直接多起几个`contract-engine`进程、让Kafka按分区把
下单/撤单事件随便分给其中一个处理，会导致同一个symbol的订单簿在多个进程里各自维护一份
互不知道的副本，正确性彻底失控。要让多个实例安全地分摊撮合负载，必须保证 **同一个symbol
在任意时刻只有一个实例在真正维护它的订单簿**——这是本文档描述的分片方案要解决的核心问题。

## 方案：静态配置分片，不依赖Kafka原生分区负载均衡

每个`contract-engine`实例通过`PERP_ENGINE_SYMBOLS`环境变量（逗号分隔，如
`BTCUSDT,ETHUSDT`）显式声明自己负责撮合哪些symbol。不设这个变量=负责全部symbol，这是
单实例部署的默认行为，不需要任何额外配置，行为跟分片之前完全一样（`EngineService.
ownedSymbols`为nil时`OwnsSymbol`恒返回true）。

**为什么是静态配置，不是Kafka多分区+consumer group自动rebalance**：这个项目历史上已经
在Kafka consumer group的异构订阅场景上踩过一次隐蔽的坑——同一个group id挂3个订阅不同
topic的member，broker端分区分配"看起来"完成了，但member实际收不到任何分区，消费彻底
卡住（见`known-limitations.md`"已经修复的历史问题"）。在没有更多实测验证之前，不值得把
"多实例负载分摊"这件事的正确性押在一个已经出过问题的机制上。静态配置牺牲了"加减实例自动
重新分配"的便利性，换来的是路由逻辑完全在应用层、可预测、可单元验证。

## fan-out消费 + 应用层过滤，不是Kafka分区路由

每个实例对`order.submit`/`order.cancel`/`round.close`这三个topic都用 **自己独立的
consumer group id**（`cmd/contract-engine/main.go`的`consumerGroupID`函数，未分片时
沿用原有的固定group id、不产生任何行为变化；分片开启后按`PERP_NODE_ID`拼出`"contract-
engine-{NodeID}"`这样的独立group id）。Kafka里不同consumer group之间是完全独立的——
每个group都会拿到topic的 **完整**消息流 (fan-out)，不是Kafka原生的"同一个group内多个
consumer分摊不同分区"那种负载均衡。

每个实例收到消息后，在真正执行撮合/撤单之前，先用`EngineService.OwnsSymbol(symbol)`
判断这个symbol归不归自己管：不归自己管就直接跳过（不碰订单簿，不做任何DB变更），让
真正拥有这个symbol的那个实例（同样fan-out收到了这条消息）去处理。这意味着：

- 每个实例上，本地`matching.Engine`里只有它自己负责的symbol才有真实、准确的订单簿状态；
  不归它管的symbol，`BookFor`会创建一个空Book，但从来不会被真正写入任何数据（`SubmitOrder`/
  `CancelOrder`入口的`OwnsSymbol`检查会在碰订单簿之前就返回）。
- 消息级去重（`processed_messages`表，见 [message-dedup.md](message-dedup.md)）的主键
  加了`consumer_group`列——因为现在同一条消息会被多个实例的独立consumer group各自fan-out
  消费一次，去重必须按"这个consumer group有没有处理过"分别判断，不能用一个全局共享的
  去重状态，不然先处理到的那个实例会把其它本来也需要独立处理的实例挡住。

**Kafka broker侧的代价**：每条消息会被读取N次（N=分片实例数），不是1次——这是fan-out
设计有意识的取舍，用broker侧读放大换取"路由逻辑完全在应用层、不依赖Kafka rebalance"的
确定性。这个项目现阶段的吞吐量级完全负担得起这点读放大，不是需要优化的瓶颈。

## 三类操作在分片下的处理方式不一样

### 1. 下单/撤单（`SubmitOrder`/`CancelOrder`）——纯粹的symbol级过滤

这两个函数入口就是`OwnsSymbol`检查，不归自己管直接`return nil`。 **这个检查必须在碰
`e.matchingEngine.BookFor(symbol)`之前**——`CancelOrder`原有的实现对"订单簿里找不到
这个orderId"是容忍的（`book.Cancel`返回`ok=false`时退回`o.RemainingAmount()`继续走
`finalizeOrderCancel`，这是为了容忍"已经被自成交保护摘掉"这种正常场景），如果没有
`OwnsSymbol`过滤，一个不拥有这个symbol的实例会把它 **从来没有真实挂过**的订单，误判成
"已经不在簿子上了"，直接标记CANCELED、退保证金——而真正拥有这个symbol的实例里这笔委托
可能还在真实排队，两边状态就对不上了。这是分片设计里最容易踩的坑，加`OwnsSymbol`检查
就是专门堵这个。

`RecoverOrderBook`（进程重启后从MySQL重建订单簿，见
[order-book-recovery.md](order-book-recovery.md)）同样加了`OwnsSymbol`过滤——不归自己
管的symbol不恢复，不然会在本地攒一份永远用不上、也永远不会被真实操作更新的"僵尸"订单簿
副本，还可能误导查询到这个实例`/depth`接口的调用方。

### 2. 强平（`RiskScanOnce`）、条件单触发（`ScanOnce`）——决策全局，动作按symbol过滤

这套系统是全仓保证金，一个uid该不该被强平，天然要看他名下 **全部symbol**的仓位合计
（`checkAndLiquidate`里的`equity`/`maintainTotal`），没法只让某一个分片去算。所以这
两个后台扫描 **不做实例级别的开关**，每个实例都独立跑一遍完整扫描（读全部uid的全部
仓位/条件单）——这部分是纯DB读，多个实例各自重复扫一遍只是浪费一点DB查询，不是正确性
问题。

真正需要按symbol过滤的是 **动作**：`checkAndLiquidate`决定"这个仓位需要强平"之后，只有
`OwnsSymbol(p.Symbol)`为true才会调用`queueLiquidation`挂出真正的强平单；`ScanOnce`
判断"这个条件单该触发了"之后，只有`OwnsSymbol(co.Symbol)`为true才会调用`trigger`。
这两处的原子状态转换（`MarkLiquidating`/`MarkTriggered`）都是"一次性"的——如果不做这层
过滤，一个不拥有该symbol的实例会抢先把这个一次性的状态转换用掉，但它自己的`SubmitOrder`
调用会被`OwnsSymbol`挡住、什么都不做，而真正拥有这个symbol的实例的扫描会因为状态已经
不是"待触发"而跳过，导致这笔强平/触发 **永久卡死、没有任何实例会再处理它**——这是分片
设计里第二容易踩的坑，比"CancelOrder误判"更隐蔽，因为不会立刻报错，只会在DB里留下一笔
永远停在中间状态的记录。

资金费率结算（`FundingService.SettleIfDue`）不需要任何分片相关的改动——它的"结算记录
先落一条UNIQUE KEY(symbol,funding_time)占坑再转账"这个既有设计（见
[funding-rate.md](funding-rate.md)）天然对"多个实例冗余调用"是安全的：谁先插入成功谁
负责这笔结算，另一个会撞唯一约束失败、直接跳过，不会重复转账。

采样（`SampleOnce`/`AccumulateFundingSample`）**理论上**也应该对多个实例冗余采样安全——
最终取的是"累加值/次数"的均值，多份重复采样只是提高采样密度、不改变均值本身，前提是
"累加"这个动作本身是原子的。这个前提最初被想当然地当成已经满足（分片工作刚完成时这里
写的是"完全不需要任何分片相关的改动"），但`AccumulateFundingSample`当时实际是"GET当前
值→在Go里算新值→SET回去"这种非原子读改写，多个实例并发调用时后写的会把先写的那次采样
静默覆盖掉——这不是"密度变高"，是真的丢样本。code review发现后改成了Redis原生
`INCRBYFLOAT`/`INCR`包在一个Lua脚本里原子执行（见`internal/cache/cache.go`），现在
"多实例冗余采样安全"这个结论才真正成立，不是靠假设。这个教训值得记住：**"这个操作本身
是原子的"这句话，要去看实现，不能只看调用方式看起来简单就假设成立**。

### 3. 结束本轮（`CloseRound`）——需要跨分片协调，唯一真正复杂的部分

结束本轮要撤销一个uid **名下全部symbol**的挂单/条件单、强平全部仓位，最后才能清零
credit、推进round——"全部完成才清算"这个收尾动作的原子性，天然跨越了分片边界，前面两类
操作都不需要的 **跨进程协调机制**，只有这里需要。

**流程**（`EngineService.CloseRound`，round.close事件fan-out给每个实例）：

1. 每个实例收到事件后，先查这个uid当前 (activeOrders + 活跃条件单 + 持仓)涉及到的全部
   symbol并集，往`round_close_progress`表（`uid, round, symbol, done`）用`INSERT
   IGNORE`给每个symbol占一行坑（`done=0`）——多个实例同时做这一步是安全的，只有第一个
   真正插入成功，其它都是无害的no-op。
2. 每个实例只处理自己`OwnsSymbol`的那些symbol：撤那个symbol上的挂单+条件单、强平那个
   symbol上的仓位，全部成功才把对应的progress行`MarkDone`。
3. 每次处理完（不管这次自己实际有没有事情要做），检查这个`(uid, round)`下全部symbol
   是不是都`done`了。 **还没全部完成不是错误**——大概率是负责其它symbol的实例还没轮到
   处理这个事件（fan-out消费不保证同时到达），直接返回nil，不打ERROR日志，等其它实例
   做完自己那部分后会各自再检查一次。
4. 全部完成后，用`LockService`（Redis `SETNX`，见
   [risk-limit-tiers.md](risk-limit-tiers.md#并发下单的原子性按uidsymbolside的分布式锁)
   同一个组件）抢一把`perpgo:lock:closeround:{uid}:{round}`锁，锁内再确认一遍全部完成，
   然后调用`AccountService.CloseRound(ctx, uid, round)`——这个方法现在多了一个"当前round
   必须还等于传入的round"的原子条件（`UPDATE ... WHERE id=? AND round=?`），双重保护
   "即使多个实例同时观察到全部完成"也只有一个真正执行清算：锁负责挡掉绝大多数并发场景，
   round原子条件是防御性的第二层（万一锁失效/过期时机不对，多写一次也不会把round推进
   两次）。结算成功后清理掉这个`(uid, round)`的全部progress行，避免表无限增长。

**单实例部署（没配`PERP_ENGINE_SYMBOLS`）下**，这整套流程照常运行，只是：`OwnsSymbol`
恒为true，所以第一次处理就会把全部symbol的份内工作做完，`AllDone`检查在同一次调用里
就会通过，直接进入最终结算——效果上跟分片之前的"一次性同步跑完"完全一样，只是多了几次
progress表的读写（可以忽略不计的开销）。

## 实测验证

本地用两个真实`contract-engine`进程（`PERP_ENGINE_SYMBOLS=BTCUSDT`+`PERP_NODE_ID=201`+
`:7010`，`PERP_ENGINE_SYMBOLS=ETHUSDT`+`PERP_NODE_ID=202`+`:7011`）连同一套MySQL/Redis/
Kafka跑过完整验证：

- 分别在两个symbol下单，`/depth`在各自负责的实例上能查到真实深度，在不负责的实例上被
  明确拒绝（`"这个实例不负责该symbol的撮合，请求路由到正确的分片"`），不是返回一个
  误导性的空深度。
- 消息去重表 (`processed_messages`)确认两个实例各自用自己的consumer group id (`contract-engine-201`/
  `contract-engine-202`)独立记录了同一个offset的处理状态，
  fan-out设计按预期工作。
- 同一个uid在两个symbol上各挂一笔单后调用结束本轮：两个实例分别撤掉了各自负责的那笔
  委托，`round_close_progress`表在最终结算完成后被正确清空，`account.round`只推进了
  一次（不是两次），日志里"结束本轮完成"只在 **一个**实例上出现（另一个实例的
  `tryFinalizeCloseRound`要么没抢到锁、要么重新检查时发现round已经被推进过，安静地
  no-op）。
- 顺带因为这次改动引入了全新的consumer group id，两个新实例第一次启动时把
  `round.close`这个topic从头重新消费了一遍（Kafka对全新group id默认从最早的offset开始）
  ——这次意外的完整重放验证了一个重要的性质：`AccountService.CloseRound`的round原子
  条件在 **几十条历史事件被两个实例重复重放**的情况下，依然把每个uid的round正确地
  一次一次推进，没有任何一次重复推进或者卡死，没有报错。
- 单实例默认模式（不设`PERP_ENGINE_SYMBOLS`）下完整走了一遍下单→查深度→结束本轮，
  行为跟这次改动之前完全一致，确认了向后兼容。

## 仍然没做的

- **client侧路由**：谁该往哪个实例的`/depth`发请求、哪个symbol的下单事件理论上"更快"
  被处理，完全是运维知道的静态信息，这次没有做一个统一的网关/路由层替调用方决定——需要
  调用方自己知道symbol到实例的映射（或者由运维在`/depth`前面搭一层按symbol路由的反向
  代理）。`POST /order/add`这类写接口不受影响，因为它们始终打`contract-api`这一个入口，
  分片路由完全在`contract-api`到`contract-engine`之间的Kafka层发生，调用方无感知。
- **动态重分片**：symbol到实例的映射完全靠部署时的`PERP_ENGINE_SYMBOLS`配置，加/减
  实例、给某个实例挪一批symbol，都需要运维手动改配置+重启，没有自动发现/自动重新平衡
  机制。
- **单个symbol内部再分片**：这次的分片粒度是"一个symbol只能整体属于一个实例"，没有
  "一个特别热门的symbol自己再拆成多个分区"这类更细粒度的方案——目前的流量量级下一个
  symbol的撮合负载本身还远没有到需要再拆分的地步。
