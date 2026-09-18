# 已知限制与下一步计划

按影响程度排序，供后续迭代参考。

## 明确排除的功能（不是遗漏，是范围决策）

- **逐仓保证金模式**：目前只有全仓，一个仓位单独隔离保证金的逐仓模式没做。
- **"没有仓位时预先声明杠杆"的接口**：杠杆是每次下单时传的参数，没有"先设置这个合约的
  杠杆倍数、以后下单沿用它"这种独立接口——修改已有仓位杠杆的接口已经做了（`POST
  /position/leverage`，见 [leverage.md](leverage.md)），这里明确没做的只是"没有仓位时
  预先声明"这一种场景，理由见该文档"范围"一节。

## 需要留意的边缘情况

- **分档配置在仓位已开仓后被删除/改坏**：维持保证金贡献会变成0，但未实现盈亏依然正常
  计入账户权益，见 [risk-limit-tiers.md](risk-limit-tiers.md)最后一节。
- **一个symbol从没有过标记价格/指数价格**：价格保护带、开仓分档校验的价格保守估计
  （`max(委托价, 标记价)`）都依赖有参考价可用，全新symbol、也没人喂过指数价的情况下
  这些保护完全防不住极端报价。
- **`TierFor`按每个仓位单独查一次分档配置，没有按symbol去重/缓存**：`positionCurrent`
  查询接口、`MaintenanceMarginTotal`风控扫描都是"每个仓位一次DB查询"，而不是"每个不同
  symbol一次查询、结果在同一次请求/扫描内复用"。分档配置在一次请求/一次扫描周期内基本
  不会变化，这是可以优化但目前没做的性能点——账户持仓symbol数量不多的MVP阶段影响有限，
  持仓/扫描规模变大后需要重新评估。

## 已经修复的历史问题（记录一下，避免以后重新踩坑）

这些是开发过程中review发现并修复的问题，之所以记在这里，是因为它们代表了这个代码库里
容易再次犯错的模式：

- **资金费率结算的顺序问题**：早期实现是先转账再落审计记录，划转到一半失败会导致重复
  结算。现在改成先落记录再转账，见 [funding-rate.md](funding-rate.md)。
- **`MaintenanceMarginTotal`因为一个仓位缺数据就跳过整个uid**：早期实现是任何一个仓位
  缺标记价格/分档配置，就让调用方跳过这个用户的整轮风控扫描——会连累其它数据齐全、
  真正该被强平的仓位一起被放过。现在改成只跳过缺数据的那一个仓位。
- **保证金分档的杠杆校验只统计已成交仓位**：早期实现没有把"同方向还在排队的挂单"计入
  已占用的名义价值，顺序提交多笔远离盘口的限价单就能绕开分档上限。
- **排队挂单的名义价值估值用了标记价格而不是委托价格自己**：修复上一条问题时引入的新
  bug——排队挂单的价格越是远离标记价，用标记价格估值就越失真、越偏小，反而把刚堵上的
  漏洞又开了回去。
- **保证金冻结用委托价格算，而不是真实成交价**：`contract-api`下单时同步冻结保证金，
  但订单实际成交价由`contract-engine`那边的订单簿撮合决定，一笔报价远离市价的"吃单"
  会立刻按盘口对手的真实价格成交，两者可能完全不一样——已经实测复现过，报价1的SHORT
  吃单只冻结了几分钱保证金，成交后却开出了名义价值很大的真实仓位。修复思路不是让
  contract-api看到engine的盘口状态，而是两步走：①下单冻结时用保守参考价（SHORT+OPEN
  用`max(委托价,标记价)`，跟保证金分档校验用的是同一个基准）②成交后（`SettlementService.
  SettleFill`）按真实成交价重算这笔仓位真正该占用多少保证金，跟冻结时的保守估计"多退
  少补"，让`available`最终净扣掉的正好是真实经济意义上的保证金，不会把该占用的保证金
  错误地留在`available`里。价格保护带仍然保留，两者互补：保护带防胖手指、把"多退少补"
  的调整幅度限制在可控范围内，多退少补负责把账做平。
- **同一个Kafka consumer group id挂多个订阅不同topic的member会导致分区分配失效**：
  `contract-engine`原本用同一个group id`"contract-engine"`同时消费下单（`perpgo.order.
  submit`）和撤单（`perpgo.order.cancel`）两个topic，新增结束本轮（`perpgo.round.close`）
  消费者时如果也复用这个group id，实测会导致这个group有3个异构订阅的member，broker端
  分区分配阶段能正常完成（日志显示"Stabilized group"），但消费者实际收不到任何分配到的
  分区，下单/撤单两个topic的消费彻底卡住（`kafka-consumer-groups.sh --describe`看到
  lag不再下降、且没有活跃的CONSUMER-ID）。根治方式是每个独立的消费职责用自己独立的
  group id，不要图省事共用同一个——这不是"以后要注意"的假设性问题，是实测复现过的一个
  真实故障模式，加新的Kafka消费者时要留意。
- **ID生成器的跨进程碰撞风险**：早期实现是`毫秒时间戳×1000+进程内自增序号`，后来加了
  一个进程启动时随机抽的偏移混进去，把"两个进程同一毫秒序号都从0开始、几乎必然碰撞"
  降低成"凑巧抽到同一个随机偏移才碰撞"的低概率事件——这是缓解，不是根治，随机偏移仍然
  不保证跨进程唯一，而且序号对1000取模，单进程单毫秒内超过1000次调用会截断绕回重复。
  现在换成标准雪花算法布局（41位时间戳+10位node id+12位序列号），node id通过
  `PERP_NODE_ID`环境变量显式配置（不设时`contract-api`/`contract-engine`分别默认用
  0/1，本地单实例部署不需要额外配置），序列号用满就忙等到下一毫秒而不是截断——从"降低
  碰撞概率"变成"结构上不可能碰撞"（前提是运维保证每个实例的node id唯一，这是部署时的
  硬性要求，不是可以疏忽的细节）。见`internal/service/idgen.go`，并发压测见
  `idgen_test.go`。这次改动本身也踩过一次坑：系统时钟早于自定义纪元时`int64`减法结果
  是负数，转`uint64`会环绕，生成的ID会静默错乱而不是报错——已经加了显式检查，时钟早于
  纪元或者时钟回拨超过5秒都直接`log.Fatalf`快速失败，不放过去；时钟小幅回拨 (在阈值内)
  一开始改成带`time.Sleep`的等待而不是纯自旋，但那一版睡眠仍然睡在锁里面 (`idMu`全程
  没释放)，等于把"这一个调用方要等时钟追上来"变成了"整个进程全部NextID调用方一起等"——
  比单纯的CPU空转更糟，因为下单/成交/强平这些全链路都要用这个函数生成ID。后来改成
  一个不持锁的重试循环：每次尝试 (`tryNextID`)只在真正要读写共享状态的一小段临界区里
  持锁，需要等待的时候先把锁放掉再睡，让其它goroutine能正常继续生成ID。这次重构还
  意外冒出一个更隐蔽的问题：序列号用满时如果先把`idSeq`改成0再返回"这次没成功"，下一次
  重试如果时钟还没真的跳到下一毫秒，会从这个"寄存"的0再往上加到1——而这一毫秒的seq=1
  早就发给过别的调用方了，两个调用方会拿到同一个 (毫秒,序列号)组合，是真实的重复ID
  bug（触发概率极低，需要重试的`time.Sleep(1ms)`恰好没有真的跨过毫秒边界，但理论上
  存在，不能放过）。修复成序列号用到上限就直接拒绝、不做任何状态改动，只有时钟真的
  往前走了才重置序列号，彻底消除这个悬空状态。
- **订单簿是O (n)线性扫描实现，且没有自成交保护**：早期实现`bids`/`asks`各自是一个
  `[]*RestingOrder`切片，撮合/挂单/撤单全部靠线性扫描+每次整体重建切片，而且完全没有
  自成交保护——同一个uid的买卖单会正常互相撮合成交。这不是假设性问题，本次会话测试
  信用额度/条件单功能时，因为测试脚本让同一个uid在买卖两边都挂了单，实际触发过自成交，
  当时当成"测试数据设计问题"处理、绕开了事，没有意识到这是撮合引擎本身缺一层保护。
  现在换成"价格档位数组二分定位+档位内FIFO双向链表+orderID→节点的map"结构（O (1)按ID
  撤单，O (log m)+O (1)挂单，m=活跃价格档位数远小于挂单数），并且加了自成交保护（撮合时
  撞上同一个uid的挂单，把那笔挂单摘掉当撤单处理，taker继续往下吃非自己的流动性，不打乱
  其他人的排队顺序）。详见 [order-book.md](order-book.md)，并发/撮合正确性测试见
  `internal/matching/book_test.go`。
- **Kafka at-least-once语义下没有完整的消息去重**：早期实现只在`EngineService.
  SubmitOrder`入口加了两层针对性防护（委托状态检查+`Book.Contains`检查），只能挡住
  "这笔委托仍在正常排队"这一种最容易触发的重复投递场景，跨事件交叉时序（比如撤单已经
  处理完、提交事件的重复投递才姗姗来迟）防不住。现在按Kafka消息自身的`(topic,
  partition, offset)`坐标加了一张`processed_messages`表做完整的消息级去重，
  `internal/mq.WithDedup`统一包一层，下单/撤单/结束本轮三个消费者都覆盖到，不依赖
  具体业务字段。详见 [message-dedup.md](message-dedup.md)。
- **并发下单能绕开保证金分档限制**：早期实现"读现有仓位/挂单→算档位→冻结保证金"
  （`router.go`的`addOrder`/`addConditionalOrder`）整体不是原子的，只堵住了"顺序提交
  多笔远离盘口限价单"这条更容易触发的路径，同一个uid+symbol+side真正并发提交多笔请求
  时，每一笔读到的都是对方还没提交时的旧状态，理论上仍能绕开分档限制。现在用
  `internal/service.LockService`（Redis`SETNX`+TTL的分布式锁，不是进程内mutex——
  `contract-api`是无状态服务、允许多实例水平扩展，进程内锁防不住跨实例的竞态）按
  uid+symbol+side序列化这段临界区，已经用真实并发请求实测验证：两笔单独名义价值都在
  tier1范围内、但合计会落到只允许更低杠杆的tier2的并发下单，后到的那笔能正确读到前一笔
  已经提交的委托、按tier2的杠杆上限被拒绝。详见
  [risk-limit-tiers.md](risk-limit-tiers.md#并发下单的原子性按uidsymbolside的分布式锁)。
- **订单簿进程重启会丢失全部挂单**：早期实现`contract-engine`重启后订单簿是空的——
  委托本身没丢（DB里`status`依然是`open`/`partially_filled`），但内存订单簿不知道
  该把这些委托放回哪个位置，等于这些挂单虽然DB状态显示还活着、实际已经从撮合逻辑里
  消失（既不会被新委托撮合到，客户端撤单请求也会因为`Book.Cancel`找不到这个orderId
  而失败）。现在启动时`EngineService.RecoverOrderBook`从MySQL按`create_time`+`order_id`
  顺序把还在排队的LIMIT委托直接`Rest`回订单簿（不经过`Match`，理由见文档），已经用
  真实重启验证过：重启前挂着的两笔买单，重启后深度快照完全一致，且真的可以被新提交的
  卖单撮合成交（价格优先级也保持正确）。详见
  [order-book-recovery.md](order-book-recovery.md)。
- **engine没有横向扩展（按symbol分片到多个实例）**：早期实现`contract-engine`只能单
  实例部署，多起几个进程不会自动分摊负载——同一个symbol的内存订单簿会在多个实例里各自
  独立维护、互相不知道对方存在，产生错误的撮合结果。现在支持按`PERP_ENGINE_SYMBOLS`
  环境变量静态配置分片，每个实例只负责自己声明的symbol，用fan-out消费+应用层按symbol
  过滤（不依赖Kafka原生分区负载均衡，这个项目历史上在consumer group异构订阅上踩过坑，
  见下一条），结束本轮这类天然跨symbol的操作靠`round_close_progress`表+分布式锁做
  跨分片协调。已经用两个真实进程分别负责BTCUSDT/ETHUSDT实测验证过：下单/深度查询/结束
  本轮跨分片协调、消息去重fan-out隔离都符合预期，单实例默认模式向后兼容。详见
  [engine-sharding.md](engine-sharding.md)。
- **MARKET单缺对手盘会永久停在未成交状态**：早期实现`EngineService.SubmitOrder`对
  MARKET单的处理是"能吃多少吃多少，剩余量不挂簿"，但剩余量既不挂簿也没有被终结——如果
  提交时撮合引擎那个symbol的订单簿对应方向完全没有挂单（或没吃满），这笔MARKET委托会
  全程停留在`open`/`partially_filled`状态不会再变化，冻结的保证金也永远要不回来。现在
  `SubmitOrder`把MARKET单撮合后没吃掉的剩余部分直接终结成`canceled`、按比例释放冻结
  保证金（复用`CancelOrder`已有的`finalizeOrderCancel`核心逻辑，拆出不重复推送快照的
  `cancelAndReleaseMargin`），完全没吃到和部分吃到两种场景都用真实成交实测验证过：
  委托状态正确变成`canceled`、未成交部分的保证金正确退回`available`、部分成交的那部分
  正常结算进仓位。条件单触发后提交的MARKET委托复用同一条路径，同样修复，见
  [conditional-orders.md](conditional-orders.md)。
- **已有仓位的保证金调整误用了`frozen_margin`/`frozen_credit`路径**：实现"独立杠杆
  设置接口"（`POST /position/leverage`，见 [leverage.md](leverage.md)）第一版时，直接
  照抄开仓下单那段代码——杠杆调低调`FreezeMargin`、调高调`UnfreezeMargin`。实测立刻
  暴露问题：已经开仓的仓位，保证金根本不记在`accounts.frozen_margin`/`frozen_credit`
  这两列里（那两列只对应还在排队等成交的挂单，成交后就已经被`DecreaseFrozenMargin`
  转出、永久体现为`available`/`credit`余额的降低了），调高杠杆想释放保证金时去调
  `UnfreezeMargin`，会因为`frozen_margin`本来就是0而返回一个文不对题的"冻结保证金
  不足"。现在分别复用`ApplyCloseFill`释放持仓保证金（直接改`available`/`credit`，
  不碰`frozen_margin`）和"`FreezeMargin`+立刻`DecreaseFrozenMargin`"（借用完整的四级
  判断路径、但让净效果只体现在`available`/`credit`上、不residual在`frozen_margin`里）
  这两条既有链路各自的正确做法，用真实仓位（含`available`+`credit`混合来源的场景）
  实测验证过。这个模式值得记住： **这个系统里已经落地的仓位保证金，跟还在排队的挂单
  冻结保证金，是两套完全不同的记账路径，不能混用同一套函数**。
- **大仓位分批强平**：早期实现`queueLiquidation`一次性把整个仓位数量挂成一张保护价单，
  一个远超正常单笔委托量的大仓位会被整笔甩给盘口，对价格冲击太大，也没有考虑
  `coin.MaxVolume`（普通下单本来就有的单笔最大量限制）。现在把强平流程拆成独立goroutine
  循环处理的多个批次，单批不超过`coin.MaxVolume`，标记价格/分档配置在每一批都重新查一次
  （避免大仓位分批耗时跨度里用陈旧价格算保护价），全部批次处理完仓位归零才结束，见
  [liquidation.md](liquidation.md)"分批强平"一节。用真实多批场景（临时调小`MaxVolume`
  强制触发分批）实测验证过：日志依次打出多批挂单/超时兜底记录、维持保证金缓冲清算只在
  最后一批处理完之后触发一次（不是每批各触发一次）、委托历史里能看到多笔独立成交、每笔
  数量都精确等于批次上限。
- **自动减仓（ADL）**：早期实现保险基金不够覆盖穿仓缺口时只能让基金余额变负硬扛，没有
  自动减仓机制。现在`HandleLiquidationSettleAftermath`穿仓分支会先检查保险基金余额，
  不够覆盖时调用`runADL`（`internal/service/adl.go`）强制减仓这个symbol上跟被强平方向
  相反、按ROE降序排列最赚钱的仓位，把它们刚实现的那部分盈利划一部分给保险基金补缺口，
  补不满的部分仍然由基金硬扛，不是万能的——见 [liquidation.md](liquidation.md)"自动
  减仓（ADL）"一节。用真实穿仓场景实测验证过（构造高杠杆多空双方仓位、直接操作标记
  价格制造大幅下跌触发多头穿仓），ADL按ROE正确排序候选、减仓量精确对应筹款需求（不是
  整仓砍掉）、跨多个候选累加凑够金额、保险基金最终余额跟手算结果完全吻合。
- **`ApplyCloseFill`无条件把仓位状态重置成`normal`，清掉强平进行中的标记**：这是一次
  全量代码review发现并实测复现过的最严重问题。`ApplyCloseFill`（分批平仓/强平分批扣减
  都走这个函数）每次部分平仓后都会把`positions.status`硬编码写回`'normal'`，包括强平
  分批clip自己的成交结算——等于每处理完一批clip，就把`MarkLiquidating`当初原子设置的
  `'liquidating'`标记悄悄擦掉，让`MarkLiquidating`那个`WHERE status='normal'`的原子
  保护重新被满足，一次尚未处理完的分批强平在下一轮风控扫描（`RiskScanOnce`）里会被
  第二次`MarkLiquidating`成功、派生出第二个独立的`liquidateInClips`协程，跟第一个并发
  处理同一个仓位。用真实压测复现过（临时把风控扫描间隔调到300ms、构造一个吃穿保证金
  缓冲的价格暴跌场景确保每批clip的保证金释放追不上价格驱动的亏损，直接改Redis标记价格
  强制价格崩盘）：同一个`positionID`在日志里连续3次`MarkLiquidating`成功、出现重叠的
  分批挂单。修复方式是让`ApplyCloseFill`保留调用前读到的`p.Status`、只有仓位数量真正
  归零时才强制置`Closed`，不再无条件覆盖成`normal`；同时把`ApplyOpenFill`/
  `ApplyCloseFill`/`OrderRepo.ApplyFill`都改成乐观并发（CAS）重试循环（先读、算好新值、
  `UPDATE ... WHERE`带上读到的旧值做守卫，0行受影响就重新读最新状态重试），避免并发
  写入之间互相覆盖丢更新——之前这几个函数是"读一次、按SQL相对表达式写"，两次并发调用
  能各自读到旧状态、写操作互不知道对方的存在。用同样的压测场景重新验证：修复后同一个
  `positionID`只成功`MarkLiquidating`一次，分批clip严格顺序处理，最终账户状态归零且
  正确。**这个模式值得记住：任何"部分更新一行记录的部分字段"的函数，只要这一行还有
  其它字段承载着跨调用的状态语义（这里是`status`标记"强平进行中"），就不能用无条件覆写
  ——必须先读、只改真正要改的字段、其余字段原样保留，否则表面上"只是改了个数量"的一次
  调用会悄悄抹掉另一个并发流程留下的状态标记**。
- **资金费率采样累加器不是原子操作，分片部署下会丢样本**：早期实现`AccumulateFundingSample`
  是"GET当前累加值→在Go里加上这次的premium→SET回去"，`engine-sharding`部署下多个
  `contract-engine`实例各自独立跑采样定时器、会并发写同一个Redis key，这个GET-加-SET
  中间存在竞态窗口，两个实例几乎同时采样时，后写的会覆盖掉先写的、丢失一次样本的
  贡献。当初`engine-sharding.md`文档错误地断言"这个操作本身是原子的、完全不需要任何
  分片相关的改动"——这个断言本身没有去看`AccumulateFundingSample`的实现，只是觉得
  "看起来是个简单操作"就假设成立。现在把累加值拆成`sum`/`count`两个Redis key，用
  一段Lua脚本（`redis.NewScript`，`INCRBYFLOAT`+`INCR`）在Redis服务端原子地一次性
  完成两个key的更新，不再有Go这边GET-改-SET的竞态窗口。详见
  [engine-sharding.md](engine-sharding.md)。**这个模式值得记住：'这个操作本身是原子的'
  这句话，要去看实现，不能只看调用方式看起来简单就假设成立**。
- **`FreezeForceIntoNegative`用了过期的`available`/`credit`快照做守卫**：`AccountService.
  FreezeMargin`在四级判断的最后一级（`available+credit+未实现盈亏`覆盖但`available`单独
  不够）里调用`FreezeForceIntoNegative`时，用的是这次调用早前读到的、可能已经过期的
  `freshAvailable`/`freshCredit`；如果在这两次读取之间有另一笔并发的资金变动，实际执行
  的扣减会基于一个不再准确的基准。现在改成在真正调用前紧邻着重新读一次最新值，且
  `FreezeForceIntoNegative`自己也改成`UPDATE ... WHERE available=? AND credit=?`的CAS
  写法，读到的快照跟实际生效的这次扣减不一致时返回`false`、上层按余额不足处理，而不是
  静默用一个过期基准执行扣减。
- **条件单触发后落库委托失败会让冻结的保证金永久卡住**：`ConditionalOrderService.
  trigger`原本的顺序是`MarkTriggered`（原子标记，跟撤单竞争）成功后直接`orders.
  Insert`，如果`Insert`失败就直接返回——条件单已经停在`triggered`状态（`ScanOnce`
  只扫`pending`的，永远不会再发现它），触发前冻结的`FrozenMargin`/`FrozenCredit`
  也没有任何路径能退回来。现在补了一个失败补偿：`Insert`失败时调用新增的
  `MarkCanceledFromTriggered`把条件单状态改回`canceled`（专门只匹配`status='triggered'`，
  跟撤单用的`MarkCanceled`区分开，见该方法注释），并对`ActionOpen`且确实冻结过保证金
  的情况调用`EngineService.UnfreezeMargin`退还，等效于"这次触发没有真的发生"，用户
  损失的只是这一次止盈止损/条件开仓的机会，不是钱。
- **强平超时兜底成交，部分成交也标记成`Filled`**：`settleTimeoutFallback`原来不管实际
  成交量是不是覆盖了委托剩余量，一律把状态写成`Filled`——如果这笔兜底成交量小于委托
  剩余量（部分成交），错误的终态会导致`RecoverOrderBook`未来进程重启时，`FindActiveLimitOrders`
  （只查`open`/`partially_filled`）永远查不到这笔委托、复活不了剩余部分，同时状态又
  显示"已完全成交"，跟实际账目对不上。现在按`closeVolume`是否覆盖`o.RemainingAmount()`
  分别写`Filled`/`Canceled`，跟`SubmitOrder`里MARKET单缺对手盘的终态判断逻辑保持一致。
- **`submitLiquidationClip`查询合约配置失败时静默放过`MaxVolume`限制**：分批强平每一批
  开始前都要重新查一次`coins.FindBySymbol`拿当前的`MaxVolume`分批上限，早期实现查询出错
  或者查到`nil`时既不重试也不中止，直接让这一批按整个剩余仓位量提交——DB不稳定的时候
  恰好是最不该放过安全限制的时候，属于"失败开放"。现在改成失败关闭：出错时清除
  `liquidating`标记、推送账户快照、直接返回，交给下一轮风控扫描重新发起，不带着一个
  没有校验过的批次量继续往下走。
- **`CloseRound`结算credit时用的是清零前单独预读的旧值，跟并发`GrantCredit`竞态**：
  `AccountService.CloseRound`原本先`FindFreshCredit`单独读一次当前credit、再调用
  `CloseRoundIfRound`（`UPDATE ... credit=0 ... WHERE round=?`）真正清零，这两步之间
  如果有并发的`GrantCredit`把credit改大，`UPDATE`清零的是并发写入后的真实值，但
  `TxRoundClose`审计流水记的金额用的是清零前更早读到的、偏小的旧值，导致审计记录跟
  实际清零的金额永久对不上（不影响账户实际余额，`UPDATE`本身是原子且正确的，只影响
  审计流水这一个数字）。修复方式沿用`FreezeSpillToCredit`已经用过的MySQL会话变量
  捕获写法：`CloseRoundIfRound`在同一条`UPDATE`语句里用`credit = (@perpgo_old_credit
  := credit) - credit`原子地把清零前的值存进会话变量，`RowsAffected()>0`之后再
  `SELECT @perpgo_old_credit`读出来当作返回值，全程用同一条`sql.Conn`（会话变量是
  连接级别状态，跟连接池的其它连接无关，必须保证`UPDATE`和后续`SELECT`落在同一条
  物理连接上）。`CloseRound`不再单独预读，直接用`CloseRoundIfRound`原子返回的清零前
  金额记审计流水。用真实请求验证过：`GrantCredit`发放50、`CloseRound`结束本轮后
  `credit`正确归零、`member_transactions`审计流水显示`credit_grant +50`紧跟着
  `round_close -50`，金额精确对应。
