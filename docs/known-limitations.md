# 已知限制与下一步计划

按影响程度排序，供后续迭代参考。

## 明确排除的功能（不是遗漏，是范围决策）

- **逐仓保证金模式**：目前只有全仓，一个仓位单独隔离保证金的逐仓模式没做。
- **"没有仓位时预先声明杠杆"的接口**：杠杆是每次下单时传的参数，没有"先设置这个合约的
  杠杆倍数、以后下单沿用它"这种独立接口——修改已有仓位杠杆的接口已经做了（`POST
  /position/leverage`，见 [leverage.md](leverage.md)），这里明确没做的只是"没有仓位时
  预先声明"这一种场景，理由见该文档"范围"一节。

## 对接相关的已知限制

- **鉴权已经实现，但还有几项没做**：API Key + HMAC 签名、`trade`/`ops`权限分级、防重放都已经落地
  （见 [auth-design.md](auth-design.md)）。还没做的：按密钥限流、IP白名单、多合作方的uid归属校验
  （WS私有频道`user:{uid}`不校验uid属于哪把密钥）、通过鉴权的请求的审计日志。
- **账户状态只有冻结/解冻，没有封禁/注销**：`POST /account/status`能冻结账户（禁止开仓、条件开仓、改杠杆，
  见 [account-and-margin.md](account-and-margin.md)"账户状态"），但没有"完全封禁"（连平仓、撤单也不让）和销户。
  冻结账户的仓位不会被系统强制平掉，需要平仓要运营/合作方自己处理。
- **分片部署下强平前撤挂单只撤得了自己拥有的symbol**：`CancelAllPendingOrders`摸不到别的分片实例的订单簿。用户在实例A的
  BTC上被强平、在实例B的ETH上还挂着开仓单时，A的强平后结算看不到B上挂单冻结的保证金，`available`为负时基金照常垫付，之后B撤单
  退回冻结的保证金，用户拿到的比真实权益多。单实例部署没有这个问题。B的风控扫描理论上也会在同一个uid权益跌破维持保证金时
  撤掉它自己拥有的symbol上的挂单，但两个实例的扫描没有先后保证，中间有窗口。根治需要强平触发时给所有分片广播"撤这个uid的挂单"
  的事件，还没做。
- **多仓位并发强平时基金流水可能有多余的进出**：一次成交的结算（仓位、保证金退回、盈亏、手续费）不是一个整体事务，
  先平完的仓位做强平后结算时，另一个仓位可能正处于结算的中间状态，看起来"已经没有剩余仓位"但保证金还没退回。
  结果是基金流水里先垫付、后回收各一行，总额守恒（每笔都等于当时被清零的金额），账户和基金的最终余额是对的。
  极端情况下先垫付可能不必要地触发一次ADL。根治需要把成交结算做成整体事务，跟"其它没有额外加锁的极端并发场景"
  是同一类已知简化。
- **引擎撮合每笔开仓委托要多查一次账户状态**：冻结的引擎侧兜底是撮合前读一次`accounts.status`（只读这一列，
  走`uid`唯一索引），没做缓存——跨进程缓存状态会让冻结有生效延迟，先选正确性。撮合延迟敏感、账户量大时，
  再考虑带短TTL的缓存或者让API层把状态带进Kafka事件。这次查询失败时那笔委托不撮合，但会停在活跃状态、
  不在订单簿里，直到引擎重启恢复（用户期间可以撤单）。
- **冻结的清理和解冻并发时可能误撤**：冻结接口先改状态再清理，如果清理进行到一半运营又解冻并且用户立刻
  下了新的开仓单，清理可能把这笔新单也撤掉。两个运营操作几乎同时打在同一个账户上才会出现，撤掉的单
  用户重新下即可，资金不会丢。
- **没有限流**：任何接口都没有按调用方限流，需要的话先在网关层做，见 [auth-design.md](auth-design.md)"还没做"。
- **没有已平仓仓位的历史记录**：`positions`表一个`uid+symbol+side`只有一行，平仓后同一行被复用
  （`volume`归零、`status=closed`），再开仓又写回这一行。合作方要对账只能用`/trade/history`和
  `/account/transactions`（已实现盈亏、手续费都有流水）。真要提供"历史仓位"需要新增一张仓位快照表。
- **`/market/ticker`的24小时统计是近似的**：用最近24根1小时K线聚合，实际窗口在23~24小时之间，
  不是严格滚动的24小时，换来的是查询成本恒定、不用扫成交明细表。
- **条件单触发后落地的委托不继承`requestId`**：见 [conditional-orders.md](conditional-orders.md)。
- **`requestId`的作用域是"每类资源、每个`uid`"**：订单、条件单、资金流水各自独立，资金类的两个
  接口（充值/扣款、发额度）共用一个空间。设计和边界见 [idempotency.md](idempotency.md)。
- **幂等键永久有效，没有过期清理**：跟订单/流水同生命周期，唯一索引随数据增长，量级跟这些表本身一致。

## 需要留意的边缘情况

- **分档配置在仓位已开仓后被删除/改坏**：维持保证金贡献会变成0，但未实现盈亏依然正常
  计入账户权益，见 [risk-limit-tiers.md](risk-limit-tiers.md)最后一节。
- **标记价的锚(指数价)只有服务端不校验、靠喂价器保护**：`POST /index-price`本身没有离群和跳变校验，拿着ops密钥的人可以
  推任意正价格；生产靠index-feeder（三家交易所取中位数、离群和跳变保护）把关，见 [index-feeder.md](index-feeder.md)。
  三家的指数价底层数据源有重叠，整个现货市场异常时喂价器防不住。指数价断供时标记价冻结、强平/资金费率/条件单暂停，
  恢复后一步跳到新价没有平滑，见 [mark-price.md](mark-price.md)。
- **资金费率的溢价被标记价压缩**：溢价率是`(标记价 - 指数价) / 指数价`，标记价被限制在指数价和指数价加盘口基差之间，
  所以溢价不会超过基差。币安用盘口冲击价格算溢价指数，后续可以照做，见 [mark-price.md](mark-price.md)。
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
  顺序把还在排队的LIMIT委托恢复回订单簿，已经用真实重启验证过：重启前挂着的两笔买单，
  重启后深度快照完全一致，且真的可以被新提交的卖单撮合成交（价格优先级也保持正确）。
  详见 [order-book-recovery.md](order-book-recovery.md)。（第一版是直接`Rest`不经过`Match`，
  后来做部署验证时发现有缺陷，见下面"恢复订单簿会让落库后没来得及处理的委托永远不成交"一条。）
- **恢复订单簿会让"落库了但引擎还没处理过"的委托永远不成交**（部署验证时用真实容器环境发现）：
  恢复逻辑是把数据库里`status=open`的委托直接`Rest`回订单簿、不撮合。但这些委托里除了"引擎已经
  处理过、静静挂在簿子上"的，还有"落库了但引擎还没处理过"的——引擎宕机或落后期间（发布重启的那
  几秒），`contract-api`照常落库并发Kafka事件，数据库里两者长得一模一样。后者被直接挂进簿子后
  会跟对手单交叉挂着，随后Kafka里积压的它们的下单事件又被当成"已经在订单簿里"的重复事件跳过，
  永远不成交。复现方法：停引擎、下两笔同价的买卖单、启动引擎，订单簿里买卖各一笔60000交叉挂着。
  日常发布就会触发，不是极端场景。修复：恢复时把每笔活跃委托重放进`Match`（走正常下单的路径），
  一组互不相交的挂单不管按什么顺序重放都不会产生成交，所以对正常挂单是安全的，对没处理过的委托
  正好补上撮合。用真实容器验证了两个方向：交叉的一对重放后正确成交、积压事件被跳过；五笔互不相交
  的挂单重启后订单簿逐档一致且成交笔数不变。见 [order-book-recovery.md](order-book-recovery.md)。
  这次也纠正了文档里原来那条"不能走`Match`"的推理——它默认落库的活跃委托都是互不相交的，这个
  前提并不成立。
- **引擎先于Kafka topic启动时，消费者一直消费不到消息**（部署验证时发现）：全新的Kafka上，topic是
  下单接口第一次写入时才自动创建的，引擎启动时消费者加入消费者组拿到的分区数可能是0，之后topic
  建出来了它也不会自己发现，订单一直积压。现在Kafka消费者打开了分区变化监听（`WatchPartitionChanges`，
  每5秒检查一次），部署时还会用一次性任务预先创建三个topic（见 [deployment.md](deployment.md)）。
- **`contract-api`不能优雅退出**：原来直接`Run()`，收到SIGTERM会立刻断开在途请求，下单请求可能正处在
  "已冻结保证金、还没落库/发Kafka"这一步。现在收到SIGTERM/SIGINT先停止接收新连接、等在途请求处理完
  （最多15秒）再退出，实测`docker stop`耗时0.2秒、退出码0。
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
  归零时才强制置`closed`，不再无条件覆盖成`normal`；同时把`ApplyOpenFill`/
  `ApplyCloseFill`/`OrderRepo.ApplyFill`都改成乐观并发（CAS）重试循环（先读、算好新值、
  `UPDATE ... WHERE`带上读到的旧值做守卫，0行受影响就重新读最新状态重试），避免并发
  写入之间互相覆盖丢更新——之前这几个函数是"读一次、按SQL相对表达式写"，两次并发调用
  能各自读到旧状态、写操作互不知道对方的存在。用同样的压测场景重新验证：修复后同一个
  `positionID`只成功`MarkLiquidating`一次，分批clip严格顺序处理，最终账户状态归零且
  正确。 **这个模式值得记住：任何"部分更新一行记录的部分字段"的函数，只要这一行还有
  其它字段承载着跨调用的状态语义（这里是`status`标记"强平进行中"），就不能用无条件覆写
  ——必须先读、只改真正要改的字段、其余字段原样保留，否则表面上"只是改了个数量"的一次
  调用会悄悄抹掉另一个并发流程留下的状态标记**。
- **合作方对接接口的几处缺陷（对接评审发现）**：① **响应JSON字段命名混乱**：模型结构体没有`json`
  tag，同一个对象里既有`"ID"`、`"UID"`（大写开头）又有`"markPrice"`（小写开头），订单是全大写驼峰、账户是
  全小写驼峰——合作方按这个写解析代码，以后一改字段名就是破坏性变更。现在全部统一成小驼峰，并加了
  `model_json_test.go`锁住字段命名。② **雪花ID当JSON数字返回**：`orderId`形如`226750310570262528`
  （约2.2×10¹⁷），超过JS安全整数范围（9×10¹⁵），经过JS/double解析会丢精度。现在所有雪花ID
  （`orderId`/`tradeId`/资金流水`id`）都是字符串。③ **余额不足下单返回500**：`FreezeMargin`返回的
  `ErrInsufficientMargin`没被`respondLockErr`识别，落进了兜底的500分支，合作方分不清"余额不足"
  （业务上的正常拒绝）和"服务端故障"。现在是`400 insufficient_margin`。④**`POST /account/balance`
  扣款不校验余额**：文档写着"扣的时候必须有足够`available`"，代码里其实是无条件的`available = available + ?`，
  扣款能把余额扣成负数。现在负数走`UPDATE ... WHERE available >= ?`原子条件更新，不够返回
  `insufficient_balance`。⑤ **WS公开成交频道泄露用户身份**：`trade:{symbol}`是任何连接都能订阅的
  公开频道，却把内部`Trade`结构整个推了出去，带着买卖双方的`uid`和委托id。现在推`PublicTrade`视图，
  用真实WS客户端订阅验证过收到的帧里没有任何`uid`字段。⑥ **列表接口"没有数据"返回`null`**：
  `nil`切片被序列化成`null`，合作方遍历会出错，REST统一改成`[]`，WS私有快照的`activeOrders`同理。
  ⑦ **下单没有幂等机制**：请求超时后重试会重复下单、重复冻结保证金。现在支持`requestId`，
  并发重复提交用10~12路并行请求验证过：只有一笔订单、保证金只冻结一次。同一个`requestId`带了
  不同参数会返回`idempotency_conflict`，不会静默返回第一笔。⑧ **充值/扣款、发信用额度没有幂等键**：
  `/account/balance`是相对加减、`/account/credit`是`credit = credit + ?`，合作方超时重试会重复入账
  或让信用额度翻倍。现在这两个接口`requestId`必填，"写流水占位幂等键+改余额"放进同一个事务（先写流水
  后改余额中间崩溃会让钱永远补不上，先改余额后写流水中间崩溃会重复入账），扣款余额不足时事务回滚、
  不占用`requestId`。12路并发同一个`requestId`充值验证过：只入账一次。（review时还压出一个死锁：同一个
  `requestId`并发扣款、先到的因余额不足回滚时，后面几个会在唯一索引上互相升级锁，16路并发约一半返回500。
  现在事务开头先`SELECT ... FOR UPDATE`锁账户行、同一账户的资金操作串行，再压24路×8轮零死锁，另外保留了
  死锁自动重试兜底，见 [idempotency.md](idempotency.md)。）⑨**`round/close`重试会把下一轮
  又结束一次**（本组里后果最重的一个）：请求里没有"要结束哪一轮"，引擎处理时读的是当前轮，合作方超时
  重试会撤掉新一轮刚挂的单、强平刚开的仓、清零刚发的信用额度，Kafka消息级去重管不到这种"另一次独立
  调用"。现在`round`必填，API层预检（`already_closed`/`round_mismatch`）加引擎层权威判断（事件里的
  `round`不等于账户当前`round`就忽略）两层防护，用真实数据验证过：结束第0轮后进入第1轮、发额度开仓，
  再重试结束第0轮，第1轮的持仓和额度原样保留；直接往Kafka发一条过期事件，引擎日志打忽略警告、状态不变。这一组问题的共同教训：
  **对外接口的"形状"（字段名、类型、错误码语义）一旦有人依赖就很难改，所以要在第一个合作方开始
  联调之前定下来**。
- **账户靠"第一次用到就自动建"，uid手误写错的充值会成功**：早期所有接口遇到没见过的`uid`都会悄悄建账户
  （`GetOrCreate`），实测`GET /account/info?uid=任意数字`会往`accounts`表里塞一行，给一个从没存在过的
  `uid`充值500也返回成功、并新建了账户——合作方把`10001`写成别的数字，钱就进了一个没人认领的账户，
  而且看不出来。现在改成显式创建：新增`POST /account/create`（天然幂等，重复创建返回已有账户），其它
  接口（充值、发额度、下单、全部查询、结束本轮等）遇到没创建的`uid`返回`account_not_found`，用真实请求
  逐个接口验证过，且被拒绝的请求之后`accounts`表里没有留下任何行。内部流程（成交结算、强平、资金费率）
  操作的账户一定已经存在，仍然用`GetOrCreate`不受影响。
- **资金费率采样累加器不是原子操作，分片部署下会丢样本**：早期实现`AccumulateFundingSample`
  是"GET当前累加值→在Go里加上这次的premium→SET回去"，`engine-sharding`部署下多个
  `contract-engine`实例各自独立跑采样定时器、会并发写同一个Redis key，这个GET-加-SET
  中间存在竞态窗口，两个实例几乎同时采样时，后写的会覆盖掉先写的、丢失一次样本的
  贡献。当初`engine-sharding.md`文档错误地断言"这个操作本身是原子的、完全不需要任何
  分片相关的改动"——这个断言本身没有去看`AccumulateFundingSample`的实现，只是觉得
  "看起来是个简单操作"就假设成立。现在把累加值拆成`sum`/`count`两个Redis key，用
  一段Lua脚本（`redis.NewScript`，`INCRBYFLOAT`+`INCR`）在Redis服务端原子地一次性
  完成两个key的更新，不再有Go这边GET-改-SET的竞态窗口。详见
  [engine-sharding.md](engine-sharding.md)。 **这个模式值得记住：'这个操作本身是原子的'
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
- **强平超时兜底成交，部分成交也标记成`filled`**：`settleTimeoutFallback`原来不管实际
  成交量是不是覆盖了委托剩余量，一律把状态写成`filled`——如果这笔兜底成交量小于委托
  剩余量（部分成交），错误的终态会导致`RecoverOrderBook`未来进程重启时，`FindActiveLimitOrders`
  （只查`open`/`partially_filled`）永远查不到这笔委托、复活不了剩余部分，同时状态又
  显示"已完全成交"，跟实际账目对不上。现在按`closeVolume`是否覆盖`o.RemainingAmount()`
  分别写`filled`/`canceled`，跟`SubmitOrder`里MARKET单缺对手盘的终态判断逻辑保持一致。
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
