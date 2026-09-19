# Kafka消息级去重

## 背景：at-least-once语义下的重复投递

`internal/mq`的Kafka消费者封装（`kafka-go`）走的是at-least-once语义——消费者宕机重启、
消费者组rebalance、网络抖动导致offset提交失败等情况，都可能让同一条消息被再投递一次，
`contract-engine`的业务handler有概率对同一笔下单/撤单/结束本轮事件跑第二遍。

在这之前，只在`EngineService.SubmitOrder`入口加了两层 **针对性**防护（委托状态检查+
`Book.Contains`检查），只能挡住"这笔委托仍在正常排队"这一种最容易触发的场景，见
[known-limitations.md](known-limitations.md)里记录的历史限制。现在补的是 **完整的消息级
去重**，不依赖具体业务字段，三个消费者（下单/撤单/结束本轮）统一生效。

## 方案：按(consumer_group, topic, partition, offset)落一张"已处理"表

Kafka里每条消息在一个topic的一个partition内的offset是严格递增、全局唯一的坐标——
`(topic, partition, offset)`这个三元组唯一标识一条消息，不管消息内容是什么。用这个坐标
做去重，不用理解、不用解析消息的业务字段，对下单/撤单/结束本轮三种不同的事件类型统一
生效。

`consumer_group`也在key里（不是只按三元组）：engine分片部署下（见
[engine-sharding.md](engine-sharding.md)），每个`contract-engine`实例对同一个topic用
自己独立的consumer group id、各自fan-out拿到完整消息流，同一条消息会被多个实例各自
独立消费一次。去重必须按"这个consumer group有没有处理过"分别判断，不能用一个全局共享
的去重状态——否则先处理到这条消息的那个实例会把其它本来也需要独立处理这条消息的实例
给挡住。单实例部署（不分片）下，这就是同一个固定group id反复用，效果上退化成原来的
三元组去重，没有额外开销。

`processed_messages`表（`sql/schema.sql`）：

```sql
CREATE TABLE IF NOT EXISTS processed_messages (
  consumer_group VARCHAR(191) NOT NULL,
  topic          VARCHAR(191) NOT NULL,
  `partition`    INT NOT NULL,
  `offset`       BIGINT NOT NULL,
  create_time    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (consumer_group, topic, `partition`, `offset`),
  KEY idx_processed_messages_create_time (create_time)
) ENGINE=InnoDB;
```

`internal/repo.ProcessedMessageRepo.TryMark(ctx, consumerGroup, topic, partition, offset)`
处理消息前先`INSERT IGNORE`占位：插入成功（受影响行数>0）说明是第一次处理，插入被忽略
（受影响行数=0，撞了主键）说明这条消息之前已经处理过。用"先插入、按受影响行数判断"而
不是"先SELECT再INSERT"，是因为唯一约束本身就是数据库层面并发安全的去重屏障，不需要
应用层自己加锁应对"查完还没插、另一个goroutine也在查"这种竞态窗口（虽然这几个Kafka
消费者目前都是单goroutine顺序处理各自topic，暂时不存在这种并发，但这个写法本身更健壮，
不依赖"暂时只有一个消费者"这个前提）。

## `internal/mq.WithDedup`：包一层，三个消费者复用同一份逻辑

`internal/mq`新增：

- `Message`结构体：`Consumer.Consume`的handler签名从`func(key, value []byte) error`
  改成`func(msg Message) error`，`Message`里带上`Topic`/`Partition`/`Offset`——之前
  这三个字段被丢弃了，去重需要它们
- `Consumer.GroupID()`：暴露这个Consumer自己订阅时用的group id，供调用方在别处需要
  跟订阅group保持一致时复用（比如按group id拼日志/监控标签）
- `DedupChecker`接口：只要求一个`TryMark(ctx, consumerGroup, topic, partition,
  offset)`方法，`mq`包不直接依赖`repo`包（避免包依赖方向不对——`mq`是更底层的基础
  设施，不应该知道`repo`这种业务层的存在），`*repo.ProcessedMessageRepo`天然满足
  这个接口
- `WithDedup(ctx, groupID, checker, handler)`：返回一个包装过的handler，处理前先按
  `groupID`+消息坐标`TryMark`，已经处理过的直接跳过（返回nil，不执行真正的业务
  handler），没处理过的正常放行

`cmd/contract-engine/main.go`三个消费者（`order.submit`/`order.cancel`/`round.close`）
各自先用`consumerGroupID(...)`算出这个消费者要用的group id（未分片时是固定值，分片
部署下按`PERP_NODE_ID`区分，见 [engine-sharding.md](engine-sharding.md)），同一个
变量既传给`mq.NewConsumer`订阅，也传给`mq.WithDedup`去重——两处必须用同一个值，不然
去重记录的key就跟这个Consumer实际订阅的group对不上，不需要三处各自实现一遍去重逻辑。
三个消费者的业务处理是`EngineService`的`HandleOrderSubmit`/`HandleOrderCancel`/`HandleRoundClose`
（`internal/service/consumer_handlers.go`），`main.go`里只负责把它们包上`WithDedup`交给消费循环。

注意消息级去重只管"**同一条Kafka消息**被重复投递"（按offset判断）。合作方**自己再调一次接口**产生的是一条
全新的消息，它管不到——这类重复要靠接口自己的幂等键：下单/条件单/资金操作的`requestId`、结束本轮的`round`
（事件`RoundCloseEvent`里带着`Round`，引擎处理时跟账户当前轮数不一致就忽略），见 [idempotency.md](idempotency.md)。

**去重层本身故障时的取舍**：`TryMark`如果失败（比如MySQL抖动），`WithDedup`选择继续
执行业务handler，只记一条`[WARN]`日志，不阻塞消息处理——去重是锦上添花的正确性加固，
不能变成"MySQL稍微抖一下、全部消息就卡住不处理"的新单点故障。这意味着去重层故障期间
理论上又退化回"只靠`SubmitOrder`那两层针对性防护"的状态，但比完全不做去重仍然更安全，
不是"要么完美、要么无用"的取舍。

## 记录会无限增长，需要定期清理

`processed_messages`按消息坐标累积，不清理的话行数只增不减。`ProcessedMessageRepo.
DeleteOlderThan(ctx, cutoff)`删除`create_time`早于cutoff的记录，`cmd/contract-engine/
main.go`里有一个独立的定时任务（`DedupCleanupIntervalMs`默认1小时扫一次）按
`DedupRetentionHours`（默认168小时=7天，跟Kafka topic的常见默认retention对齐）算cutoff
并清理。保留窗口的选择逻辑：只要比"重复投递实际可能出现的最大延迟"长得多就够用——正常
情况下重复投递发生在consumer重启/rebalance附近的很短时间内，7天是非常宽松的安全余量，
不是精确计算出来的下限。

## 仍然不覆盖的场景

- 这套方案挡的是"同一条Kafka消息被投递两次"，不是业务层面的其它重复来源（比如客户端
  自己在HTTP层重试导致下了两笔内容相同但是`order_id`不同的委托）——`order_id`不同的
  两笔请求在这套机制看来是两条完全独立的合法消息，不会被去重，这不是这套方案要解决的
  问题
- `processed_messages`表本身没有跨MySQL实例的复制/高可用考虑，MySQL不可用时整套下单/
  撤单/结束本轮链路本来就不可用（这几个handler处理过程里也要读写MySQL），去重表不是
  额外引入的新依赖点
