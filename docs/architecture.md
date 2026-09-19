# 系统架构

## 两个进程

```
  HTTP 请求  ───▶ ┌──────────────────┐         ┌───────────────────┐
  WS 连接    ───▶ │   contract-api   │──Kafka─▶│  contract-engine  │
                 │ (Gin+WS网关,无状态)│         │ (内存订单簿, 可分片)  │
                 └──────────────────┘         └───────────────────┘
                         │      ▲                       │      │
                         │      └────Redis Pub/Sub───────┘      │
                         ├──────────────┬───────────────────────┤
                         ▼              ▼                       ▼
                      MySQL          Redis                   Kafka
                 (账户/仓位/委托/    (标记价格/指数价格/       (下单/撤单/
                  分档配置等)       资金费率采样累加器/         结束本轮事件)
                                    WS推送pub/sub频道)
```

- **contract-api**：对外HTTP服务+WS网关。校验参数、冻结保证金、把委托落库（`status=open`），
  然后发一条事件到Kafka给engine去真正撮合。查询类接口直接读MySQL，不经过engine。同时
  承载`GET /ws`：`internal/ws.Hub`按需订阅`contract-engine`发布到Redis的频道、转发给
  订阅了对应频道的WS客户端，见 [websocket.md](websocket.md)。
- **contract-engine**：消费Kafka里的下单/撤单事件，维护每个symbol一个内存订单簿（价格-时间
  优先），撮合成交后做结算（划保证金、结已实现盈亏、扣手续费），状态变化时顺手通过
  `internal/service.PushService`往Redis Pub/Sub发布（深度/成交/K线/标记价格/账户快照），
  并且跑三个后台定时任务：
    - 风控扫描（`RiskScanOnce`）：判断哪些账户需要强平
    - 资金费率采样+结算（`SampleOnce` / `SettleIfDue`）
    - 条件单触发扫描（`ConditionalOrderService.ScanOnce`）
    - （撮合本身是事件驱动的，不是定时任务）

contract-api是无状态的，可以直接多开实例。contract-engine的"状态"是内存订单簿——进程
重启不会丢挂单，启动时会从MySQL重建（见 [order-book-recovery.md](order-book-recovery.md)），
也支持按symbol静态分片到多个实例分摊撮合负载（见 [engine-sharding.md](engine-sharding.md)），
默认（不配置分片）仍然是单实例部署全部symbol，跟上图画的一致。

## 为什么保证金冻结在contract-api同步完成

下单接口收到请求后， **先在contract-api里同步校验+冻结保证金+落库**，再发Kafka事件给engine，
不是让engine异步处理"扣不扣得动钱"这件事。这样"余额不足"能在HTTP响应里直接告诉调用方，
不用等一趟Kafka往返再来查状态。

这个设计的代价是：contract-api在冻结保证金、做保证金分档校验的时候， **看不到
contract-engine那边订单簿的实时状态**——比如一笔报价远低于市价的"吃单"，contract-api按
用户填的价格算冻结的保证金，但订单实际会在engine那边按盘口对手的真实价格成交，两边对不上。
这是一个已知的架构局限，细节和缓解措施见 [known-limitations.md](known-limitations.md)。

## 技术选型

- **MySQL + sqlx**：不用ORM，因为账户余额的加减/冻结/解冻全部用"UPDATE ... WHERE 字段>=金额"
  这种数据库层面的原子条件更新做并发控制（见 [account-and-margin.md](account-and-margin.md)），
  这个模式要求精确控制SQL语句本身，跟ORM的抽象合不来。
- **Redis**：存标记价格、指数价格、资金费率周期内的采样累加器——都是"频繁读写、允许短暂不一致、
  不需要事务"的数据，适合放缓存而不是MySQL。
- **Kafka**：contract-api和contract-engine之间解耦。按symbol哈希分区，保证同一个symbol的
  下单/撤单事件在engine端消费时相对顺序不乱。
- **shopspring/decimal**：金融计算全程用`decimal.Decimal`，不用`float64`——浮点数无法精确
  表示十进制小数，累加会产生误差，这是硬性要求，不是风格偏好。

## 目录结构

```
cmd/
  contract-api/       contract-api 进程入口
  contract-engine/    contract-engine 进程入口
internal/
  api/                Gin路由/handler层：router.go核心交易接口、extra.go查询/批量接口、errors.go错误码、params.go分页与幂等键校验、ws_server.go WS升级入口
  ws/                 contract-api侧WS网关：Hub(频道订阅路由)+Client(单连接读写)
  pubsub/             WS推送的Redis channel命名规则，发布端(push.go)/订阅端(ws.Hub)共用
  service/            业务逻辑（账户、持仓、结算、强平、资金费率、保险基金、WS推送编排）
  repo/                数据访问层，一个repo对应一张表
  matching/           订单簿撮合引擎
  model/              领域模型结构体
  cache/              Redis封装（含WS推送用的Publish/Subscribe）
  mq/                 Kafka生产者/消费者封装
  config/             环境变量配置加载
  db/                 MySQL连接
  events/             Kafka事件结构体
sql/
  schema.sql          唯一的建库脚本(纯建表+初始数据)，见下方"数据库"
deploy/               容器化部署：Dockerfile、docker-compose.yml、环境变量模板，见 [deployment.md](deployment.md)
```

## 数据库

`sql/schema.sql`是唯一的建库脚本，里面**只有建表语句和演示用的初始数据**，每张表直接写成最终形态：
所有列、索引、唯一约束都在`CREATE TABLE`里，不出现任何`ALTER TABLE`、`ADD/DROP/MODIFY COLUMN`、
`CREATE INDEX`这类修改已有表的语句。

- 新表用`CREATE TABLE IF NOT EXISTS`，初始数据用`ON DUPLICATE KEY UPDATE`，所以这个文件可以
  重复执行、不会报错，也不会让种子数据翻倍。
- **改表结构就直接改对应的`CREATE TABLE`**，不要在文件里追加`ALTER`。`CREATE TABLE IF NOT EXISTS`对
  已经存在的表是no-op，所以已经建过库的开发/测试环境不会自动跟上新结构，需要重建库
  （`DROP DATABASE perpgo`后重新执行`schema.sql`）。项目还没有上线、库里没有需要保留的生产数据，
  这样最简单，也不会在文件里堆积一串历史迁移逻辑。
- 验证改动的办法：在一个全新库里执行`schema.sql`，再用`information_schema.COLUMNS`/`STATISTICS`
  跟期望的结构对比。
- **上线之后这个做法要换**：有了不能丢的数据就不能靠重建库，到时候需要引入独立的迁移工具
  （每次结构变更一个带版本号的迁移脚本，如`golang-migrate`），`schema.sql`只保留给全新环境做初始化。
