# 系统架构

## 两个进程

```
                 ┌──────────────────┐         ┌───────────────────┐
  HTTP 请求  ───▶ │   contract-api   │──Kafka─▶│  contract-engine  │
                 │ (Gin, 无状态)     │         │ (内存订单簿, 单实例) │
                 └──────────────────┘         └───────────────────┘
                         │                              │
                         ├──────────────┬───────────────┤
                         ▼              ▼               ▼
                      MySQL          Redis           Kafka
                 (账户/仓位/委托/    (标记价格/       (下单/撤单事件)
                  分档配置等)       指数价格/资金费率
                                    采样累加器)
```

- **contract-api**：对外HTTP服务。校验参数、冻结保证金、把委托落库（`status=NEW`），
  然后发一条事件到Kafka给engine去真正撮合。查询类接口直接读MySQL，不经过engine。
- **contract-engine**：消费Kafka里的下单/撤单事件，维护每个symbol一个内存订单簿（价格-时间
  优先），撮合成交后做结算（划保证金、结已实现盈亏、扣手续费），并且跑三个后台定时任务：
    - 风控扫描（`RiskScanOnce`）：判断哪些账户需要强平
    - 资金费率采样+结算（`SampleOnce` / `SettleIfDue`）
    - （撮合本身是事件驱动的，不是定时任务）

两个进程都是无状态的（engine的"状态"是内存订单簿，MVP阶段单实例部署，重启会丢失挂单——
这是已知的、还没解决的问题，见 [known-limitations.md](known-limitations.md)）。

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
  api/                Gin路由/handler层
  service/            业务逻辑（账户、持仓、结算、强平、资金费率、保险基金）
  repo/                数据访问层，一个repo对应一张表
  matching/           订单簿撮合引擎
  model/              领域模型结构体
  cache/              Redis封装
  mq/                 Kafka生产者/消费者封装
  config/             环境变量配置加载
  db/                 MySQL连接
  events/             Kafka事件结构体
sql/
  schema.sql          唯一的建表脚本，见下方"数据库迁移"
```

## 数据库迁移

这个项目没有独立的迁移工具，`sql/schema.sql`就是唯一的建表脚本。它被设计成 **对任何状态的
数据库重复执行都是安全的**：

- 新表用`CREATE TABLE IF NOT EXISTS`，对全新库和已经建过的库都天然安全。
- 给已有表加列/加索引， **不能**用`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`这类语法——
  这是MariaDB的扩展语法，标准MySQL不支持（实测MySQL 8.4会直接报语法错误）。schema.sql里
  用`information_schema`查列/索引是否存在、拼出动态SQL再`PREPARE`/`EXECUTE`执行的方式来
  模拟同样的效果。

改schema时如果要给 **已有表**加字段/加索引，照着`sql/schema.sql`里`coins`表和`positions`表
后面那几段`SET @sql := ...`的写法抄一份，不要直接把新列写进`CREATE TABLE`语句里就完事——
那样只对全新库有效，对已经跑起来的环境是no-op。
