-- perp-go 第一期(MVP)表结构，独立新库(建议库名 perpgo)。
-- 账户(含信用额度)、合约配置与保证金分档、委托/条件单/持仓/成交、K线、资金流水、指数价格与
-- 资金费率结算、保险基金、Kafka消息去重。不建：逐仓模式——按项目约定只支持全仓，明确不做。
--
-- 这个文件只包含建表语句和演示用的初始数据，表结构一律直接写成最终形态，不写任何
-- ALTER/DROP COLUMN、MODIFY COLUMN、CREATE INDEX这类修改已有表的语句。要改表结构就直接改
-- 下面对应的CREATE TABLE，已经建过库的环境重建库即可(见docs/architecture.md"数据库"一节)。

CREATE DATABASE IF NOT EXISTS perpgo DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE perpgo;

-- 合约账户：一个uid一行，全仓保证金(账户级别共享资金池，不做逐仓)
CREATE TABLE IF NOT EXISTS accounts (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  uid            BIGINT UNSIGNED NOT NULL,
  is_insured     TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否投保：0-不投保，1-投保',
  round          BIGINT UNSIGNED NOT NULL DEFAULT 1 COMMENT '轮数，从1开始(0不是合法值，POST /account/round/close的round用binding:"required"校验)',
  credit         DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '信用额度总额，只能用于开仓保证金，不能转出/提现',
  balance        DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '余额总额(不随下单/开仓冻结而变化，只有真实的充值/提现/已实现盈亏/手续费才会改它)，可能为负(全仓下用持仓浮盈当买力借出去的部分)',
  frozen_margin  DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '来自balance的锁定额——挂单冻结+持仓占用的保证金合计，可用余额=balance-frozen_margin',
  frozen_credit  DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '来自credit的锁定额，跟frozen_margin对称，可用信用额度=credit-frozen_credit',
  version        INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '乐观锁版本号，MVP阶段原子UPDATE为主，这个字段先留着备用',
  status         ENUM('active','frozen') NOT NULL DEFAULT 'active' COMMENT 'frozen=禁止开仓/条件开仓/改杠杆，仍允许平仓、撤单、查询、结束本轮和运营的资金操作',
  status_reason  VARCHAR(255) NOT NULL DEFAULT '' COMMENT '最近一次状态变更的原因',
  status_time    BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '最近一次状态变更的毫秒时间戳，0=从没变更过',
  created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_accounts_uid (uid)
) ENGINE=InnoDB;

-- 账户状态变更历史：每次冻结/解冻记一行，谁(哪把API密钥)、什么时候、因为什么。这是管理类操作，
-- 出了纠纷要能追溯，所以单独留一张表，不靠日志
CREATE TABLE IF NOT EXISTS account_status_history (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  uid          BIGINT UNSIGNED NOT NULL,
  from_status  ENUM('active','frozen') NOT NULL,
  to_status    ENUM('active','frozen') NOT NULL,
  reason       VARCHAR(255) NOT NULL DEFAULT '',
  operator     VARCHAR(64) NOT NULL DEFAULT '' COMMENT '执行变更的API密钥id，鉴权关闭(本地开发)时为空',
  create_time  BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (id),
  KEY idx_account_status_history_uid (uid)
) ENGINE=InnoDB;

-- 合约配置：维持保证金率/最大杠杆按名义价值分档，见下面的risk_limit_tiers表。
-- 带"0=不限制/不校验"注释的列统一用0表示不启用这项限制
CREATE TABLE IF NOT EXISTS coins (
  symbol                    VARCHAR(32) NOT NULL,
  base_coin_scale           TINYINT UNSIGNED NOT NULL DEFAULT 8 COMMENT '标的币数量精度(小数位数)',
  price_scale               TINYINT UNSIGNED NOT NULL DEFAULT 2 COMMENT '价格展示精度(小数位数)',
  enable                    TINYINT(1) NOT NULL DEFAULT 1,
  maker_fee                 DECIMAL(8,6) NOT NULL DEFAULT 0.000200,
  taker_fee                 DECIMAL(8,6) NOT NULL DEFAULT 0.000500,
  price_tick                DECIMAL(18,8) NOT NULL DEFAULT 0 COMMENT '价格最小变动单位，0=不校验',
  volume_step               DECIMAL(18,8) NOT NULL DEFAULT 0 COMMENT '数量步长，0=不校验',
  min_volume                DECIMAL(18,8) NOT NULL DEFAULT 0,
  max_volume                DECIMAL(18,8) NOT NULL DEFAULT 0 COMMENT '0=不限制',
  funding_interval_hours    INT UNSIGNED NOT NULL DEFAULT 8 COMMENT '资金费率结算周期(小时)，对齐到从0点起的整点边界',
  funding_rate_cap          DECIMAL(10,6) NOT NULL DEFAULT 0.007500 COMMENT '资金费率上下限，0=不限制',
  funding_impact_notional   DECIMAL(18,8) NOT NULL DEFAULT 10000 COMMENT '资金费率溢价用的冲击名义金额(USDT)：按这个金额吃盘口算冲击买卖价，某一侧盘口不够深这个合约就采不到溢价样本。必须大于0，0=不采样(资金费率恒为0)',
  price_protection_ratio    DECIMAL(8,6) NOT NULL DEFAULT 0.050000 COMMENT '限价单允许偏离标记/指数价格的最大比例，0=不校验',
  created_at                DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (symbol)
) ENGINE=InnoDB;

-- 保证金分档(风险限额)：一个symbol配多档，按tier(1开始)从小到大对应名义价值从低到高。
-- maintenance_amount是速算扣除数，让跨档位时维持保证金连续，公式=名义价值*maintenance_margin_rate
-- -maintenance_amount，见internal/service/position.go的TierFor/MaintenanceMarginTotal。
-- 每个symbol必须至少配一档，没有配置分档的合约不允许开仓(风控判断没有依据)
CREATE TABLE IF NOT EXISTS risk_limit_tiers (
  id                      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  symbol                  VARCHAR(32) NOT NULL,
  tier                    TINYINT UNSIGNED NOT NULL COMMENT '档位序号，从1开始，越大风险越高',
  max_notional            DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '本档名义价值上限，0=不限(最后一档)',
  maintenance_margin_rate DECIMAL(8,6) NOT NULL,
  maintenance_amount      DECIMAL(26,16) NOT NULL DEFAULT 0,
  max_leverage            INT UNSIGNED NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_risk_limit_tiers_symbol_tier (symbol, tier)
) ENGINE=InnoDB;

-- 委托单。枚举列的取值全部小写，跟对外API的枚举约定一致(见docs/api.md)。
-- request_id是合作方指定的幂等键，同一uid内唯一；没传就是NULL——MySQL唯一索引允许
-- 多个NULL，所以没传的委托不受这个唯一约束影响。request_hash是这次请求参数的摘要，
-- 用来识别"同一个request_id却带了不同参数"的误用
CREATE TABLE IF NOT EXISTS orders (
  order_id        BIGINT UNSIGNED NOT NULL COMMENT '雪花ID或类似的分布式唯一ID，应用层生成',
  uid             BIGINT UNSIGNED NOT NULL,
  symbol          VARCHAR(32) NOT NULL,
  side            ENUM('long','short') NOT NULL,
  action          ENUM('open','close') NOT NULL,
  type            ENUM('limit','market') NOT NULL,
  price           DECIMAL(18,8) NOT NULL DEFAULT 0 COMMENT '市价单恒为0',
  amount          DECIMAL(26,16) NOT NULL COMMENT '标的币数量',
  traded_amount   DECIMAL(26,16) NOT NULL DEFAULT 0,
  avg_deal_price  DECIMAL(18,8) NOT NULL DEFAULT 0,
  frozen_margin   DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '这笔委托占用的冻结保证金(来自balance的部分)',
  frozen_credit   DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '这笔委托占用的冻结保证金(来自credit的部分)',
  leverage        INT UNSIGNED NOT NULL,
  reduce_only     TINYINT(1) NOT NULL DEFAULT 0,
  liquidation     TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否强平单——结算后要走保险基金穿仓/盈余清算分支',
  status          ENUM('open','partially_filled','filled','canceled','rejected') NOT NULL DEFAULT 'open',
  create_time     BIGINT UNSIGNED NOT NULL COMMENT '毫秒时间戳，撮合引擎按这个做时间优先排序',
  update_time     BIGINT UNSIGNED NOT NULL,
  request_id      VARCHAR(64) NULL COMMENT '合作方指定的幂等键，同一uid内唯一，NULL=没传',
  request_hash    VARCHAR(64) NULL COMMENT '请求参数摘要，同一个request_id再次提交时用来判断参数是否一致',
  PRIMARY KEY (order_id),
  UNIQUE KEY uk_orders_uid_request_id (uid, request_id),
  KEY idx_orders_uid (uid),
  KEY idx_orders_symbol_status (symbol, status)
) ENGINE=InnoDB;

-- 条件单(止盈止损/条件开仓)：创建时不进撮合引擎的订单簿，只是"记着一个触发条件"，
-- 由contract-engine定时扫描标记价格，触发了才转成一笔真正的委托(落到orders表)按正常流程
-- 提交撮合。order_id在创建条件单的时候就分配好，触发后落地到orders表也用这同一个id，
-- 客户端不需要另外维护"条件单id"和"委托id"两套编号。
-- request_id含义同orders表，唯一索引是这张表自己的(uid, request_id)，触发后落地到
-- orders表的委托不继承它
CREATE TABLE IF NOT EXISTS conditional_orders (
  order_id          BIGINT UNSIGNED NOT NULL COMMENT '触发后落地到orders表用同一个id，创建时就分配好',
  uid               BIGINT UNSIGNED NOT NULL,
  symbol            VARCHAR(32) NOT NULL,
  side              ENUM('long','short') NOT NULL,
  action            ENUM('open','close') NOT NULL,
  trigger_price     DECIMAL(18,8) NOT NULL,
  trigger_direction ENUM('gte','lte') NOT NULL COMMENT 'gte=标记价格涨到(或以上)触发价才触发，lte=跌到(或以下)才触发',
  type              ENUM('limit','market') NOT NULL COMMENT '触发后按这个类型提交委托',
  price             DECIMAL(18,8) NOT NULL DEFAULT 0 COMMENT '触发后委托的价格，limit类型必填，market类型恒为0',
  amount            DECIMAL(26,16) NOT NULL,
  leverage          INT UNSIGNED NOT NULL,
  reduce_only       TINYINT(1) NOT NULL DEFAULT 0,
  frozen_margin     DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '创建时冻结的保证金(来自balance的部分)，只有开仓方向的条件单才会冻结',
  frozen_credit     DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '创建时冻结的保证金(来自credit的部分)，只有开仓方向的条件单才会冻结',
  status            ENUM('pending','triggered','canceled') NOT NULL DEFAULT 'pending',
  create_time       BIGINT UNSIGNED NOT NULL,
  update_time       BIGINT UNSIGNED NOT NULL,
  request_id        VARCHAR(64) NULL COMMENT '合作方指定的幂等键，同一uid内唯一，NULL=没传',
  request_hash      VARCHAR(64) NULL COMMENT '请求参数摘要，同一个request_id再次提交时用来判断参数是否一致',
  PRIMARY KEY (order_id),
  UNIQUE KEY uk_conditional_orders_uid_request_id (uid, request_id),
  KEY idx_conditional_orders_uid (uid),
  KEY idx_conditional_orders_status (status)
) ENGINE=InnoDB;

-- 持仓：全仓保证金，一个(uid,symbol,side)一行。idx_positions_symbol供资金费率结算按symbol
-- 批量查仓位用，uk_positions_uid_symbol_side因为uid在最前面覆盖不到这个查询
CREATE TABLE IF NOT EXISTS positions (
  id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  uid               BIGINT UNSIGNED NOT NULL,
  symbol            VARCHAR(32) NOT NULL,
  side              ENUM('long','short') NOT NULL,
  volume            DECIMAL(26,16) NOT NULL DEFAULT 0,
  avg_entry_price   DECIMAL(18,8) NOT NULL DEFAULT 0,
  position_margin   DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '这个仓位占用的保证金，用于按symbol算强平价；这笔钱本身仍然锁在accounts.frozen_margin/frozen_credit里，这里只是同一份锁定按仓位维度的分账',
  credit_margin     DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT 'position_margin里来自credit的部分，平仓释放时要按这个比例精确释放accounts.frozen_credit而不是笼统释放frozen_margin',
  leverage          INT UNSIGNED NOT NULL DEFAULT 1,
  status            ENUM('normal','liquidating','closed') NOT NULL DEFAULT 'normal',
  version           INT UNSIGNED NOT NULL DEFAULT 0,
  update_time       BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_positions_uid_symbol_side (uid, symbol, side),
  KEY idx_positions_symbol (symbol)
) ENGINE=InnoDB;

-- 成交记录
CREATE TABLE IF NOT EXISTS trades (
  trade_id       BIGINT UNSIGNED NOT NULL,
  symbol         VARCHAR(32) NOT NULL,
  price          DECIMAL(18,8) NOT NULL,
  volume         DECIMAL(26,16) NOT NULL,
  buy_order_id   BIGINT UNSIGNED NOT NULL,
  sell_order_id  BIGINT UNSIGNED NOT NULL,
  buy_uid        BIGINT UNSIGNED NOT NULL,
  sell_uid       BIGINT UNSIGNED NOT NULL,
  maker_order_id BIGINT UNSIGNED NOT NULL COMMENT 'buy/sell_order_id其中之一，另一个自然是taker',
  create_time    BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (trade_id),
  KEY idx_trades_symbol (symbol),
  KEY idx_trades_buy_uid (buy_uid),
  KEY idx_trades_sell_uid (sell_uid)
) ENGINE=InnoDB;

-- K线：每个symbol+周期+开盘时间一行，每笔成交实时更新对应的那根K线(UPSERT)，不是查询时
-- 现算——多个周期(1m/5m/15m/1h/4h/1d)各自独立维护一份，不是从1m现场聚合大周期，见
-- docs/kline.md
CREATE TABLE IF NOT EXISTS klines (
  symbol       VARCHAR(32) NOT NULL,
  `interval`   VARCHAR(8) NOT NULL COMMENT '1m/5m/15m/1h/4h/1d',
  open_time    BIGINT UNSIGNED NOT NULL COMMENT '这根K线的开盘时间(毫秒时间戳，按interval对齐)',
  open         DECIMAL(18,8) NOT NULL,
  high         DECIMAL(18,8) NOT NULL,
  low          DECIMAL(18,8) NOT NULL,
  close        DECIMAL(18,8) NOT NULL,
  volume       DECIMAL(26,16) NOT NULL DEFAULT 0,
  trade_count  INT UNSIGNED NOT NULL DEFAULT 0,
  update_time  BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (symbol, `interval`, open_time)
) ENGINE=InnoDB;

-- 资金变动流水(充值扣减/手续费/已实现盈亏/资金费/信用额度发放/结束本轮回收等)，纯审计用途，
-- 不参与任何计算。合作方发起的充值/扣减/发放额度带request_id(幂等键)，同一uid内唯一，靠唯一
-- 索引保证同一个请求只生效一次；系统内部产生的流水(手续费、盈亏等)没有request_id，是NULL
CREATE TABLE IF NOT EXISTS member_transactions (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  uid          BIGINT UNSIGNED NOT NULL,
  symbol       VARCHAR(32) NOT NULL DEFAULT '',
  amount       DECIMAL(26,16) NOT NULL COMMENT '正数=入账，负数=出账',
  type         VARCHAR(32) NOT NULL COMMENT 'deposit/fee/realized_pnl/funding_fee/credit_grant/round_close/liquidation_clear，字符串常量见internal/model',
  create_time  BIGINT UNSIGNED NOT NULL,
  request_id   VARCHAR(64) NULL COMMENT '合作方指定的幂等键，同一uid内唯一，NULL=系统内部产生的流水',
  request_hash VARCHAR(64) NULL COMMENT '请求参数摘要，同一个request_id再次提交时用来判断参数是否一致',
  PRIMARY KEY (id),
  UNIQUE KEY uk_member_transactions_uid_request_id (uid, request_id),
  KEY idx_member_transactions_uid (uid)
) ENGINE=InnoDB;

-- 保险基金：全局唯一一行
CREATE TABLE IF NOT EXISTS insurance_fund (
  id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  balance  DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '允许为负，代表系统亏空，MVP阶段只记日志告警不熔断',
  version  INT UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
) ENGINE=InnoDB;

INSERT INTO insurance_fund (id, balance) VALUES (1, 0) ON DUPLICATE KEY UPDATE id = id;

CREATE TABLE IF NOT EXISTS insurance_fund_ledger (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  symbol        VARCHAR(32) NOT NULL DEFAULT '',
  uid           BIGINT UNSIGNED NOT NULL DEFAULT 0,
  position_id   BIGINT UNSIGNED NOT NULL DEFAULT 0,
  amount        DECIMAL(26,16) NOT NULL COMMENT '正数=强平盈余/其它注入，负数=穿仓垫付',
  balance_after DECIMAL(26,16) NOT NULL,
  remark        VARCHAR(255) NOT NULL DEFAULT '',
  create_time   BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB;

-- 资金费率结算历史：每个symbol每个结算周期一行，funding_time是对齐到整点边界的周期时间戳，
-- 同一个symbol同一个funding_time只会结算一次(FundingService.SettleIfDue靠查这张表判断
-- 有没有结算过)，同时也是给客户端展示历史费率用的
CREATE TABLE IF NOT EXISTS funding_rate_history (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  symbol        VARCHAR(32) NOT NULL,
  funding_time  BIGINT UNSIGNED NOT NULL COMMENT '结算周期对齐后的毫秒时间戳',
  rate          DECIMAL(10,6) NOT NULL COMMENT '这个周期的资金费率，已经clamp到funding_rate_cap',
  mark_price    DECIMAL(18,8) NOT NULL,
  index_price   DECIMAL(18,8) NOT NULL,
  create_time   BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_funding_rate_history_symbol_time (symbol, funding_time)
) ENGINE=InnoDB;

-- Kafka消息级去重：给at-least-once语义下的重复投递做最后一道防线，按(consumer_group,topic,
-- partition,offset)这个坐标标记"已处理"，处理前先INSERT IGNORE占位，插入失败(0行受影响)
-- 说明这条消息之前已经处理过，直接跳过业务逻辑。见docs/message-dedup.md。consumer_group在
-- 主键里(不是只按topic+partition+offset)是因为engine分片(docs/engine-sharding.md)之后，
-- 同一条消息会被多个engine实例各自独立的consumer group各消费一次(fan-out，不是Kafka原生的
-- 分区负载均衡)，去重必须按"这个consumer group有没有处理过"分别判断，不能用一个全局共享的
-- 去重状态——否则先处理到这条消息的那个实例会把其它本来也需要独立处理这条消息的实例给挡住。
-- create_time上的索引供定期清理过期记录用，不然这张表会无限增长
CREATE TABLE IF NOT EXISTS processed_messages (
  consumer_group VARCHAR(191) NOT NULL,
  topic          VARCHAR(191) NOT NULL,
  `partition`    INT NOT NULL,
  `offset`       BIGINT NOT NULL,
  create_time    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (consumer_group, topic, `partition`, `offset`),
  KEY idx_processed_messages_create_time (create_time)
) ENGINE=InnoDB;

-- 结束本轮(CloseRound)在engine分片部署下的跨分片完成度追踪：一个uid结束某一round时涉及到
-- 的每个symbol各占一行(done=0)，负责这个symbol的分片实例做完自己那部分(撤单+强平)之后
-- 把done改成1，全部symbol都done了才能做"清零credit+round前进"这个只能发生一次的最终结算——
-- 见docs/engine-sharding.md"结束本轮的异步化"一节。单实例部署(没配PERP_ENGINE_SYMBOLS)下
-- 这张表也会用到，只是每次都只有当前实例自己在读写，退化成一个进度记录，没有实际的跨进程协调
CREATE TABLE IF NOT EXISTS round_close_progress (
  uid          BIGINT UNSIGNED NOT NULL,
  round        BIGINT UNSIGNED NOT NULL,
  symbol       VARCHAR(32) NOT NULL,
  done         TINYINT(1) NOT NULL DEFAULT 0,
  create_time  BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (uid, round, symbol)
) ENGINE=InnoDB;

-- 演示用初始合约配置，方便本地对照测试。price_tick/volume_step配成跟价格/数量精度一致(10^-scale)：
-- 通过GET /contract/list、/contract/detail对外提供，对接方靠它们知道输入框的步进，服务端也据此拒绝位数超标的订单。
-- 重复执行这个文件不会覆盖已经调过的配置(ON DUPLICATE KEY UPDATE是no-op)；已经建过库的环境要手动UPDATE
INSERT INTO coins (symbol, base_coin_scale, price_scale, maker_fee, taker_fee, min_volume, price_tick, volume_step)
VALUES
  ('BTCUSDT', 3, 1, 0.000200, 0.000500, 0.001, 0.1, 0.001),
  ('ETHUSDT', 2, 2, 0.000200, 0.000500, 0.01, 0.01, 0.01)
ON DUPLICATE KEY UPDATE symbol = symbol;

-- 保证金分档：照抄币安BTCUSDT/ETHUSDT永续合约当前的杠杆分层(2026-09-24从币安公开接口取的实时数据，
-- 币安会不定期调整，这里不会跟着自动同步，要更新就手动改这张表)。maintenance_amount(速算扣除数)
-- 直接用的是币安接口返回的cumFastMaintenanceAmount，公式跟本表头注释一致，天然跨档位连续，
-- 不用重新手工推算。唯一跟币安不同的地方：币安最后一档(tier 12)是有限的名义价值上限(BTC 18亿/
-- ETH 12亿)，超过就不允许开仓；这里按本系统的约定把最后一档的max_notional改成0(不限)——
-- PositionService.TierFor要求最后一档必须是0，不然会把整个分档配置当成"没配置"处理(开仓被拒)，
-- 见risk-limit-tiers.md
INSERT INTO risk_limit_tiers (symbol, tier, max_notional, maintenance_margin_rate, maintenance_amount, max_leverage)
VALUES
  ('BTCUSDT', 1,  300000,      0.004000, 0,         150),
  ('BTCUSDT', 2,  800000,      0.005000, 300,       100),
  ('BTCUSDT', 3,  3000000,     0.006500, 1500,      75),
  ('BTCUSDT', 4,  12000000,    0.010000, 12000,     50),
  ('BTCUSDT', 5,  70000000,    0.020000, 132000,    25),
  ('BTCUSDT', 6,  100000000,   0.025000, 482000,    20),
  ('BTCUSDT', 7,  230000000,   0.050000, 2982000,   10),
  ('BTCUSDT', 8,  480000000,   0.100000, 14482000,  5),
  ('BTCUSDT', 9,  600000000,   0.125000, 26482000,  4),
  ('BTCUSDT', 10, 800000000,   0.150000, 41482000,  3),
  ('BTCUSDT', 11, 1200000000,  0.250000, 121482000, 2),
  ('BTCUSDT', 12, 0,           0.500000, 421482000, 1),
  ('ETHUSDT', 1,  300000,      0.004000, 0,         150),
  ('ETHUSDT', 2,  800000,      0.005000, 300,       100),
  ('ETHUSDT', 3,  3000000,     0.006500, 1500,      75),
  ('ETHUSDT', 4,  12000000,    0.010000, 12000,     50),
  ('ETHUSDT', 5,  50000000,    0.020000, 132000,    25),
  ('ETHUSDT', 6,  65000000,    0.025000, 382000,    20),
  ('ETHUSDT', 7,  150000000,   0.050000, 2007000,   10),
  ('ETHUSDT', 8,  320000000,   0.100000, 9507000,   5),
  ('ETHUSDT', 9,  400000000,   0.125000, 17507000,  4),
  ('ETHUSDT', 10, 530000000,   0.150000, 27507000,  3),
  ('ETHUSDT', 11, 800000000,   0.250000, 80507000,  2),
  ('ETHUSDT', 12, 0,           0.500000, 280507000, 1)
ON DUPLICATE KEY UPDATE symbol = symbol;
