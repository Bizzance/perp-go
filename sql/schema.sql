-- framework-go 第一期(MVP)表结构，独立新库(建议库名 perpgo)。
-- 账户(含信用额度)、合约配置与保证金分档、委托/条件单/持仓/成交、标记价格、指数价格与
-- 资金费率结算、保险基金。不建：逐仓模式——按项目约定只支持全仓，明确不做。

CREATE DATABASE IF NOT EXISTS perpgo DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE perpgo;

-- 合约账户：一个uid一行，全仓保证金(账户级别共享资金池，不做逐仓)
CREATE TABLE IF NOT EXISTS accounts (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  uid            BIGINT UNSIGNED NOT NULL,
  is_insured     TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否投保：0-不投保，1-投保',
  round          BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '轮数',
  credit         DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '信用额度余额，只能用于开仓保证金，不能转出/提现',
  available      DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '可用余额，可能为负(全仓下用持仓浮盈当买力借出去的部分)',
  frozen_margin  DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '挂单冻结保证金(来自available的部分)',
  frozen_credit  DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '挂单冻结保证金(来自credit的部分)，必须单独记账才能精确退回',
  version        INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '乐观锁版本号，MVP阶段原子UPDATE为主，这个字段先留着备用',
  created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_accounts_uid (uid)
) ENGINE=InnoDB;

-- 给已经建过表的库补上is_insured/round/credit这几列，理由跟下面coins表那几条ALTER一样：
-- CREATE TABLE IF NOT EXISTS对已存在的accounts表是no-op，不会补上这几个新列
SET @sql := (SELECT IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'is_insured') = 0,
  'ALTER TABLE accounts ADD COLUMN is_insured TINYINT(1) NOT NULL DEFAULT 0 COMMENT ''是否投保：0-不投保，1-投保'' AFTER uid',
  'SELECT 1'
));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql := (SELECT IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'round') = 0,
  'ALTER TABLE accounts ADD COLUMN round BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT ''轮数'' AFTER is_insured',
  'SELECT 1'
));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql := (SELECT IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'credit') = 0,
  'ALTER TABLE accounts ADD COLUMN credit DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT ''信用额度余额，只能用于开仓保证金，不能转出/提现'' AFTER round',
  'SELECT 1'
));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql := (SELECT IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'frozen_credit') = 0,
  'ALTER TABLE accounts ADD COLUMN frozen_credit DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT ''挂单冻结保证金(来自credit的部分)，必须单独记账才能精确退回'' AFTER frozen_margin',
  'SELECT 1'
));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 合约配置：维持保证金率/最大杠杆按名义价值分档，见下面的risk_limit_tiers表
CREATE TABLE IF NOT EXISTS coins (
  symbol                    VARCHAR(32) NOT NULL,
  base_coin_scale           TINYINT UNSIGNED NOT NULL DEFAULT 8 COMMENT '标的币数量精度(小数位数)',
  price_scale               TINYINT UNSIGNED NOT NULL DEFAULT 2 COMMENT '价格展示精度(小数位数)',
  enable                    TINYINT(1) NOT NULL DEFAULT 1,
  maker_fee                 DECIMAL(8,6) NOT NULL DEFAULT 0.000200,
  taker_fee                 DECIMAL(8,6) NOT NULL DEFAULT 0.000500,
  price_tick                DECIMAL(18,8) NOT NULL DEFAULT 0 COMMENT '价格最小变动单位，0=不校验',
  volume_step                DECIMAL(18,8) NOT NULL DEFAULT 0 COMMENT '数量步长，0=不校验',
  min_volume                DECIMAL(18,8) NOT NULL DEFAULT 0,
  max_volume                DECIMAL(18,8) NOT NULL DEFAULT 0 COMMENT '0=不限制',
  funding_interval_hours    INT UNSIGNED NOT NULL DEFAULT 8 COMMENT '资金费率结算周期(小时)，对齐到从0点起的整点边界',
  funding_rate_cap          DECIMAL(10,6) NOT NULL DEFAULT 0.007500 COMMENT '资金费率上下限，0=不限制',
  price_protection_ratio    DECIMAL(8,6) NOT NULL DEFAULT 0.050000 COMMENT '限价单允许偏离标记/指数价格的最大比例，0=不校验',
  created_at                DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (symbol)
) ENGINE=InnoDB;

-- 这个项目没有独立的迁移工具，schema.sql就是唯一的建表脚本——上面CREATE TABLE IF NOT EXISTS
-- 对已经存在的coins表是no-op，不会补上后续这几次迭代新增/删除的列，直接重跑这个文件在一个
-- 已经建过库的环境上会导致代码里SELECT的列在数据库里不存在。MySQL(不是MariaDB)的ALTER TABLE
-- 不支持ADD/DROP COLUMN IF NOT EXISTS/IF EXISTS这种语法(实测8.4.11直接报语法错误)，用
-- information_schema查列是否存在、拼接成动态SQL再PREPARE/EXECUTE来模拟同样的效果，把coins表
-- 补齐到跟上面CREATE TABLE定义一致，让这个文件对"全新库"和"已经建过表、只是列结构落后"两种
-- 情况都能安全重复执行
SET @sql := (SELECT IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'coins' AND COLUMN_NAME = 'funding_interval_hours') = 0,
  'ALTER TABLE coins ADD COLUMN funding_interval_hours INT UNSIGNED NOT NULL DEFAULT 8 COMMENT ''资金费率结算周期(小时)，对齐到从0点起的整点边界''',
  'SELECT 1'
));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql := (SELECT IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'coins' AND COLUMN_NAME = 'funding_rate_cap') = 0,
  'ALTER TABLE coins ADD COLUMN funding_rate_cap DECIMAL(10,6) NOT NULL DEFAULT 0.007500 COMMENT ''资金费率上下限，0=不限制''',
  'SELECT 1'
));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql := (SELECT IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'coins' AND COLUMN_NAME = 'price_protection_ratio') = 0,
  'ALTER TABLE coins ADD COLUMN price_protection_ratio DECIMAL(8,6) NOT NULL DEFAULT 0.050000 COMMENT ''限价单允许偏离标记/指数价格的最大比例，0=不校验''',
  'SELECT 1'
));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql := (SELECT IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'coins' AND COLUMN_NAME = 'max_leverage') > 0,
  'ALTER TABLE coins DROP COLUMN max_leverage',
  'SELECT 1'
));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql := (SELECT IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'coins' AND COLUMN_NAME = 'maintenance_margin_rate') > 0,
  'ALTER TABLE coins DROP COLUMN maintenance_margin_rate',
  'SELECT 1'
));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

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

-- 委托单
CREATE TABLE IF NOT EXISTS orders (
  order_id       BIGINT UNSIGNED NOT NULL COMMENT '雪花ID或类似的分布式唯一ID，应用层生成',
  uid            BIGINT UNSIGNED NOT NULL,
  symbol         VARCHAR(32) NOT NULL,
  side           ENUM('long','short') NOT NULL,
  action         ENUM('open','close') NOT NULL,
  type           ENUM('limit','market') NOT NULL,
  price          DECIMAL(18,8) NOT NULL DEFAULT 0 COMMENT '市价单恒为0',
  amount         DECIMAL(26,16) NOT NULL COMMENT '标的币数量',
  traded_amount  DECIMAL(26,16) NOT NULL DEFAULT 0,
  avg_deal_price DECIMAL(18,8) NOT NULL DEFAULT 0,
  frozen_margin  DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '这笔委托占用的冻结保证金(来自available的部分)',
  frozen_credit  DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '这笔委托占用的冻结保证金(来自credit的部分)',
  leverage       INT UNSIGNED NOT NULL,
  reduce_only    TINYINT(1) NOT NULL DEFAULT 0,
  liquidation    TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否强平单——结算后要走保险基金穿仓/盈余清算分支',
  status         ENUM('open','partially_filled','filled','canceled','rejected') NOT NULL DEFAULT 'open',
  create_time    BIGINT UNSIGNED NOT NULL COMMENT '毫秒时间戳，撮合引擎按这个做时间优先排序',
  update_time    BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (order_id),
  KEY idx_orders_uid (uid),
  KEY idx_orders_symbol_status (symbol, status)
) ENGINE=InnoDB;

-- 给已经建过表的库把side/action/type/status这几个ENUM列改成小写取值(对齐合作方API的
-- 大小写约定)，同时给status补上rejected这个新状态。MODIFY COLUMN在这里是安全的幂等操作：
-- ENUM在存储层是按位置索引存的，只要新枚举列表里各个取值的先后顺序跟旧的一一对应(这里只是
-- 把每个值原地改成小写、在末尾追加rejected)，已有数据不需要任何转换，读出来的值自动就是
-- 新的小写形式——不是"改列表定义"和"数据"两件事，是同一件事
ALTER TABLE orders MODIFY COLUMN side ENUM('long','short') NOT NULL;
ALTER TABLE orders MODIFY COLUMN action ENUM('open','close') NOT NULL;
ALTER TABLE orders MODIFY COLUMN type ENUM('limit','market') NOT NULL;
ALTER TABLE orders MODIFY COLUMN status ENUM('open','partially_filled','filled','canceled','rejected') NOT NULL DEFAULT 'open';

SET @sql := (SELECT IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'orders' AND COLUMN_NAME = 'frozen_credit') = 0,
  'ALTER TABLE orders ADD COLUMN frozen_credit DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT ''这笔委托占用的冻结保证金(来自credit的部分)'' AFTER frozen_margin',
  'SELECT 1'
));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 条件单(止盈止损/条件开仓)：创建时不进撮合引擎的订单簿，只是"记着一个触发条件"，
-- 由contract-engine定时扫描标记价格，触发了才转成一笔真正的委托(落到orders表)按正常流程
-- 提交撮合。order_id在创建条件单的时候就分配好，触发后落地到orders表也用这同一个id，
-- 客户端不需要另外维护"条件单id"和"委托id"两套编号
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
  frozen_margin     DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '创建时冻结的保证金(来自available的部分)，只有开仓方向的条件单才会冻结',
  frozen_credit     DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '创建时冻结的保证金(来自credit的部分)，只有开仓方向的条件单才会冻结',
  status            ENUM('pending','triggered','canceled') NOT NULL DEFAULT 'pending',
  create_time       BIGINT UNSIGNED NOT NULL,
  update_time       BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (order_id),
  KEY idx_conditional_orders_uid (uid),
  KEY idx_conditional_orders_status (status)
) ENGINE=InnoDB;

-- 持仓：全仓保证金，一个(uid,symbol,side)一行
CREATE TABLE IF NOT EXISTS positions (
  id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  uid               BIGINT UNSIGNED NOT NULL,
  symbol            VARCHAR(32) NOT NULL,
  side              ENUM('long','short') NOT NULL,
  volume            DECIMAL(26,16) NOT NULL DEFAULT 0,
  avg_entry_price   DECIMAL(18,8) NOT NULL DEFAULT 0,
  position_margin   DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '记账用名义值，全仓下不是真锁定的钱',
  credit_margin     DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT 'position_margin里来自credit的部分，平仓释放时要按这个比例精确退回credit而不是笼统退available',
  leverage          INT UNSIGNED NOT NULL DEFAULT 1,
  status            ENUM('normal','liquidating','closed') NOT NULL DEFAULT 'normal',
  version           INT UNSIGNED NOT NULL DEFAULT 0,
  update_time       BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_positions_uid_symbol_side (uid, symbol, side)
) ENGINE=InnoDB;

-- 同上，把positions的side/status也改成小写取值，理由和安全性说明见orders表那几条MODIFY
ALTER TABLE positions MODIFY COLUMN side ENUM('long','short') NOT NULL;
ALTER TABLE positions MODIFY COLUMN status ENUM('normal','liquidating','closed') NOT NULL DEFAULT 'normal';

SET @sql := (SELECT IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'positions' AND COLUMN_NAME = 'credit_margin') = 0,
  'ALTER TABLE positions ADD COLUMN credit_margin DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT ''position_margin里来自credit的部分，平仓释放时要按这个比例精确退回credit而不是笼统退available'' AFTER position_margin',
  'SELECT 1'
));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 资金费率结算按symbol批量查仓位用，uk_positions_uid_symbol_side因为uid在最前面覆盖不到
-- 这个查询。MySQL的CREATE INDEX不支持IF NOT EXISTS(实测报语法错误，跟上面coins表ALTER
-- 同样的原因)，用information_schema.STATISTICS查索引是否存在来模拟，对已经建过表的库
-- 能补上这个索引，不能指望CREATE TABLE IF NOT EXISTS生效
SET @sql := (SELECT IF(
  (SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'positions' AND INDEX_NAME = 'idx_positions_symbol') = 0,
  'CREATE INDEX idx_positions_symbol ON positions (symbol)',
  'SELECT 1'
));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

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

-- 资金变动流水(注资/手续费/已实现盈亏/强平清算)，纯审计用途，不参与任何计算
CREATE TABLE IF NOT EXISTS member_transactions (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  uid          BIGINT UNSIGNED NOT NULL,
  symbol       VARCHAR(32) NOT NULL DEFAULT '',
  amount       DECIMAL(26,16) NOT NULL COMMENT '正数=入账，负数=出账',
  type         VARCHAR(32) NOT NULL COMMENT 'DEPOSIT/FEE/REALIZED_PNL/LIQUIDATION_CLEAR等，字符串常量见internal/model',
  create_time  BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (id),
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

-- 老版本的processed_messages表主键是(topic,partition,offset)，没有consumer_group列——
-- 补列+把主键换成新的四元组，让这个文件对"全新库"和"已经建过旧版表"两种情况都能安全重复执行
SET @sql := (SELECT IF(
  (SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'processed_messages' AND COLUMN_NAME = 'consumer_group') = 0,
  'ALTER TABLE processed_messages ADD COLUMN consumer_group VARCHAR(191) NOT NULL DEFAULT ''''',
  'SELECT 1'
));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql := (SELECT IF(
  (SELECT COUNT(*) FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'processed_messages' AND CONSTRAINT_NAME = 'PRIMARY' AND COLUMN_NAME = 'consumer_group') = 0,
  'ALTER TABLE processed_messages DROP PRIMARY KEY, ADD PRIMARY KEY (consumer_group, topic, `partition`, `offset`)',
  'SELECT 1'
));
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

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

-- 演示用初始合约配置，方便本地对照测试
INSERT INTO coins (symbol, base_coin_scale, price_scale, maker_fee, taker_fee, min_volume)
VALUES
  ('BTCUSDT', 3, 1, 0.000200, 0.000500, 0.001),
  ('ETHUSDT', 2, 2, 0.000200, 0.000500, 0.01)
ON DUPLICATE KEY UPDATE symbol = symbol;

-- 演示用保证金分档：maintenance_amount(速算扣除数)是按"跨档位维持保证金连续"手工算好的常量，
-- 不是运行时推导——每一档的值=上一档在其上限名义价值处的维持保证金，减去本档费率在同一个
-- 名义价值下算出来的数值，公式推导见表头注释
INSERT INTO risk_limit_tiers (symbol, tier, max_notional, maintenance_margin_rate, maintenance_amount, max_leverage)
VALUES
  ('BTCUSDT', 1, 50000,      0.004000, 0,      125),
  ('BTCUSDT', 2, 250000,     0.005000, 50,     100),
  ('BTCUSDT', 3, 1000000,    0.006500, 425,    50),
  ('BTCUSDT', 4, 5000000,    0.010000, 3925,   20),
  ('BTCUSDT', 5, 0,          0.025000, 78925,  5),
  ('ETHUSDT', 1, 50000,      0.004000, 0,      100),
  ('ETHUSDT', 2, 250000,     0.005000, 50,     75),
  ('ETHUSDT', 3, 1000000,    0.006500, 425,    40),
  ('ETHUSDT', 4, 0,          0.015000, 8925,   10)
ON DUPLICATE KEY UPDATE symbol = symbol;
