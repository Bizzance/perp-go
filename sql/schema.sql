-- framework-go 第一期(MVP)表结构，独立新库(建议库名 perpgo)。
-- 账户(无信用额度)、合约配置与保证金分档、委托/持仓/成交、标记价格、指数价格与资金费率结算、
-- 保险基金。不建：信用账户、条件单、逐仓模式——这些是后续阶段。

CREATE DATABASE IF NOT EXISTS perpgo DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE perpgo;

-- 合约账户：一个uid一行，全仓保证金(账户级别共享资金池，不做逐仓)
CREATE TABLE IF NOT EXISTS accounts (
  id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  uid            BIGINT UNSIGNED NOT NULL,
  available      DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '可用余额，可能为负(全仓下用持仓浮盈当买力借出去的部分)',
  frozen_margin  DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '挂单冻结保证金',
  version        INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '乐观锁版本号，MVP阶段原子UPDATE为主，这个字段先留着备用',
  created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_accounts_uid (uid)
) ENGINE=InnoDB;

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
  side           ENUM('LONG','SHORT') NOT NULL,
  action         ENUM('OPEN','CLOSE') NOT NULL,
  type           ENUM('LIMIT','MARKET') NOT NULL,
  price          DECIMAL(18,8) NOT NULL DEFAULT 0 COMMENT '市价单恒为0',
  amount         DECIMAL(26,16) NOT NULL COMMENT '标的币数量',
  traded_amount  DECIMAL(26,16) NOT NULL DEFAULT 0,
  avg_deal_price DECIMAL(18,8) NOT NULL DEFAULT 0,
  frozen_margin  DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '这笔委托占用的冻结保证金',
  leverage       INT UNSIGNED NOT NULL,
  reduce_only    TINYINT(1) NOT NULL DEFAULT 0,
  liquidation    TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否强平单——结算后要走保险基金穿仓/盈余清算分支',
  status         ENUM('NEW','PARTIALLY_FILLED','FILLED','CANCELED') NOT NULL DEFAULT 'NEW',
  create_time    BIGINT UNSIGNED NOT NULL COMMENT '毫秒时间戳，撮合引擎按这个做时间优先排序',
  update_time    BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (order_id),
  KEY idx_orders_uid (uid),
  KEY idx_orders_symbol_status (symbol, status)
) ENGINE=InnoDB;

-- 持仓：全仓保证金，一个(uid,symbol,side)一行
CREATE TABLE IF NOT EXISTS positions (
  id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  uid               BIGINT UNSIGNED NOT NULL,
  symbol            VARCHAR(32) NOT NULL,
  side              ENUM('LONG','SHORT') NOT NULL,
  volume            DECIMAL(26,16) NOT NULL DEFAULT 0,
  avg_entry_price   DECIMAL(18,8) NOT NULL DEFAULT 0,
  position_margin   DECIMAL(26,16) NOT NULL DEFAULT 0 COMMENT '记账用名义值，全仓下不是真锁定的钱',
  leverage          INT UNSIGNED NOT NULL DEFAULT 1,
  status            ENUM('NORMAL','LIQUIDATING','CLOSED') NOT NULL DEFAULT 'NORMAL',
  version           INT UNSIGNED NOT NULL DEFAULT 0,
  update_time       BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_positions_uid_symbol_side (uid, symbol, side)
) ENGINE=InnoDB;

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
