# K线

## 实时聚合，不是查询时现算

每笔成交（`EngineService.settleOneFill`，紧跟在`trades.Insert`后面）实时更新对应的K线，
不是等查询的时候才从`trades`表现场`GROUP BY`聚合。理由：

- 查询时现算对每次请求都要扫一遍时间范围内的全部原始成交记录，随着历史数据增长会越来越慢
- 实时更新是简单的单行UPSERT（`INSERT ... ON DUPLICATE KEY UPDATE`），成本固定、不随
  历史数据量增长

`klines`表一行代表一个`(symbol, interval, open_time)`的K线，`open_time`是成交时间按
`interval`对齐后的分桶起点（`floor(成交时间毫秒 / 周期毫秒) * 周期毫秒`）。

## 多个周期各自独立维护，不是从小周期现场聚合大周期

支持的周期固定为`1m`/`5m`/`15m`/`1h`/`4h`/`1d`（`model.AllKlineIntervals`），每笔成交
会**同时**更新这六个周期各自对应的那一根K线——不是只维护1m、大周期查询时再从1m现场拼。
这是空间换时间：写入时多做几份K线数据（都是O(1)的单行更新，成本很低），换来查询任意
周期都是直接读、不需要现场聚合的O(1)体验。

## 一条多行UPSERT，不是六次单独的DB往返

`KlineService.RecordTrade`不会对六个周期各发一次UPSERT——那样等于让成交结算这条热路径
里的每一笔成交都串行等六次DB round-trip，而K线只是辅助展示数据（本身不参与任何交易
正确性判断，见下一节）。改成一次批量`INSERT ... VALUES (...),(...),...(六行) ON
DUPLICATE KEY UPDATE`（`KlineRepo.UpsertBatch`），六个周期一条SQL、一次往返写完：

```sql
INSERT INTO klines (symbol, `interval`, open_time, open, high, low, close, volume, trade_count, update_time)
VALUES
  (?, '1m', ?, ?, ?, ?, ?, ?, 1, ?),
  (?, '5m', ?, ?, ?, ?, ?, ?, 1, ?),
  ... -- 15m/1h/4h/1d同理，一共6行
ON DUPLICATE KEY UPDATE
  high = GREATEST(high, VALUES(high)),
  low = LEAST(low, VALUES(low)),
  close = VALUES(close),
  volume = volume + VALUES(volume),
  trade_count = trade_count + 1,
  update_time = VALUES(update_time)
```

`VALUES(列名)`取的是**这一行**本来要写入的值（这里是成交价/成交量），不是当前表里已有
的值，也不会跟同一条语句里其它行的值混在一起——多行UPSERT里每一行的冲突处理是各自独立
计算的，已经用真实MySQL实例验证过。一条SQL原子完成"这根K线不存在就新建
(open=high=low=close=成交价)，存在就按GREATEST/LEAST规则更新"，不是"先查是否存在、
再判断插入还是更新"的两步走（那样在并发下会有竞态：两笔几乎同时的成交都判断出"不存在"，
都尝试INSERT，后一个因为主键冲突失败）。

## 为什么K线更新失败不影响成交结算

`KlineService.RecordTrade`批量UPSERT失败只记日志，不返回error、不影响调用方
（`settleOneFill`）的其它逻辑——K线是行情展示用的辅助数据，不是交易正确性的一部分，没有
理由因为这个让整笔成交结算失败。跟`markPrice.UpdateFromTrade`失败时只记`[WARN]`日志是
同一个道理。

## 查询：`GET /kline?symbol=BTCUSDT&interval=1m&limit=200`

在`contract-api`（不是`contract-engine`）——跟深度查询不同，K线数据完全存在MySQL里，
不依赖内存中的订单簿状态，两个进程都能读到，放在对外业务API上更自然。`interval`必须是
六个固定值之一，其它字符串一律拒绝。返回按开盘时间**升序**（从旧到新），常见的画图/回放
习惯。
