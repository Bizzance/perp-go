# K线

## K线的来源：币安（生产）或我们自己的成交（默认）

`PERP_KLINE_SOURCE`决定K线从哪来，`contract-api`和`contract-engine`必须配成一样的：

| 取值             | K线怎么来                                                                                         | 用在哪                                       |
|------------------|---------------------------------------------------------------------------------------------------|----------------------------------------------|
| `trades`（默认） | 用我们自己的成交实时生成，见下面"实时聚合"                                                        | 本地开发、只用模拟客户端做市（靠对敲成交画K线）的测试环境 |
| `external`       | **只来自币安**：orderbook-sync定时拉币安的K线，通过`POST /kline/sync`写进来；我们自己的成交**不再写K线** | 生产环境（用orderbook-sync）                 |

`external`时：K线、24h涨跌/最高最低/成交量、最新价都是币安的数据。K线里的成交量和成交笔数是**币安全市场的**，不是我们平台的。
我们自己的成交不写K线，否则同一根K线会被我们的成交价和币安的数据混在一起，成交量还会先加后被覆盖回去。
合作方订阅的WebSocket K线频道（`kline:{symbol}:{interval}`）不变，`POST /kline/sync`写入后把**变了的**K线推出去。

### `POST /kline/sync`（运营接口，orderbook-sync调用）

请求体`{symbol, interval, candles: [{openTime, open, high, low, close, volume, tradeCount}]}`，一次1到200根（受请求体64KB上限限制，
补历史要多根就分批推）：

- **整根覆盖**：存在就把开高低收、成交量、成交笔数都换成这一批的值（不是跟已有的合并），不存在就新建；重复推送结果不变
- **整批校验，任何一根不合法整批拒绝、一根都不写**：`openTime`必须是这个周期的整点开盘时间且严格递增、不能在未来，开高低收必须大于0，
  最高价不低于开/收/最低价，最低价不高于开/收，成交量不能为负
- 只推变了的：每几秒同步一次最近几根，绝大部分没变，全推的话订阅者每次都收到一堆重复数据
- 只在`PERP_KLINE_SOURCE=external`时可用，否则返回`kline_source_not_external`，一根都不写

### 最新价

`GET /market/ticker`的`lastPrice`：`external`时是最近一根1分钟K线的收盘价（币安的最新价，几秒内更新），**不用我们自己的成交价**——
我们自己成交少的时候最后一笔成交价会停很久，跟币安差很多；`trades`时是最新一笔成交价，还没有成交时退回最近一根K线的收盘价。

24h统计是最近24根1小时K线聚合（含当前这一根），不是严格滚动的24小时，所以跟币安的24hr行情有约1%的差异，见 [api.md](api.md)。

## 实时聚合，不是查询时现算（`trades`来源）

每笔成交（`EngineService.settleOneFill`，紧跟在`trades.Insert`后面）实时更新对应的K线，
不是等查询的时候才从`trades`表现场`GROUP BY`聚合。理由：

- 查询时现算对每次请求都要扫一遍时间范围内的全部原始成交记录，随着历史数据增长会越来越慢
- 实时更新是简单的单行UPSERT（`INSERT ... ON DUPLICATE KEY UPDATE`），成本固定、不随
  历史数据量增长

`klines`表一行代表一个`(symbol, interval, open_time)`的K线，`open_time`是成交时间按
`interval`对齐后的分桶起点（`floor(成交时间毫秒 / 周期毫秒) * 周期毫秒`）。

## 多个周期各自独立维护，不是从小周期现场聚合大周期

支持的周期固定为`1m`/`5m`/`15m`/`1h`/`4h`/`1d`（`model.AllKlineIntervals`），每笔成交
会 **同时**更新这六个周期各自对应的那一根K线——不是只维护1m、大周期查询时再从1m现场拼。
这是空间换时间：写入时多做几份K线数据（都是O (1)的单行更新，成本很低），换来查询任意
周期都是直接读、不需要现场聚合的O (1)体验。

## 一条多行UPSERT，不是六次单独的DB往返

`KlineService.RecordTrade`不会对六个周期各发一次UPSERT——那样等于让成交结算这条热路径
里的每一笔成交都串行等六次DB round-trip，而K线只是辅助展示数据（本身不参与任何交易
正确性判断，见下一节）。改成一次批量`INSERT ... VALUES (...),(...),...(六行) ON
DUPLICATE KEY UPDATE`（`KlineRepo.UpsertBatch`），六个周期一条SQL、一次往返写完：

```sql
INSERT INTO klines (symbol, `interval`, open_time, open, high, low, close, volume, trade_count, update_time)
VALUES (?, '1m', ?, ?, ?, ?, ?, ?, 1, ?),
       (?, '5m', ?, ?, ?, ?, ?, ?, 1, ?), ... -- 15m/1h/4h/1d同理，一共6行
    ON DUPLICATE KEY
UPDATE
    high = GREATEST(high, VALUES (high)),
    low = LEAST(low, VALUES (low)),
    close =
VALUES (close), volume = volume +
VALUES (volume), trade_count = trade_count + 1, update_time =
VALUES (update_time)
```

`VALUES(列名)`取的是 **这一行**本来要写入的值（这里是成交价/成交量），不是当前表里已有
的值，也不会跟同一条语句里其它行的值混在一起——多行UPSERT里每一行的冲突处理是各自独立
计算的，已经用真实MySQL实例验证过。一条SQL原子完成"这根K线不存在就新建 (open=high=low=close=成交价)
，存在就按GREATEST/LEAST规则更新"，不是"先查是否存在、
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
六个固定值之一，其它字符串一律拒绝。返回按开盘时间 **升序**（从旧到新），常见的画图/回放
习惯。
