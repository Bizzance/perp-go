# HTTP接口

面向合作方（客户端）的对接文档。合作方把本系统当作自己系统里专门负责U本位合约交易的子系统，
通过下面这些接口开户入金、下单、查询、结束本轮。本文所有响应示例都是从真实运行的服务抓取的。

## 对接须知

### 服务与端口

| 进程              | 默认端口 | 提供什么                                           |
|-------------------|----------|----------------------------------------------------|
| `contract-api`    | `:7001`  | 本文全部业务接口 + `GET /ws`（WebSocket推送）      |
| `contract-engine` | `:7002`  | 仅 `GET /depth`（订单簿只存在这个进程的内存里）    |

### 鉴权

**全部接口都要求带请求签名**（API Key + HMAC-SHA256，交易所通行的做法），只有 `GET /health` 免鉴权。
每个请求带四个头：

```
X-Api-Key    密钥标识
X-Timestamp  毫秒时间戳（与服务器相差不能超过 30 秒）
X-Nonce      随机串，16~64 位，每个请求必须不同（重试也要换）
X-Signature  HMAC-SHA256(secret, timestamp\nnonce\nMETHOD\npath\nrawQuery\nsha256hex(body)) 的十六进制小写
```

签名算法、固定测试向量、Python 和 bash 示例、权限范围、错误码见 [auth-design.md](auth-design.md)；
`deploy/apisign.sh` 可以直接拿来调用。密钥分两种权限范围：`trade`（交易和查询）和 `ops`（加钱扣钱、发信用额度、
设投保、冻结/解冻账户、喂指数价），越权返回 `forbidden`。`uid` 仍然是独立的请求参数。

### 请求约定

- GET 查询接口的参数走 query string；POST 写接口统一用 JSON body，`Content-Type: application/json`
- `uid`是合作方自己体系里的用户ID（正整数，`uint64`），本系统直接沿用、不另外分配编号。**账户必须先用
  `POST /account/create`创建**，其它接口遇到没创建过的`uid`一律返回`account_not_found`，不会替你悄悄建
- 金额、价格、数量这类小数：请求里可以传 JSON 数字（`65000`、`0.1`）也可以传字符串（`"0.1"`），
  服务端按十进制精确解析，不会经过浮点数
- 枚举字段全部是**小写**且大小写敏感（`"LONG"`会被拒绝）

### 响应格式

**HTTP 状态码固定返回 200**，成功还是失败看 body 里的 `code`：

```json
{ "code": 200, "message": "success", "data": { } }
```

```json
{ "code": 400, "errCode": "insufficient_margin", "message": "可用余额不足，无法冻结保证金" }
```

- `code`：粗粒度分类，`200`成功 / `400`请求本身有问题（参数、业务规则）/ `429`同一账户的并发请求正在处理，稍后重试 / `500`服务端内部错误
- `errCode`：**稳定的机器可读错误码，合作方程序应该按它做分支**。`message`是给人看的中文描述，措辞可能调整，不要匹配它的文本
- 新增错误码只增不改，已经发布的`errCode`含义不会变

### 错误码

| errCode                 | code | 含义 / 建议处理                                                                           |
|-------------------------|------|-------------------------------------------------------------------------------------------|
| `invalid_param`         | 400  | 参数缺失、格式或取值不合法，`message`里有具体是哪个参数。修正请求，不要重试               |
| `symbol_not_found`      | 400  | 合约不存在或已下架。用 `GET /contract/list` 查可用合约                                    |
| `order_not_found`       | 400  | 委托/条件单不存在，或不属于这个`uid`                                                      |
| `order_not_cancelable`  | 400  | 委托已成交完/已撤销，或条件单已触发，不能再撤。不是故障，按最新状态处理即可               |
| `position_not_found`    | 400  | 这个`uid+symbol+side`没有持仓（改杠杆时）                                                 |
| `no_mark_price`         | 400  | 这个合约还没有标记价格（新合约没成交过，或生产模式下还没喂过指数价），市价单/校验杠杆无法进行 |
| `price_out_of_range`    | 400  | 限价单价格偏离参考价超过价格保护带（`priceProtectionRatio`），开仓单才会触发              |
| `price_tick_invalid`    | 400  | 价格不是最小变动单位（`priceTick`）的整数倍                                               |
| `volume_out_of_range`   | 400  | 数量低于`minVolume`、超过`maxVolume`或不是`volumeStep`的整数倍                            |
| `tier_not_configured`   | 400  | 这个合约没有配置保证金分档，暂不允许开仓                                                  |
| `leverage_exceeds_tier` | 400  | 杠杆超出当前仓位名义价值对应档位的上限，档位见 `GET /contract/detail`                     |
| `insufficient_margin`   | 400  | 可用余额+信用额度+持仓浮盈不够冻结这笔委托的保证金                                        |
| `insufficient_balance`  | 400  | `POST /account/balance`扣款时可用余额不足                                                 |
| `server_busy`           | 429  | 同一个`uid+symbol+side`有并发请求正在处理。短暂等待后重试（带`requestId`重试是安全的）|
| `account_not_found`     | 400  | 这个`uid`的账户还没创建，或者`uid`写错了。先调 `POST /account/create`；`uid`写错时这个错误码正好帮你拦住手误 |
| `idempotency_conflict`  | 400  | 同一个`requestId`已经用于一笔**参数不同**的请求。是调用方误用（同一个键复用到了另一笔请求），换一个新的`requestId` |
| `round_mismatch`        | 400  | `POST /account/round/close`指定的`round`大于账户当前轮数                                  |
| `account_frozen`        | 400  | 账户已被冻结，不能开仓、创建条件开仓单、修改杠杆（平仓、撤单、查询仍可用），见`POST /account/status` |
| `kline_source_not_external` | 400  | `POST /kline/sync`只在K线来源是外部行情（`PERP_KLINE_SOURCE=external`）时可用，当前不是，一根都没写 |
| `index_price_jump`      | 400  | `POST /index-price`的指数价相对当前值跳变超过服务端阈值，**没有写入**。新价位持续几秒后会被承认，继续按周期推即可，不是故障，见该接口说明 |
| `auth_missing`          | 401  | 缺鉴权请求头，或者 nonce/时间戳格式不对                                                   |
| `auth_expired`          | 401  | 时间戳不在前后 30 秒内。检查合作方服务器的时钟是否同步（NTP）                             |
| `auth_invalid_signature`| 401  | 签名不对，或者密钥不存在（两种情况故意返回完全相同的响应）                                |
| `auth_replayed`         | 401  | 这个 nonce 已经用过。每个请求都要用新的 nonce 和新的时间戳重新签名，包括重试              |
| `forbidden`             | 403  | 签名通过了，但这把密钥没有这个接口的权限范围（比如 `trade` 密钥调加钱接口）               |
| `dispatch_failed`       | 500  | 委托已落库，但发往撮合引擎失败。**用同一个`requestId`重试即可补发**，不会重复下单     |
| `internal_error`        | 500  | 服务端内部错误。可以稍后重试；写接口重试前建议先查一次状态或使用`requestId`           |

### 数据类型约定

| 类型       | 约定                                                                                                      |
|------------|-----------------------------------------------------------------------------------------------------------|
| 金额/价格/数量 | 响应里一律是**字符串**（`"58666.66666667"`），避免JSON数字的浮点精度问题                              |
| 时间       | 毫秒级 Unix 时间戳（整数），字段名以`Time`结尾                                                            |
| 委托/成交ID | **字符串**（`"226887380290764800"`）。这是雪花ID，约 2.2×10¹⁷，超过 JS 的安全整数范围（9×10¹⁵），当数字解析会丢精度 |
| `uid`      | 整数（合作方自己分配的，本系统只透传）                                                                    |
| 字段命名   | 全部小驼峰（`avgEntryPrice`）                                                                             |
| 空列表     | 返回`[]`，不会是`null`                                                                                    |
| 未知字段   | 合作方解析时应当**忽略**响应里不认识的字段，后续版本可能新增字段                                          |

### 枚举

| 字段        | 取值                                                                                                                           |
|-------------|--------------------------------------------------------------------------------------------------------------------------------|
| `side`      | `long` 多 / `short` 空                                                                                                         |
| `action`    | `open` 开仓 / `close` 平仓                                                                                                     |
| `type`      | `limit` 限价 / `market` 市价                                                                                                   |
| 委托`status`| `open` 已挂单未成交 / `partially_filled` 部分成交 / `filled` 全部成交 / `canceled` 已撤销（含部分成交后撤销）/ `rejected`（预留，目前不会出现） |
| 条件单`status` | `pending` 等待触发 / `triggered` 已触发（已转成一笔真正的委托，同一个`orderId`）/ `canceled` 触发前被撤销                  |
| 仓位`status`| `normal` / `liquidating` 强平进行中 / `closed`                                                                                 |
| `triggerDirection` | `gte` 标记价格涨到或超过触发价才触发 / `lte` 跌到或低于触发价才触发                                                     |
| 资金流水`type` | `deposit` 合作方充值/扣减 / `fee` 手续费 / `realized_pnl` 已实现盈亏 / `funding_fee` 资金费 / `credit_grant` 发放信用额度 / `round_close` 结束本轮回收信用额度 / `liquidation_clear` 强平清算 |
| K线`interval` | `1m` `5m` `15m` `1h` `4h` `1d`                                                                                              |

### 分页

历史类列表接口（`/order/history`、`/order/conditional/history`、`/trade/history`、
`/funding/history`、`/account/transactions`、`/liquidation/history`、`/market/trades`）统一：

- `limit`：每页条数，默认100，最大500，超过会被拒绝（不会悄悄截断）
- `before`：游标，省略=从最新开始；传上一页**最后一条**的 id（`orderId`/`tradeId`/资金流水`id`，`/funding/history`是`fundingTime`），只返回比它更早的记录

结果按 id 倒序（新的在前）。用游标而不是页码，是因为翻页期间有新记录插入时页码会错位、漏行或重复。
拿到的记录少于`limit`就说明到底了。

### 幂等：`requestId`

网络超时后合作方分不清"到底成功没有"，重试就可能重复下单、重复入账。所以**会改变资金或状态的写接口**
都用同一个幂等键 `requestId`（字母数字和`_-`，1~64位）：同一个`uid`重复提交同一个`requestId`，
只会生效一次，后面的返回第一次的结果并带 `"duplicate": true`。建议直接用 UUID。

| 接口                                      | 幂等键        | 必填吗 | 说明                                                             |
|-------------------------------------------|---------------|--------|------------------------------------------------------------------|
| `POST /account/balance`（充值/扣款）      | `requestId`   | **必填** | 不带会重复入账/扣款                                            |
| `POST /account/credit`（发放信用额度）    | `requestId`   | **必填** | 发额度是累加，不带会让额度翻倍                                 |
| `POST /account/round/close`（结束本轮）   | `round`       | **必填** | 要结束的那一轮，天然幂等键，见该接口说明                       |
| `POST /order/add`、`/order/conditional/add` | `requestId` | 可选，强烈建议 | 不带也能下单，但超时后无法安全重试                     |
| 撤单、批量撤单、改杠杆、设投保、喂指数价  | 不需要        | —      | "设为目标值"或天然幂等，重复调用没有副作用                       |

行为约定：

- **参数一致**：返回第一次的结果，带`"duplicate": true`，不会重复扣款/入账/冻结保证金
- **参数不一致**：返回 `idempotency_conflict`。服务端会存每次请求的参数摘要，同一个`requestId`带了不同金额、
  价格、方向等就报错，而不是悄悄返回第一次的结果——否则你会误以为第二笔也成功了
- **并发提交同一个`requestId`**是安全的（数据库唯一索引串行化），只有一个会真正生效
- **失败不占用`requestId`**：比如扣款余额不足，这个`requestId`没有被消耗，补足余额后可以用同一个`requestId`重试
- **`requestId`的作用域是"每类资源、每个`uid`"**：订单、条件单、资金流水各自独立。资金类的两个接口共用一个空间，
  所以用同一个`requestId`先充值再发额度会被判为冲突——用UUID就不用操心这个
- 也可以用 `GET /order/detail?requestId=` 反查订单，`GET /account/transactions`的每条记录会带上它的`requestId`，方便对账

注意：条件单触发后落地的那笔委托**不继承**`requestId`（条件单自己的幂等键只在条件单表里）。

如果下单返回 `dispatch_failed`（委托已落库但发往撮合引擎失败），带同一个`requestId`重试就会把它补发出去，不会重复下单。

设计细节、为什么这样设计、业界做法对比见 [idempotency.md](idempotency.md)。

### 异步语义（重要）

| 接口                                 | 返回时表示                                       | 怎么知道最终结果                                                        |
|--------------------------------------|--------------------------------------------------|-------------------------------------------------------------------------|
| `POST /order/add`                    | 委托已校验、冻结保证金、落库，**已发往撮合引擎** | 撮合是异步的。查 `GET /order/detail`，或订阅 WS `user:{uid}` 频道       |
| `POST /order/cancel/:orderId`        | 撤单**请求已提交**                               | 查委托状态变成`canceled`，或订阅 WS                                     |
| `POST /order/cancel-all`             | 撤单请求已提交（返回提交了多少笔）               | 同上；条件单的撤销是同步生效的                                          |
| `POST /account/round/close`          | 结束本轮**请求已提交**（返回`status: submitted`）| 轮询 `GET /account/info`，`round`加1就说明完成了；订阅 WS 也能收到      |
| `POST /account/status`（冻结时）     | 状态已经改成功（同步），存量开仓委托的撤单**请求已提交** | 同`cancel-all`：委托状态变成`canceled`；条件开仓单的撤销是同步生效的 |
| 其它写接口（改杠杆、发额度、加款等） | 已经生效（同步）                                 | —                                                                       |

余额不足、参数错误这类校验失败是**同步**返回的，不用等撮合。

## 典型对接流程

```
0. 创建账户          POST /account/create       {uid}                  （用户注册时调用，重复调用安全）
1. 入金              POST /account/balance      {uid, amount, requestId}
2. （运营）喂指数价  POST /index-price          {symbol, price}        （资金费率结算依赖，见 funding-rate.md）
3. 查合约规则        GET  /contract/detail      ?symbol=BTCUSDT        （精度/最大杠杆/手续费，客户端下单表单用）
4. 订阅私有推送      WS   /ws                   subscribe user:{uid}   （挂单/成交/强平后自动收到账户快照）
5. 下单              POST /order/add            带 requestId
6. 跟踪委托          GET  /order/detail         或等 WS 推送
7. 查持仓/权益       GET  /position/current  /  GET /account/info
8. 平仓/止盈止损      POST /order/add (action=close)  /  POST /order/conditional/add
9. 本轮结束          POST /account/round/close  {uid, round}           （撤单+强平+信用额度清零+round加1）
10. 对账              GET /account/transactions  ?uid=...               （资金流水，分页）
```

用户如果买了保险：`POST /account/insured`置投保状态、`POST /account/credit`发放信用额度
（信用额度只能当保证金开仓、不能提现），详见 [account-and-margin.md](account-and-margin.md)。

---

## 账户

### `POST /account/create`

创建账户。`uid`是合作方自己体系里的用户ID，直接沿用。**在合作方的用户注册流程里调用**，之后才能充值、下单、查询。

```json
{ "uid": 10001 }
```

```json
{
  "code": 200, "message": "success",
  "data": {
    "uid": 10001, "isInsured": false, "status": "active", "round": 0,
    "credit": "0", "available": "0", "frozenMargin": "0", "frozenCredit": "0",
    "positionMargin": "0", "totalUnrealizedPnl": "0", "equity": "0",
    "created": true
  }
}
```

- **天然幂等**：账户已经存在就原样返回已有账户（`created: false`），不改任何字段，所以超时重试没有风险，不需要`requestId`
- 并发创建同一个`uid`也是安全的，只有一个请求得到`created: true`
- 为什么必须先创建：如果其它接口遇到新`uid`就自动建账户，`uid`手误写错的充值会成功地充给一个没人认领的账户，
  而且只读接口（比如`GET /account/info`）也会往库里塞垃圾账户。要求先创建，写错的`uid`会被`account_not_found`拦住

### `POST /account/balance`

合作方/运营调整账户可用余额（不是用户提现接口）。`amount`正数=加钱，负数=扣钱，不能为0。

```json
{ "uid": 10001, "amount": 1000, "requestId": "dep-20260919-0001" }
```

**`requestId`必填**，见上面"幂等"：同一个`requestId`重复提交只入账/扣款一次。扣款时`available`必须够，
否则返回`insufficient_balance`、什么都不改，而且这个`requestId`不会被占用，补足余额后可以重试。

```json
{ "code": 200, "message": "success", "data": { "requestId": "dep-20260919-0001" } }
```

重放时`data`里多一个`"duplicate": true`。同一个`requestId`带了不同金额返回`idempotency_conflict`。

### `GET /account/info?uid=10001`

```json
{
  "code": 200, "message": "success",
  "data": {
    "uid": 990102, "isInsured": false, "status": "active", "round": 0,
    "credit": "0", "available": "1115.6",
    "frozenMargin": "0", "frozenCredit": "0",
    "positionMargin": "500",
    "totalUnrealizedPnl": "100.0000000005", "equity": "1715.6000000005"
  }
}
```

| 字段                 | 说明                                                                                     |
|----------------------|------------------------------------------------------------------------------------------|
| `round`              | 当前轮数，`POST /account/round/close`成功后加1                                           |
| `isInsured`          | 本轮是否投保                                                                             |
| `credit`             | 信用额度余额（保险赔付，只能当保证金，不能转出提现）                                     |
| `available`          | 可用余额。全仓模式下可能为负（持仓浮盈被当作买力借用时）                                 |
| `frozenMargin` / `frozenCredit` | 挂单占用的冻结保证金，分别来自`available`/`credit`                            |
| `positionMargin`     | 全部持仓占用的保证金之和（含来自信用额度的部分）。开仓成交时这笔钱从`available`转进仓位   |
| `totalUnrealizedPnl` | 全部持仓的未实现盈亏之和                                                                 |
| `equity`             | 账户权益 = `available + credit + frozenMargin + frozenCredit + positionMargin + totalUnrealizedPnl`，即全部属于用户的钱加浮动盈亏，强平判断用的就是这个口径。开仓只是把钱从`available`挪进保证金，价格不动权益不变（只被手续费拉低） |

### `POST /account/credit`

发放/追加信用额度（用户买保险后的赔付）。同一轮内可多次调用，直接累加。`amount`必须大于0。

```json
{ "uid": 10001, "amount": 1000, "requestId": "credit-20260919-0001" }
```

**`requestId`必填**：发额度是累加操作，重试不带幂等键会让信用额度翻倍。响应和重放规则同
`POST /account/balance`。

### `POST /account/insured`

单独设置本轮是否投保，跟发放额度互不联动。

```json
{ "uid": 10001, "insured": true }
```

### `POST /account/status`（运营接口）

冻结/解冻账户，`ops`权限。冻结是"禁止新增风险，不禁止降低风险"（跟挂单"冻结保证金"是两回事）：

| 冻结后                                                                 | 行为                                       |
|------------------------------------------------------------------------|--------------------------------------------|
| 开仓委托、创建条件开仓单、`POST /position/leverage`                    | 拒绝，返回`account_frozen`                 |
| 平仓委托、撤单、批量撤单、结束本轮、全部查询、WebSocket                | 照常可用                                   |
| 运营的加钱扣钱、发信用额度、设投保                                     | 照常可用                                   |
| 强平、ADL、资金费、成交结算等系统自己的动作                            | 照常执行（冻结不能让账户躲过强平）         |

```json
{ "uid": 10001, "status": "frozen", "reason": "风控：异常交易" }
```

`status`取值`active` / `frozen`，`reason`选填，最长255字节，会和操作的密钥id、时间一起记入变更历史表
`account_status_history`。

```json
{ "code": 200, "message": "success", "data": {
    "uid": 10001, "status": "frozen", "changed": true,
    "cancelRequested": 1, "cancelRequestFailed": 0, "conditionalCanceled": 1, "conditionalFailed": 0 } }
```

- **天然幂等**：设为目标值，已经是这个状态就什么都不改（`changed: false`），不需要`requestId`
- **冻结时顺带清理存量的开仓类挂单**：普通开仓委托给每笔发一条撤单请求（异步，含义同`POST /order/cancel-all`的
  `cancelRequested`）、条件开仓单直接撤销（同步）；平仓委托、平仓类止盈止损条件单和强平委托保留。撤单会退回挂单占用的保证金
- **清理每次冻结请求都会执行**，不管这次有没有改变状态：`cancelRequestFailed`或`conditionalFailed`非0时，带同样的参数
  重试即可补完。清理本身出错时返回500，但状态已经改成功了，重试即可
- 冻结前已经落库、还在排队等撮合的开仓委托，以及冻结后刚好被触发的条件开仓单，撮合前会被引擎再检查一次，已冻结就直接撤销
  退款，不会成交
- 冻结前已经成功的下单，带同一个`requestId`重试仍然返回原结果（`duplicate: true`），不会因为账户被冻结变成`account_frozen`
- `GET /account/info`和WebSocket私有频道的账户快照里有`status`字段

### `POST /account/round/close`

通知本轮结束：撤销该`uid`全部挂单和未触发条件单、按当前标记价强平全部仓位、`credit`清零（没用完的
赔付额度不追讨）、`isInsured`重置、`round`加1，细节见 [account-and-margin.md](account-and-margin.md#轮次round生命周期)。

```json
{ "uid": 10001, "round": 3 }
```

**`round`必填**：要结束的那一轮，取值是 `GET /account/info` 返回的`round`（第一轮是`0`）。它同时是这个接口的
幂等键：结束第3轮只会生效一次。没有它的话，超时重试会在账户已经进入第4轮之后又结束一次，把第4轮刚挂的单撤掉、
刚开的仓强平、刚发的信用额度清零。

```json
{ "code": 200, "message": "success", "data": { "round": 3, "status": "submitted" } }
```

| `status`          | 含义                                                                                   |
|-------------------|----------------------------------------------------------------------------------------|
| `submitted`       | 请求已提交，**只表示已受理**，见上面"异步语义"                                         |
| `already_closed`  | `round`比账户当前轮数小，说明这一轮之前已经结束过了，什么都没做——重试的正常结果        |

`round`比账户当前轮数大返回`round_mismatch`。如果某个合约当时缺标记价格，那个仓位这一轮会强平失败并跳过，
`round`没有推进，用**同一个`round`**再调用一次即可重试。

### `GET /account/transactions?uid=10001&type=fee&limit=100&before=331`

资金流水，合作方对账用。`type`可选，取值见"枚举"；支持分页。

```json
{
  "code": 200, "message": "success",
  "data": [
    { "id": "335", "uid": 990101, "symbol": "USDT", "amount": "1000", "type": "deposit", "createTime": 1789783870000, "requestId": "dep-20260919-0001" },
    { "id": "331", "uid": 990101, "symbol": "BTCUSDT", "amount": "-1.45", "type": "fee", "createTime": 1789783875255 },
    { "id": "330", "uid": 990101, "symbol": "BTCUSDT", "amount": "-33.3333333335", "type": "realized_pnl", "createTime": 1789783875255 }
  ]
}
```

`amount`正数=入账、负数=出账；`symbol`对跟具体合约无关的流水（充值、发放额度）不是合约名（比如`USDT`）。合作方发起的充值/扣款/发额度会带上当时传的`requestId`，方便跟自己的请求一一对账；系统内部产生的流水（手续费、盈亏等）没有这个字段。

---

## 委托

### `POST /order/add`

```json
{
  "uid": 10001, "symbol": "BTCUSDT",
  "side": "long", "action": "open", "type": "limit",
  "price": 65000, "amount": 0.1, "leverage": 10,
  "reduceOnly": false, "requestId": "order-20260919-0001"
}
```

| 字段            | 必填        | 说明                                                                                    |
|-----------------|-------------|-----------------------------------------------------------------------------------------|
| `uid`           | 是          |                                                                                         |
| `symbol`        | 是          | 合约，如`BTCUSDT`                                                                       |
| `side`          | 是          | `long` / `short`                                                                        |
| `action`        | 是          | `open` / `close`                                                                        |
| `type`          | 否          | `limit`（默认）/ `market`                                                               |
| `price`         | 限价单必填  | 市价单忽略，按标记价格估算                                                              |
| `amount`        | 二选一      | 标的币数量                                                                              |
| `marginAmount`  | 二选一      | 用保证金金额反推数量：`amount = marginAmount × leverage / price`。两个都传优先用它      |
| `leverage`      | 否          | 默认1，**必须是整数**；显式传0或小数会报错。开仓时不能超过当前名义价值对应档位的上限    |
| `reduceOnly`    | 否          | 默认false，只减仓                                                                       |
| `requestId` | 否          | 幂等键，见上面"幂等"，强烈建议传                                                        |

成功响应：

```json
{ "code": 200, "message": "success", "data": { "orderId": "226887380290764800", "requestId": "order-20260919-0001" } }
```

重复提交同一个`requestId`时`data`里多一个`"duplicate": true`，`orderId`是第一次那笔。
没传`requestId`时响应里没有这个字段。

**注意**：`price`对会立刻成交的"吃单"来说跟真实成交价可能不一致——冻结保证金按保守参考价估算、成交后按真实成交价
多退少补，账户最终不会吃亏，见 [matching-and-settlement.md](matching-and-settlement.md#冻结保证金的保守估计)。

### `POST /order/cancel/:orderId`

```json
{ "uid": 10001 }
```

`orderId`是 URL 路径参数。响应`data`是`"撤单请求已提交"`（异步）。已成交完/已撤销的委托返回`order_not_cancelable`。

### `POST /order/cancel-all`

批量撤销这个`uid`的全部挂单。

```json
{ "uid": 10001, "symbol": "BTCUSDT", "includeConditional": false }
```

| 字段                 | 说明                                                                                                   |
|----------------------|--------------------------------------------------------------------------------------------------------|
| `symbol`             | 可选，省略=全部合约                                                                                    |
| `includeConditional` | 默认`false`。条件单（止盈止损）通常是用户想一直留着保护仓位的，"撤销全部委托"默认不动它们；传`true`才一起撤 |

```json
{ "code": 200, "message": "success",
  "data": { "cancelRequested": 2, "cancelRequestFailed": 0, "conditionalCanceled": 1, "conditionalFailed": 0 } }
```

`cancelRequested`是**已提交撤单请求**的笔数（异步，不代表都已撤成功）；`conditionalCanceled`是同步已撤销的条件单。
`*Failed`大于0时重试本接口即可（重复提交撤单请求是安全的）。

### `GET /order/current?uid=10001&symbol=BTCUSDT`

当前挂单（`open`/`partially_filled`），`symbol`可省略查全部。

```json
{
  "code": 200, "message": "success",
  "data": [
    {
      "orderId": "226888243595968512", "uid": 990102, "symbol": "BTCUSDT",
      "side": "short", "action": "close", "type": "limit",
      "price": "90000", "amount": "0.01", "tradedAmount": "0", "avgDealPrice": "0",
      "frozenMargin": "0", "frozenCredit": "0", "leverage": 1,
      "reduceOnly": true, "liquidation": false, "status": "open",
      "createTime": 1789783972653, "updateTime": 1789783972653
    }
  ]
}
```

| 字段                          | 说明                                                                     |
|-------------------------------|--------------------------------------------------------------------------|
| `tradedAmount` / `avgDealPrice` | 已成交数量 / 加权平均成交价                                            |
| `frozenMargin` / `frozenCredit` | 这笔委托占用的冻结保证金（来自可用余额/信用额度），平仓单为0           |
| `liquidation`                 | `true`表示这是系统发起的强平委托，不是用户下的                           |
| `requestId`               | 只在下单时传了才有这个字段                                               |

### `GET /order/history?uid=10001&limit=100&before=`

历史委托（含已成交/已撤销/挂单中），结构同上，支持分页。

### `GET /order/detail?uid=10001&orderId=226887380290764800`

按`orderId`或`requestId`（二选一，都传优先`orderId`）查单笔委托，结构同上。不存在或不属于该`uid`返回`order_not_found`。

```
GET /order/detail?uid=10001&requestId=order-20260919-0001
```

### `GET /liquidation/history?uid=10001&limit=100&before=`

这个`uid`的强平委托记录（`liquidation=true`的委托），结构同`/order/history`。每笔强平委托的成交价、成交量、
状态都在里面。分批强平的大仓位会有多笔。

---

## 条件单（止盈止损/条件开仓）

详细设计见 [conditional-orders.md](conditional-orders.md)。

### `POST /order/conditional/add`

```json
{
  "uid": 10001, "symbol": "BTCUSDT",
  "side": "long", "action": "close",
  "triggerPrice": 60000, "triggerDirection": "gte",
  "type": "market", "amount": 0.1, "leverage": 10,
  "reduceOnly": true, "requestId": "tp-0001"
}
```

字段跟`POST /order/add`基本一致，额外两个必填字段：

| 字段               | 说明                                                             |
|--------------------|------------------------------------------------------------------|
| `triggerPrice`     | 触发价                                                           |
| `triggerDirection` | `gte`=标记价格涨到/超过触发价才触发，`lte`=跌到/低于触发价才触发 |

`type=limit`时`price`是触发后要执行的委托价格（必填）；`type=market`时不传`price`，触发后按当时的标记价成交。
`action=open`会在**创建时**就冻结保证金（分档/杠杆校验同下单接口），`action=close`不冻结。

响应同`POST /order/add`（`orderId`是字符串，可带`requestId`/`duplicate`）。这个`orderId`和触发后落地到
委托表的`orderId`是同一个，触发后用`GET /order/detail`能查到那笔真实委托。

### `POST /order/conditional/cancel/:orderId`

```json
{ "uid": 10001 }
```

只能撤销还没触发（`pending`）的条件单；已触发的要用`POST /order/cancel/:orderId`撤（这时它已经是真正的委托了）。
同步生效，冻结的保证金立即退回。

### `GET /order/conditional/current?uid=10001&symbol=BTCUSDT`

当前还没触发的条件单，`symbol`可省略。

```json
{
  "code": 200, "message": "success",
  "data": [
    {
      "orderId": "226888260717117440", "uid": 990102, "symbol": "BTCUSDT",
      "side": "short", "action": "close",
      "triggerPrice": "50000", "triggerDirection": "lte",
      "type": "market", "price": "0", "amount": "0.05", "leverage": 1, "reduceOnly": true,
      "frozenMargin": "0", "frozenCredit": "0", "status": "pending",
      "createTime": 1789783976735, "updateTime": 1789783976735
    }
  ]
}
```

### `GET /order/conditional/history?uid=10001&limit=100&before=`

条件单历史（含已触发/已撤销），支持分页。

### `GET /order/conditional/detail?uid=10001&orderId=` 或 `&requestId=`

查单笔条件单。

---

## 持仓

### `GET /position/current?uid=10001`

返回持仓原始字段 + 现算的标记价/未实现盈亏/回报率/名义价值/预估强平价。

```json
{
  "code": 200, "message": "success",
  "data": [
    {
      "id": 90, "uid": 990102, "symbol": "BTCUSDT", "side": "short",
      "volume": "0.15", "avgEntryPrice": "58666.66666667",
      "positionMargin": "880", "creditMargin": "0", "leverage": 10,
      "status": "normal", "updateTime": 1789783855187,
      "markPrice": "58000", "unrealizedPnl": "100.0000000005", "roe": "0.1136363636369318",
      "notionalValue": "8700", "liquidationPrice": "64276.2284196580345286"
    }
  ]
}
```

| 字段               | 说明                                                                                                |
|--------------------|-----------------------------------------------------------------------------------------------------|
| `positionMargin`   | 这个仓位占用的保证金（记账值）；`creditMargin`是其中来自信用额度的部分                              |
| `roe`              | 回报率 = 未实现盈亏 / 占用保证金                                                                    |
| `liquidationPrice` | **仅供展示的估算值**（按单仓公式），全仓下真实强平以整个账户权益 vs 全部仓位维持保证金为准，见 [liquidation.md](liquidation.md) |
| `status`           | `liquidating`表示正在被强平                                                                         |

没有持仓返回`[]`。目前**没有已平仓仓位的历史记录**（仓位平掉后同一行会被复用），需要对账请用 `/trade/history` 和 `/account/transactions`。

### `POST /position/leverage`

修改一个**已有仓位**的杠杆，详细设计见 [leverage.md](leverage.md)。

```json
{ "uid": 10001, "symbol": "BTCUSDT", "side": "long", "leverage": 10 }
```

只对已经有仓位的`uid+symbol+side`生效，没有仓位返回`position_not_found`（杠杆本来是下单时的参数，
没有"没有仓位时预先声明"的场景）。修改成功会按新杠杆重算占用保证金、多退少补，不产生成交/资金费流水。
杠杆调低（需要更多保证金）时余额不够返回`insufficient_margin`。

---

## 成交

### `GET /trade/history?uid=10001&limit=100&before=`

这个`uid`参与的历史成交（不论买方卖方），支持分页。

```json
{
  "code": 200, "message": "success",
  "data": [
    {
      "tradeId": "226887750907858944", "symbol": "BTCUSDT",
      "price": "58000", "volume": "0.05",
      "buyOrderId": "226887479800627200", "sellOrderId": "226887746545778688",
      "buyUid": 990101, "sellUid": 990102,
      "makerOrderId": "226887479800627200", "createTime": 1789783855187
    }
  ]
}
```

`makerOrderId`等于`buyOrderId`说明买方是挂单方（maker）、卖方是吃单方（taker），反之亦然。

---

## 合约与行情（公开数据，不需要`uid`）

### `GET /contract/list`

全部可交易的合约及交易规则。**每个合约都带价格/数量的精度信息**，前端展示和下单输入靠它，不用写死：

| 字段            | 用途                                                                                                  |
|-----------------|-------------------------------------------------------------------------------------------------------|
| `priceScale`    | **价格显示几位小数**。固定显示这么多位，末尾的0也显示（BTCUSDT是1位：`81255.0`，不是`81255`）          |
| `baseCoinScale` | **数量显示几位小数**，同样固定位数（BTCUSDT是3位：`0.040`；ETHUSDT是2位：`4.20`）                      |
| `priceTick`     | 价格最小变动单位，下单输入框的步进。**`0`表示不校验**，此时输入精度按`priceScale`                       |
| `volumeStep`    | 数量步长。**`0`表示不校验**，此时输入精度按`baseCoinScale`                                             |
| `minVolume` / `maxVolume` | 单笔最小/最大下单量，`maxVolume`为`0`表示不限                                                |

价格、数量在接口里都是字符串；返回值末尾可能没有0（`"81255"`），要按`priceScale`/`baseCoinScale`补齐后再显示。

这些配置存在数据库的`coins`表里，每个合约各自配置，改了下一次请求就生效。种子数据里`priceTick`/`volumeStep`配成跟位数一致（BTCUSDT价格步进0.1、
数量步长0.001；ETHUSDT价格步进0.01、数量步长0.01）。下单（`POST /order/add`）和创建条件单按它们校验：限价单价格和条件单触发价必须是`priceTick`的整数倍
（`price_tick_invalid`），数量必须是`volumeStep`的整数倍、不低于`minVolume`、不超过`maxVolume`（`volume_out_of_range`），错误提示里带着具体的步长。
市价单不校验价格。已经建过库的环境要手动更新一次：
`UPDATE coins SET price_tick=0.1, volume_step=0.001 WHERE symbol='BTCUSDT'; UPDATE coins SET price_tick=0.01, volume_step=0.01 WHERE symbol='ETHUSDT';`

### `GET /contract/detail?symbol=BTCUSDT`

单个合约的完整规则，加保证金分档。客户端做下单表单校验、展示最大杠杆都靠这个，不需要写死。

```json
{
  "code": 200, "message": "success",
  "data": {
    "symbol": "BTCUSDT", "baseCoinScale": 3, "priceScale": 1, "enable": true,
    "makerFee": "0.0002", "takerFee": "0.0005",
    "priceTick": "0.1", "volumeStep": "0.001", "minVolume": "0.001", "maxVolume": "0",
    "fundingIntervalHours": 8, "fundingRateCap": "0.0075", "fundingImpactNotional": "10000", "priceProtectionRatio": "0.05",
    "tiers": [
      { "symbol": "BTCUSDT", "tier": 1, "maxNotional": "50000", "maintenanceMarginRate": "0.004", "maintenanceAmount": "0", "maxLeverage": 125 },
      { "symbol": "BTCUSDT", "tier": 2, "maxNotional": "250000", "maintenanceMarginRate": "0.005", "maintenanceAmount": "50", "maxLeverage": 100 }
    ]
  }
}
```

| 字段                          | 说明                                                                                          |
|-------------------------------|-----------------------------------------------------------------------------------------------|
| `baseCoinScale` / `priceScale`| 数量 / 价格的小数位数，**前端展示按这个位数固定显示**（末尾的0也显示）                        |
| `priceTick` / `volumeStep`    | 最小变动价位 / 数量步长（下单输入的步进）。**`0`表示不限制**（本系统里这类配置统一 0=不限），此时输入精度按上一行的位数 |
| `minVolume` / `maxVolume`     | 单笔最小/最大下单量，`maxVolume`为`0`表示不限                                                 |
| `makerFee` / `takerFee`       | 手续费率                                                                                      |
| `fundingIntervalHours`        | 资金费率结算周期（小时）                                                                      |
| `fundingRateCap`              | 资金费率上下限，`0`表示不限                                                                   |
| `fundingImpactNotional`       | 算资金费率溢价用的冲击名义金额（USDT），见 [funding-rate.md](funding-rate.md)；**`0`表示这个合约不采样（资金费率恒为0），不是不限制** |
| `priceProtectionRatio`        | 价格保护带：开仓限价单价格偏离参考价超过这个比例会被拒绝                                      |
| `tiers[].maxNotional`         | 本档名义价值上限，`0`表示不限（最后一档）。仓位越大档位越高、允许的杠杆越低                   |
| `tiers[].maxLeverage`         | 本档最大杠杆                                                                                  |

### `GET /market/ticker?symbol=BTCUSDT`

```json
{
  "code": 200, "message": "success",
  "data": {
    "symbol": "BTCUSDT", "lastPrice": "58000", "markPrice": "58000", "indexPrice": "60000",
    "open24h": "56500", "high24h": "59000", "low24h": "56000", "volume24h": "0.6", "change24h": "0.0265486725663717"
  }
}
```

- 没有对应数据的字段是`null`（比如合约从没成交过），**不是0**——0是合法价格，区分不了
- `lastPrice`：K线来自币安（`PERP_KLINE_SOURCE=external`，生产）时是最近一根1分钟K线的收盘价，即币安的最新价；
  否则是最新一笔成交价。`markPrice`是标记价，由指数价、盘口基差、最新成交价取中位数得出，**不等于**最新成交价，
  见 [mark-price.md](mark-price.md)、[kline.md](kline.md)
- `indexPrice`是外部行情源喂进来的指数价格（`POST /index-price`）
- **24h统计口径**：最近24根1小时K线聚合（含当前还没走完的这一根），实际时间窗口在23~24小时之间，不是严格滚动的24小时
- `change24h`是小数比例（`0.0265`=+2.65%）

### `GET /market/trades?symbol=BTCUSDT&limit=100&before=`

公开最新成交，支持分页。**不含买卖双方的uid和委托ID**（不能泄露其它用户的身份）：

```json
{ "code": 200, "message": "success",
  "data": [ { "tradeId": "226887750907858944", "symbol": "BTCUSDT", "price": "58000", "volume": "0.05", "takerSide": "sell", "createTime": 1789783855187 } ] }
```

`takerSide`是吃单方向（`buy`=主动买入），行情展示常用来给成交着色。WS的`trade:{symbol}`频道推送的是同一个结构。

### `GET /kline?symbol=BTCUSDT&interval=1m&limit=200`

详细设计见 [kline.md](kline.md)。`interval`见"枚举"，`limit`默认200。按开盘时间**升序**（从旧到新）。
生产环境的K线**来自币安**（成交量和成交笔数是币安全市场的，不是我们平台的），我们自己的成交不写K线：

```json
{ "code": 200, "message": "success",
  "data": [ { "symbol": "BTCUSDT", "interval": "1h", "openTime": 1789783200000,
              "open": "59000", "high": "59000", "low": "58000", "close": "58000",
              "volume": "0.15", "tradeCount": 2, "updateTime": 1789783855187 } ] }
```

### `GET /funding/rate?symbol=BTCUSDT`

```json
{ "code": 200, "message": "success",
  "data": { "symbol": "BTCUSDT", "estimatedRate": "-0.0075", "nextFundingTime": 1789804800000 } }
```

当前预估费率 + 下次结算时间。机制见 [funding-rate.md](funding-rate.md)。

### `GET /funding/history?symbol=BTCUSDT&limit=100&before=`

历史结算记录，`fundingTime`倒序；`before`传上一页最后一条的`fundingTime`。

```json
{ "code": 200, "message": "success",
  "data": [ { "id": 9, "symbol": "BTCUSDT", "fundingTime": 1789776000000, "rate": "-0.0075",
              "markPrice": "20000", "indexPrice": "60000", "createTime": 1789776041204 } ] }
```

### `POST /kline/sync`（运营接口）

外部行情源（orderbook-sync）推送某个合约某个周期的一批K线，整根覆盖已有的同一根，写入后把变了的K线推给WebSocket订阅者。
**只在`PERP_KLINE_SOURCE=external`时可用**，否则返回`kline_source_not_external`。一次1到200根，整批校验，任何一根不合法整批拒绝。
详见 [kline.md](kline.md)。

```json
{ "symbol": "BTCUSDT", "interval": "1m",
  "candles": [ { "openTime": 1789783200000, "open": "59000", "high": "59100", "low": "58900", "close": "59050", "volume": "12.5", "tradeCount": 320 } ] }
```

响应`{"written": 1, "changed": 1}`：写入的根数、其中真的变了（新建或值不同）的根数。

### `POST /index-price`（运营接口）

外部行情源推送指数价格。指数价是标记价的锚，标记价再决定强平、盈亏、条件单触发和资金费率，见
[mark-price.md](mark-price.md)。**这是运营/行情源调用的接口，不是给终端用户的**，正式对接时应该单独授权。

**要持续推**：服务端记录每次推送的时间，超过30秒（`PERP_MARK_MAX_INDEX_AGE_SEC`）没更新就算断供，标记价冻结、
强平/资金费率/条件单暂停。建议每秒推1到2次。

```json
{ "symbol": "BTCUSDT", "price": 64800.5 }
```

**服务端跳变保护**（配了`PERP_INDEX_MAX_JUMP`才生效，生产建议`0.05`，默认不校验）：一次推送相对当前指数价变动超过
阈值，这次**不写入**，返回`code=400`、`errCode=index_price_jump`，`message`里有当前指数价和这个新价位已经持续了几秒。
同一个新价位（容差1%）持续满`PERP_INDEX_JUMP_CONFIRM_SEC`（默认3秒）之后再推就会被承认；期间只要有一次落在正常范围内
的推送，待确认状态就清掉重来。所以：

- 真实的行情大幅变动只是被延迟几秒，行情源继续按周期推同一个价位就行，**不需要特殊处理**
- 当前指数价已经断供（超过30秒没更新）时不校验，直接写，避免喂价断了以后再也恢复不了
- 它限制的是变动速度，挡不住拿着密钥每次只挪一小步的人，所以密钥仍然要单独发、只给`ops`

### `GET /health`

存活探针，返回`{"status":"ok","time":<毫秒时间戳>}`。

---

## 订单簿深度（`contract-engine`进程，不是`contract-api`）

订单簿只存在于`contract-engine`进程的内存里，这个接口在`contract-engine`自己的端口（默认`:7002`，
`PERP_ENGINE_HTTP_ADDR`可配），理由见 [order-book.md](order-book.md#为什么深度接口开在contract-engine而不是contract-api)。
分片部署下只有负责这个合约的实例能回答，见 [engine-sharding.md](engine-sharding.md)。

### `GET /health`（`contract-engine`）

存活探针，返回`{"status":"ok"}`。订单簿恢复在HTTP服务启动之前就跑完了（恢复失败进程会直接退出），所以探针
通过就代表订单簿是完整的。

### `GET /depth?symbol=BTCUSDT&levels=20`

`levels`默认20档。按价格聚合，不含单笔委托的uid/orderId：

```json
{ "code": 200, "message": "success",
  "data": {
    "bids": [ { "price": "57500", "volume": "0.05", "count": 1 } ],
    "asks": []
  } }
```

`bids`价格从高到低，`asks`从低到高；`count`是这一档的挂单笔数。

---

## WebSocket实时推送

详细设计（channel命名、订阅协议）见 [websocket.md](websocket.md)。

### `GET /ws`（`contract-api`，默认`:7001`）

升级成WebSocket连接后发JSON控制消息订阅/取消订阅：

```json
{ "op": "subscribe", "channels": ["depth:BTCUSDT", "trade:BTCUSDT", "kline:BTCUSDT:1m", "markprice:BTCUSDT", "user:10001"] }
```

推送消息统一格式：`{"channel": "trade:BTCUSDT", "data": {...}}`。

| 频道                      | 内容                                                                                       |
|---------------------------|--------------------------------------------------------------------------------------------|
| `depth:{symbol}`          | 深度快照，结构同 `GET /depth`                                                              |
| `trade:{symbol}`          | 公开成交，结构同 `/market/trades` 的单条（不含uid）                                        |
| `kline:{symbol}:{interval}` | K线，结构同 `GET /kline` 的单条                                                          |
| `markprice:{symbol}`      | `{"symbol": "BTCUSDT", "price": "58000"}`                                                  |
| `user:{uid}`              | **私有**。账户快照：`{account, positions, activeOrders}`，结构分别同`/account/info`、`/position/current`、`/order/current`。这个`uid`的挂单/成交/强平/结束本轮之后自动推送，是**完整快照不是增量** |

WS推送不保证绝对不丢（慢客户端的发送队列满了会丢弃新消息），客户端应该定期用REST接口校准状态。
`GET /ws` 握手时要带同样的签名请求头（`method=GET`，需要 `trade` 权限），握手失败响应不是 101，响应体里有
`errCode`；连接建立后订阅频道不再需要额外的签名。私有频道 `user:{uid}` 目前不校验这个 uid 是否属于这把密钥
（单个合作方不需要，多合作方时要做，见 [auth-design.md](auth-design.md)"还没做"）。
