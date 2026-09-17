# HTTP接口

`contract-api`进程对外提供的接口。MVP阶段鉴权用明文`uid`参数占位（不做HMAC/token校验），
`uid`都是独立传参，方便后续直接换成鉴权中间件注入、不用改业务代码。

**约定**：GET查询接口走query string；POST写接口统一用JSON body
（`Content-Type: application/json`），不用表单编码。

统一响应格式：

```json
{
  "code": 200,
  "message": "success",
  "data": {
    ...
  }
}
```

失败时`code`是HTTP语义的错误码（400/500等，不是HTTP状态码本身——HTTP状态码固定
返回200，错误信息在body里的`code`字段），`message`是错误描述。

## 账户

### `POST /account/balance`

合作方/运营调整账户可用余额（不是用户提现接口，也不经过任何第三方支付/风控）。

```json
{
  "uid": 10001,
  "amount": 1000
}
```

`amount`正数=加钱，负数=扣钱（扣的时候必须有足够`available`）。

### `GET /account/info?uid=10001`

返回账户原始字段+现算的未实现盈亏/权益，见 [account-and-margin.md](account-and-margin.md)。

### `POST /account/credit`

合作方发放/追加信用额度（用户买保险后的赔付）。同一轮内可以多次调用、直接累加，不会
覆盖之前发放的额度。

```json
{
  "uid": 10001,
  "amount": 1000
}
```

`amount`必须大于0。

### `POST /account/insured`

单独设置这个账户本轮是否投保。跟`POST /account/credit`是两个独立接口，互不联动——
投保状态不会自动触发发放额度，发放额度也不会自动置投保状态。

```json
{
  "uid": 10001,
  "insured": true
}
```

### `POST /account/round/close`

合作方通知本轮结束：撤销该uid全部挂单、按当前标记价强平全部仓位、`credit`清零
（没用完的赔付额度不追讨）、`is_insured`重置、`round`+1，细节见
[account-and-margin.md](account-and-margin.md#轮次round生命周期)。

```json
{
  "uid": 10001
}
```

**注意**：这个接口只是把请求发到Kafka异步路由给`contract-engine`执行（跟撤单接口
同样的道理——撤销挂单要摸`contract-engine`内存里的订单簿，`contract-api`这边做不到），
HTTP响应`"结束本轮请求已提交"`只表示请求已受理，不代表撤单/强平/清算已经全部执行完。
如果某个symbol当时缺标记价格，那个仓位这一轮会强平失败、跳过并记日志告警，需要等价格
恢复后重新调用一次本接口。

## 委托

### `POST /order/add`

```json
{
  "uid": 10001,
  "symbol": "BTCUSDT",
  "side": "long",
  "action": "open",
  "type": "limit",
  "price": 65000,
  "amount": 0.1,
  "leverage": 10,
  "reduceOnly": false
}
```

字段说明（枚举类字段全部是**小写**，大小写敏感，`"LONG"`这种大写值会被拒绝）：

| 字段           | 必填        | 说明                                                                   |
|----------------|-------------|------------------------------------------------------------------------|
| `side`         | 是          | `long` / `short`，其它值一律拒绝（不会被撮合引擎当成默认方向悄悄放行） |
| `action`       | 是          | `open` / `close`，其它值一律拒绝                                       |
| `type`         | 否          | `limit`（默认）/ `market`，其它值一律拒绝（不会被当成market处理）      |
| `price`        | LIMIT单必填 | MARKET单忽略此字段，按标记价格估算                                     |
| `amount`       | 二选一      | 标的币数量                                                             |
| `marginAmount` | 二选一      | 用保证金金额+杠杆反推数量：`amount = marginAmount * leverage / price`  |
| `leverage`     | 否          | 默认1；**必须是整数**，显式传0或小数都会报错，不会被当成"没传"或被截断 |
| `reduceOnly`   | 否          | 默认false                                                              |

`amount`和`marginAmount`必须传一个，两个都传优先用`marginAmount`。`leverage`/
`marginAmount`/`amount`这三个字段的JSON类型是可选指针——"没传这个字段"和"传了显式的0"
是两种不同的语义，不能混为一谈，见下方注意事项。

完整的下单校验链见 [matching-and-settlement.md](matching-and-settlement.md)。

**注意**：由于contract-api在下单时看不到contract-engine那边订单簿的真实状态，`price`
字段对于会立刻成交的"吃单"来说，跟真实成交价可能不一致——冻结保证金按保守参考价估算、
成交后按真实成交价多退少补，账户最终不会吃亏，细节见
[matching-and-settlement.md](matching-and-settlement.md#冻结保证金的保守估计)。

### `POST /order/cancel/:orderId`

```json
{
  "uid": 10001
}
```

`orderId`是URL路径参数。

### `GET /order/current?uid=10001&symbol=BTCUSDT`

当前挂单（`open`/`partially_filled`状态），`symbol`可省略查全部。

### `GET /order/history?uid=10001`

历史委托，最近100条。

## 条件单（止盈止损/条件开仓）

详细设计见 [conditional-orders.md](conditional-orders.md)。

### `POST /order/conditional/add`

```json
{
  "uid": 10001,
  "symbol": "BTCUSDT",
  "side": "long",
  "action": "close",
  "triggerPrice": 60000,
  "triggerDirection": "gte",
  "type": "market",
  "amount": 0.1,
  "leverage": 10,
  "reduceOnly": true
}
```

字段跟`POST /order/add`基本一致，额外两个必填字段：

| 字段               | 说明                                                             |
|--------------------|------------------------------------------------------------------|
| `triggerPrice`     | 触发价                                                            |
| `triggerDirection` | `gte`=标记价格涨到/超过触发价才触发，`lte`=跌到/低于触发价才触发 |

`type=limit`时`price`是触发后要执行的委托价格（必填）；`type=market`时不需要传`price`，
触发后按当时的标记价成交。`action=open`时会在创建时就冻结保证金（分档/杠杆校验同下单
接口），`action=close`不冻结。返回的id和触发后落地到`orders`表的`orderId`是同一个，
`GET /order/history`能查到触发后的真实委托记录。

### `POST /order/conditional/cancel/:orderId`

```json
{
  "uid": 10001
}
```

只能撤销还没触发（`pending`）的条件单；已经触发的要用`POST /order/cancel/:orderId`
撤销（这时候它已经是一笔真正的委托了）。

### `GET /order/conditional/current?uid=10001&symbol=BTCUSDT`

当前还没触发的条件单，`symbol`可省略查全部。

### `GET /order/conditional/history?uid=10001`

条件单历史（含已触发/已撤销），最近100条。

## 持仓

### `GET /position/current?uid=10001`

返回持仓原始字段+现算的标记价/未实现盈亏/回报率/名义价值/预估强平价。

## 成交

### `GET /trade/history?uid=10001`

历史成交，最近100条。

## 资金费率

### `GET /funding/rate?symbol=BTCUSDT`

当前预估费率+下次结算时间。

### `GET /funding/history?symbol=BTCUSDT`

历史结算记录，最近100条。

### `POST /index-price`

外部行情源推送指数价格，见 [funding-rate.md](funding-rate.md)。

```json
{
  "symbol": "BTCUSDT",
  "price": 64800.5
}
```

## 订单簿深度（`contract-engine`进程，不是`contract-api`）

订单簿只存在于`contract-engine`进程的内存里，这一个接口不在上面`contract-api`的端口
（默认`:7001`）上，而是`contract-engine`自己的轻量HTTP服务（默认`:7002`，
`PERP_ENGINE_HTTP_ADDR`可配），理由见 [order-book.md](order-book.md#为什么深度接口开在contract-engine而不是contract-api)。

### `GET /depth?symbol=BTCUSDT&levels=20`

`levels`可省略，默认20档。返回按价格聚合的深度快照，不含单笔委托的uid/orderID：

```json
{
  "code": 200,
  "message": "success",
  "data": {
    "Bids": [{"Price": "64800", "Volume": "1.5", "Count": 3}],
    "Asks": [{"Price": "64810", "Volume": "0.8", "Count": 1}]
  }
}
```
