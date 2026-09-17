# HTTP接口

`contract-api`进程对外提供的接口。MVP阶段鉴权用明文`uid`参数占位（不做HMAC/token校验），
`uid`都是独立传参，方便后续直接换成鉴权中间件注入、不用改业务代码。

**约定**：GET查询接口走query string；POST写接口统一用JSON body
（`Content-Type: application/json`），不用表单编码。

统一响应格式：

```json
{"code": 200, "message": "success", "data": {...}}
```

失败时`code`是HTTP语义的错误码（400/500等，不是HTTP状态码本身——HTTP状态码固定
返回200，错误信息在body里的`code`字段），`message`是错误描述。

## 账户

### `POST /account/balance`

合作方/运营调整账户可用余额（不是用户提现接口，也不经过任何第三方支付/风控）。

```json
{"uid": 10001, "amount": 1000}
```

`amount`正数=加钱，负数=扣钱（扣的时候必须有足够`available`）。

### `GET /account/info?uid=10001`

返回账户原始字段+现算的未实现盈亏/权益，见 [account-and-margin.md](account-and-margin.md)。

## 委托

### `POST /order/add`

```json
{
  "uid": 10001,
  "symbol": "BTCUSDT",
  "side": "LONG",
  "action": "OPEN",
  "type": "LIMIT",
  "price": 65000,
  "amount": 0.1,
  "leverage": 10,
  "reduceOnly": false
}
```

字段说明：

| 字段 | 必填 | 说明 |
|---|---|---|
| `side` | 是 | `LONG` / `SHORT`，其它值一律拒绝（不会被撮合引擎当成默认方向悄悄放行） |
| `action` | 是 | `OPEN` / `CLOSE`，其它值一律拒绝 |
| `type` | 否 | `LIMIT`（默认）/ `MARKET`，其它值一律拒绝（不会被当成MARKET处理） |
| `price` | LIMIT单必填 | MARKET单忽略此字段，按标记价格估算 |
| `amount` | 二选一 | 标的币数量 |
| `marginAmount` | 二选一 | 用保证金金额+杠杆反推数量：`amount = marginAmount * leverage / price` |
| `leverage` | 否 | 默认1；**必须是整数**，显式传0或小数都会报错，不会被当成"没传"或被截断 |
| `reduceOnly` | 否 | 默认false |

`amount`和`marginAmount`必须传一个，两个都传优先用`marginAmount`。`leverage`/
`marginAmount`/`amount`这三个字段的JSON类型是可选指针——"没传这个字段"和"传了显式的0"
是两种不同的语义，不能混为一谈，见下方注意事项。

完整的下单校验链见 [matching-and-settlement.md](matching-and-settlement.md)。

**注意**：由于contract-api在下单时看不到contract-engine那边订单簿的真实状态，`price`
字段对于会立刻成交的"吃单"来说，跟真实成交价可能不一致——见
[known-limitations.md](known-limitations.md)。

### `POST /order/cancel/:orderId`

```json
{"uid": 10001}
```

`orderId`是URL路径参数。

### `GET /order/current?uid=10001&symbol=BTCUSDT`

当前挂单（`NEW`/`PARTIALLY_FILLED`状态），`symbol`可省略查全部。

### `GET /order/history?uid=10001`

历史委托，最近100条。

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
{"symbol": "BTCUSDT", "price": 64800.5}
```
