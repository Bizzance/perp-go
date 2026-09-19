# 接口鉴权

`contract-api` 的全部接口和 `contract-engine` 的 `/depth` 都要求带有效的请求签名，只有 `/health` 免鉴权（编排探针用）。
方案是 **API Key + HMAC-SHA256 请求签名**，也就是交易所（币安、OKX、Bybit）通行的做法。本文既是协议说明，也是
对接方实现签名的参考。

## 为什么是这个方案

合作方是**服务端**，用户不会直接调本系统。要解决的是"这个请求是不是真的来自合作方"，不是"这个终端用户是谁"——
终端用户的身份验证是合作方自己系统的事，本系统信任合作方传过来的 `uid`。

| 方案                            | 特点                                                                                       |
|---------------------------------|--------------------------------------------------------------------------------------------|
| 静态 Bearer Key                 | 最简单，但密钥每次都在网络上传输，泄露后没有防重放，请求内容不防篡改                       |
| **API Key + HMAC 请求签名**     | 密钥不上网；签名覆盖方法、路径、查询串、请求体，防篡改；时间戳加 nonce 防重放。交易类 API 的事实标准 |
| 合作方自签 JWT                  | 合作方用现成库最省事，但令牌不覆盖请求体（要防篡改得另外加声明）                           |
| OAuth2 client credentials + JWT | 短期令牌、权限范围原生支持，但要有令牌服务，对单个合作方太重                               |
| mTLS                            | 安全性最高，证书运维成本高，通常叠加在别的方式之上                                         |

选 HMAC 签名的理由：

1. **合作方有交易所经验**，这套签名对他们最熟悉，接入成本反而最低。
2. **请求内容被签名覆盖**。我们的部署里 TLS 在网关终止，网关到应用是内网明文，内网里的攻击者不需要攻破 TLS 就能改请求；
   加钱扣钱（`/account/balance`）、发额度（`/account/credit`）是直接动资金的接口，请求体必须防篡改。普通 JWT 做不到这一点。
3. 不需要新增组件：nonce 用现有的 Redis，密钥放环境变量。

## 协议

### 请求头

| Header        | 内容                                                              |
|---------------|-------------------------------------------------------------------|
| `X-Api-Key`   | 密钥标识（公开）                                                  |
| `X-Timestamp` | 毫秒级 Unix 时间戳                                                |
| `X-Nonce`     | 随机串，16~64 位字母数字和 `_-`，**每个请求必须不同**             |
| `X-Signature` | `HMAC-SHA256(secret, 待签名串)` 的十六进制小写                    |

### 待签名串

```
timestamp + "\n" + nonce + "\n" + METHOD + "\n" + path + "\n" + rawQuery + "\n" + sha256hex(body)
```

- `METHOD`：大写，如 `POST`
- `path`：不含查询串，如 `/order/add`。**用请求里实际发送的原始路径**（百分号编码的部分保持编码，如
  `/a%2Fb` 就签 `/a%2Fb`），服务端用 `EscapedPath()` 验证，不用解码后的路径
- `rawQuery`：请求里**实际发送**的查询串，原样，不排序、不重新编码；没有就是空串。选择"原样"而不是规范化，
  是因为不同语言的编码细节不一致，规范化了反而容易签错
- `sha256hex(body)`：对实际发送的**原始字节**算 SHA256 的十六进制小写。GET 或没有请求体就是空串的哈希
  （`e3b0c442...`）。**不要重新序列化 JSON 再算**，不同语言序列化出来的字节不一样

### 固定测试向量

用来验证你的实现（这个向量也是单元测试的一部分，期望值是用 `openssl` 独立算出来的）：

```
secret    = secret-secret-secret-1
timestamp = 1789800000000
nonce     = nonce-0000000001
method    = POST
path      = /order/add
rawQuery  = a=1
body      = {"uid":1}
sha256hex(body) = b1f12b3af58caccd12ac2cf8b4596be4165b1239a395665607e9a9ab3fbb94a5

X-Signature = 0a5fba8e94ec1c1e8d97dc4cfb5d3434b50b1c9c53dd6e8836344e403741dfeb
```

### 示例

`deploy/apisign.sh` 是一个 bash 加 openssl 的完整示例，可以直接用：

```
export PERP_API_KEY=partner-a PERP_API_SECRET=你的secret
deploy/apisign.sh POST 'http://localhost:7001/account/create' '{"uid":10001}'
deploy/apisign.sh GET  'http://localhost:7001/account/info?uid=10001'
```

Python：

```python
import hashlib, hmac, os, time
from urllib.parse import urlsplit
import requests

def call(method, url, key, secret, body=b""):
    u = urlsplit(url)
    ts = str(int(time.time() * 1000))
    nonce = os.urandom(16).hex()
    body_hash = hashlib.sha256(body).hexdigest()
    to_sign = "\n".join([ts, nonce, method, u.path, u.query, body_hash])
    sig = hmac.new(secret.encode(), to_sign.encode(), hashlib.sha256).hexdigest()
    headers = {"X-Api-Key": key, "X-Timestamp": ts, "X-Nonce": nonce, "X-Signature": sig}
    if body:
        headers["Content-Type"] = "application/json"
    return requests.request(method, url, data=body, headers=headers).json()
```

注意 `data=body` 必须发送和算哈希时**完全相同的字节**。

## 服务端校验顺序

1. **头齐全且格式对**，否则 `auth_missing`
2. **时间戳在前后 30 秒内**，否则 `auth_expired`。合作方服务器要开 NTP，时钟偏差超过 30 秒会被拒绝
3. **密钥存在且签名正确**（常量时间比较），否则 `auth_invalid_signature`。**密钥不存在和签名错返回完全相同的响应**，
   并且对不存在的密钥也走一遍完整的 HMAC 计算，避免被拿来枚举有效的密钥
4. **nonce 没用过**，否则 `auth_replayed`。nonce 记在 Redis（`SETNX`，TTL 65 秒，覆盖时间戳的整个有效期）。
   **签名通过之后才记录 nonce**：先记的话，没有密钥的人也能拿一堆假请求把存储塞满
5. **权限范围**：签名通过后检查路由声明的权限范围和这把密钥是否匹配，否则 `forbidden`。**路由必须显式声明
   权限范围（`Auth.Route`），没声明的一律拒绝**：以后新增接口忘了声明，结果是调不通，而不是任何有效密钥都能调。
   有单测遍历真实路由表检查每个路由都声明了范围

**失败关闭**：nonce 存储（Redis）不可用时拒绝请求（返回 500），不会因为防重放存储挂了就放行。

### 错误响应

| errCode                  | code | 含义                                                                   |
|--------------------------|------|------------------------------------------------------------------------|
| `auth_missing`           | 401  | 缺请求头，或者 nonce/时间戳格式不对                                    |
| `auth_expired`           | 401  | 时间戳不在前后 30 秒内。检查合作方服务器的时钟                         |
| `auth_invalid_signature` | 401  | 签名不对，或者密钥不存在                                               |
| `auth_replayed`          | 401  | 这个 nonce 已经用过。每个请求都要生成新的 nonce，**包括重试**          |
| `forbidden`              | 403  | 签名通过了，但这把密钥没有这个接口的权限范围                           |

响应格式同其他接口（HTTP 状态码固定 200，错误在 body 里），见 [api.md](api.md)。

**重试要用新的 nonce 和新的时间戳重新签名**，不能重发同一份请求，否则会被判为重放。幂等靠业务上的
`requestId`（见 [idempotency.md](idempotency.md)），跟签名层的 nonce 是两回事。

## 权限范围

一把密钥带一个或多个权限范围：

| 范围    | 接口                                                                                                   |
|---------|--------------------------------------------------------------------------------------------------------|
| `trade` | 创建账户、下单、撤单、批量撤单、条件单、改杠杆、结束本轮、全部查询类接口、公开行情、WebSocket、引擎的 `/depth` |
| `ops`   | `POST /account/balance`（加钱扣钱）、`POST /account/credit`（发额度）、`POST /account/insured`、`POST /account/status`（冻结/解冻）、`POST /index-price` |

**建议给不同用途的调用方发不同的密钥**：合作方业务后端用 `trade`（需要充值就再加 `ops`），行情/运营来源只给 `ops`。
一把只用来喂指数价的密钥不应该能下单，一把交易密钥也不应该能给账户加钱。`/index-price` 尤其要单独授权：它直接决定
标记价格和资金费率，喂一个假价格就能触发批量强平。

## WebSocket

`GET /ws` 握手时带同样的四个请求头（`method=GET`、`path=/ws`、没有查询串和请求体），需要 `trade` 权限。
握手失败时响应不是 101，响应体里有 `errCode`。标准浏览器 WebSocket API 不能设置请求头，所以这个接口只能由服务端
客户端调用，合作方本来就是服务端，不受影响。连接建立之后订阅任何频道不再需要额外的签名。

## 配置

密钥通过环境变量 `PERP_API_KEYS` 注入，`contract-api` 和 `contract-engine` 读同一份：

```
PERP_API_KEYS=partner-a:<secret>:trade|ops,feeder:<secret>:ops
```

- 格式 `id:secret:范围`，多把用逗号分隔，范围用 `|` 分隔
- secret 至少 16 位，里面不能有 `:` `,` `|`。生成：`openssl rand -hex 24`。环境变量模板里的占位值
  `CHANGE_ME` **原样带进去会拒绝启动**（哪怕补长到 16 位以上也拒绝），防止忘了改的部署带着写在仓库里的密钥上线
- **secret 存的是明文，不是哈希**：服务端要用它重新计算签名，验证签名必须知道原始密钥。所以放环境变量或密钥管理服务，
  不落业务数据库、不写日志、不提交到 git
- 配置格式不对（secret 太短、范围不合法、id 重复）时进程**拒绝启动**并说明原因

**默认开启鉴权**：开启但一个密钥都没配置时进程拒绝启动，不会悄悄退化成"没有鉴权"。本地开发用
`PERP_AUTH_DISABLED=true` 显式关闭，启动时会打醒目的警告；`make run-*` 目标已经带了这个变量。**生产绝对不能设。**

### 轮换密钥

支持多把密钥同时有效，所以轮换是平滑的：新增一把密钥、让合作方切换、确认旧密钥没有流量后再删掉旧的，中间没有停机。

## 部署时要注意

- **网关不能改写请求**：签名覆盖路径、查询串和请求体。如果网关做了路径前缀改写、查询串重排或请求体转换，
  签名会对不上。合作方签名用的必须是应用**实际收到**的路径和查询串
- **请求体上限 64KB**：鉴权要读完整个请求体算哈希，而这一步发生在确认调用方身份之前，所以上限要小，避免没有
  密钥的人拿大请求体撑内存。声明的 `Content-Length` 超限直接拒绝、不读取；分块传输的也只会读到上限。超限返回
  `invalid_param`（这个判断跟密钥无关，不会泄露密钥是否存在）。本系统的请求体都是几百字节的 JSON
- TLS 仍然要在网关或负载均衡层终止。签名不加密内容，传输保密靠 TLS

## 已经验证过的行为

用真实容器环境（含 Redis nonce 存储）逐项验证：无签名、错误 secret、不存在的密钥（响应与签名错完全一致）、
同一 nonce 重放、过期时间戳、签名后篡改请求体、权限越界（trade 密钥调运营接口、ops 密钥下单）、
`/health` 免鉴权、引擎 `/depth` 的保护、WebSocket 握手（无签名、错误签名、权限不足都被拒绝）、
以及带鉴权的完整业务流程（创建账户、充值、喂价、撮合、持仓、结束本轮）。

## 还没做

| 项目            | 说明                                                                                                             |
|-----------------|------------------------------------------------------------------------------------------------------------------|
| **限流**        | 按密钥限流（Redis 计数、滑动窗口），超限返回 429。下单类和查询类分开配额                                         |
| **IP 白名单**   | 合作方是固定出口的服务端，可以在密钥上再绑一组来源 IP，作为签名之外的第二道防线                                  |
| **uid 归属**    | 只有一个合作方时不需要。以后有多个，需要防止合作方 A 用自己的密钥操作合作方 B 的 `uid`：按 uid 分段，或者账户表加 `partner_id`，在 `POST /account/create` 创建账户时绑定 |
| 私有频道归属校验 | WS 订阅 `user:{uid}` 时检查该 uid 是否属于这把密钥，依赖上面的 uid 归属                                          |
| 审计日志        | 目前只记录被拒绝的请求（权限不足）。通过鉴权的请求没有单独的审计记录                                             |
