# 部署

两个Go服务打成Docker镜像，用Docker Compose编排。测试/联调环境把MySQL、Redis、Kafka也用容器拉起来；
**生产环境这三个用云厂商的托管服务**，编排里只有两个应用服务。本文的命令和现象都在真实的容器环境里验证过。

## 拓扑与依赖

```
                      ┌─────────────┐   Kafka    ┌──────────────────┐
  合作方 ── 网关 ──▶  │ contract-api│──────────▶ │ contract-engine  │
  (TLS+鉴权)  :7001   │  (无状态)   │            │ (有状态，订单簿) │  :7002
                      └─────┬───────┘            └───────┬──────────┘
                            └────────────┬───────────────┘
                                MySQL · Redis · Kafka
```

| 组件              | 端口 | 说明                                                              |
|-------------------|------|-------------------------------------------------------------------|
| `contract-api`    | 7001 | 对外接口 + WebSocket网关，无状态，可多实例                        |
| `contract-engine` | 7002 | 撮合/风控/条件单/资金费率，**有状态**（订单簿在内存），默认单实例 |
| `orderbook-sync`  | 无   | 订单簿同步：把币安的订单簿和指数价同步进我们系统，用户下单的对手方就是系统。**生产必须部署**，见 [orderbook-sync.md](orderbook-sync.md) |
| MySQL 8.x         | 3306 | 用`sql/schema.sql`初始化                                          |
| Redis             | 6379 | 标记价、指数价、分布式锁、WS推送                                  |
| Kafka             | 9092 | 下单/撤单/结束本轮事件                                            |

## 文件清单

```
deploy/
  Dockerfile                 两个服务共用的多阶段构建，target=api / engine
  docker-compose.yml         两个应用服务（生产只用这个）
  docker-compose.deps.yml    叠加层：MySQL、Redis、Kafka、topic 预创建（仅测试/联调）
  .env.test.example          测试环境变量模板，开箱即用
  .env.prod.example          生产环境变量模板，填 CHANGE_ME
.dockerignore                构建上下文排除项
```

`deploy/.env`（真实的环境变量文件）含密码，已在`.gitignore`里， **不要提交**。

## 构建镜像

```
make docker-build IMAGE_TAG=$(git rev-parse --short HEAD)
```

等价于对`deploy/Dockerfile`分别构建`api`和`engine`两个target。产物是静态二进制，运行镜像基于
`alpine:3.20`，以非root用户运行，`api`约57MB、`engine`约42MB。

**生产建议用具体的版本号（提交哈希）打 tag，不要用`latest`**，这样回滚时能精确回到上一个版本。

构建时下载 Go 模块，如果构建机访问官方代理慢或不通（比如国内网络），换代理：

```
docker build -f deploy/Dockerfile --target api --build-arg GOPROXY=https://goproxy.cn,direct -t perp-go/api .
```

Compose里通过环境变量`GOPROXY`传入同一个参数。

## 测试 / 联调环境

依赖一起拉起来，依赖的端口 **不对宿主机发布**，只在Compose内部网络里互相访问。

```
cp deploy/.env.test.example deploy/.env      # 按需改密码；密码里别用 @ : / 等会破坏 DSN 的字符
sed -i "s/CHANGE_ME/$(openssl rand -hex 24)/" deploy/.env   # 生成 API 密钥；不换的话进程拒绝启动
make compose-test-up                         # 等价于 docker compose ... up -d --build
```

启动顺序由健康检查保证：MySQL、Redis、Kafka 健康 → 一次性任务`kafka-init`预创建三个topic →
`contract-engine` 健康 → `contract-api`。MySQL 首次启动时会自动执行 `sql/schema.sql`（数据卷为空才执行）。

验证：

```
curl localhost:7001/health                    # {"code":200,...,"data":{"status":"ok",...}}
curl localhost:7002/health
docker compose --env-file deploy/.env -f deploy/docker-compose.yml -f deploy/docker-compose.deps.yml ps
```

业务冒烟（创建账户 → 充值 → 喂指数价 → 下单撮合）。接口要求签名，用 `deploy/apisign.sh`（测试环境模板里的密钥
`partner-a` 同时带 `trade` 和 `ops`）：

```
export PERP_API_KEY=partner-a
export PERP_API_SECRET=$(grep -o 'partner-a:[^:]*' deploy/.env | cut -d: -f2)   # 你上面生成的 secret
deploy/apisign.sh POST localhost:7001/account/create  '{"uid":1001}'
deploy/apisign.sh POST localhost:7001/account/balance '{"uid":1001,"amount":10000,"requestId":"dep-1"}'
deploy/apisign.sh POST localhost:7001/index-price     '{"symbol":"BTCUSDT","price":60000}'
# 再用另一个 uid 下一买一卖两笔同价限价单，见 api.md
```

拆除（ **连数据卷一起删**，下次启动会重新初始化数据库）：

```
make compose-test-down
```

## 生产环境

### 1. 准备托管服务

| 服务  | 要求                                                                                                                                             |
|-------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| MySQL | 8.x，字符集 utf8mb4。先手工执行 `sql/schema.sql` 初始化（见下面"初始化数据库"）。应用账号只需要 `perpgo` 库的读写权限。**开启自动备份和 binlog** |
| Redis | 设置密码。里面是标记价、指数价、锁和资金费率采样累加器，丢了不会丢资金，但**指数价需要重新喂**、当前周期的资金费率采样会丢失。建议开持久化       |
| Kafka | 预先创建三个 topic（见下面命令）。**保留时长建议不低于 7 天**（跟应用里 Kafka 消息去重记录的保留期 168 小时对齐）                                |

创建 topic（副本数按你的集群来，生产建议 3）：

```
for t in perpgo.order.submit perpgo.order.cancel perpgo.round.close; do
  kafka-topics.sh --bootstrap-server $BROKER --create --if-not-exists \
    --topic $t --partitions 1 --replication-factor 3 --config retention.ms=604800000
done
```

分区数 1 对应默认的单实例 engine（同一个 symbol 的事件按 key 落到同一分区来保证顺序）。

### 2. 初始化数据库

```
mysql -h $HOST -u $ADMIN -p < sql/schema.sql
```

脚本可重复执行，不会报错也不会让种子数据翻倍。 **注意脚本末尾有"演示用"的初始数据**：BTCUSDT、ETHUSDT 两个合约的
手续费率、数量精度、保证金分档。上线前必须核对这些是不是你要的业务参数（`coins` 表和 `risk_limit_tiers` 表），
不是的话直接改脚本再执行，或者执行后手工调整。

### 3. 配置

```
cp deploy/.env.prod.example deploy/.env      # 把所有 CHANGE_ME 换成真实值
```

| 变量（应用读取）              | 说明                                                                                                            | 默认值（代码里）                 |
|-------------------------------|-----------------------------------------------------------------------------------------------------------------|----------------------------------|
| `PERP_MYSQL_DSN`              | `user:pass@tcp(host:3306)/perpgo?parseTime=true&loc=UTC`，`loc` 要跟 `TZ` 一致                                  | 本地开发用的默认值，**必须覆盖** |
| `PERP_REDIS_ADDR`             | `host:6379`                                                                                                     | `127.0.0.1:6379`                 |
| `PERP_REDIS_PASS`             | Redis 密码                                                                                                      | 本地开发用的默认值，**必须覆盖** |
| `PERP_KAFKA_BROKER`           | `host:9092`                                                                                                     | `127.0.0.1:9092`                 |
| `PERP_API_ADDR`               | api 监听地址                                                                                                    | `:7001`                          |
| `PERP_ENGINE_HTTP_ADDR`       | engine 的 HTTP 监听地址                                                                                         | `:7002`                          |
| `PERP_API_KEYS`               | 接口鉴权的密钥，格式 `id:secret:trade\|ops`，多把逗号分隔。**必填**，没配置进程拒绝启动                         | 空                               |
| `PERP_AUTH_DISABLED`          | `true` 关闭鉴权，**只给本地开发用，生产绝不能设**                                                               | `false`                          |
| `PERP_NODE_ID`                | 雪花 ID 的节点号，**每个实例必须不同**                                                                          | api=0，engine=1                  |
| `PERP_ENGINE_SYMBOLS`         | engine 分片：本实例负责的 symbol，逗号分隔。留空=负责全部（单实例）                                             | 空                               |
| `PERP_MARK_REQUIRE_INDEX`     | `true`=没有指数价就不产生标记价，**生产必须设**；false 时没喂过指数价的合约标记价退回最新成交价，可被自成交操纵 | `false`                          |
| `PERP_MARK_MAX_INDEX_AGE_SEC` | 指数价多久没更新算断供（标记价冻结、强平/资金费率/条件单暂停）                                                  | `30`                             |
| `PERP_MARK_MAX_DEVIATION`     | 标记价相对指数价的最大偏离比例                                                                                  | `0.01`                           |
| `BOOKSYNC_*`（orderbook-sync读取，不是应用读取）| 密钥、合约、同步档数、间隔、币安数据过期时间，见 [orderbook-sync.md](orderbook-sync.md) | 见文档 |
| `PERP_MARK_BASIS_WINDOW_SEC`  | 盘口基差取多长时间窗口的平均                                                                                    | `60`                             |
| `PERP_INDEX_MAX_JUMP`         | `POST /index-price`服务端跳变保护：一次推送变动超过这个比例，新价位要持续几秒才承认。**生产建议设`0.05`**，测试环境留空 | 空（不校验）                     |
| `PERP_INDEX_JUMP_CONFIRM_SEC` | 超过上面阈值的新价位要持续多少秒才承认                                                                          | `3`                              |

Compose 层的变量：`IMAGE_TAG`、`BIND_ADDR`、`API_PORT`、`ENGINE_PORT`、`API_NODE_ID`、`ENGINE_NODE_ID`、`TZ`。
代码里的默认密码只是为了本地开发方便，写在源码里， **生产一定要用环境变量覆盖**；必填的变量缺失时 Compose 会直接
报错拒绝启动，不会悄悄用默认值。

### 4. 启动

```
make compose-prod-up      # 等价于 docker compose --env-file deploy/.env -f deploy/docker-compose.yml up -d
```

启动顺序：先 `contract-engine`（启动时先重建订单簿， **失败会直接退出**，不会带着不完整的订单簿运行），健康后再启
`contract-api`。

### 5. 安全清单（上线前必须过一遍）

- [ ] **接口鉴权已开启**：`PERP_API_KEYS` 配置了真实密钥（`openssl rand -hex 24` 生成 secret）， **`PERP_AUTH_DISABLED` 没有设成
  true**。给不同用途发不同的密钥：合作方业务后端 `trade`（需要充值再加 `ops`），
  行情/运营来源只给 `ops`。方案和协议见 [auth-design.md](auth-design.md)
- [ ] 7001、7002 仍然建议只放内网，前面有 TLS 网关。Compose 里端口默认只绑 `127.0.0.1`（`BIND_ADDR`），
  网关和这台机器通网时再改成内网地址， **不要绑 `0.0.0.0` 直接暴露到公网**
- [ ] **网关没有改写路径、查询串、请求体**（签名覆盖这三样，改写会让签名对不上）
- [ ] 限流和 IP 白名单还没做（见 auth-design.md"还没做"），需要的话先在网关层做
- [ ] **订单簿和指数价来源已经接好**：`PERP_MARK_REQUIRE_INDEX=true`，并且 orderbook-sync 在跑（`COMPOSE_PROFILES=booksync`，
  `PERP_API_KEYS` 里加了一把 `booksync:<secret>:trade|ops`，`BOOKSYNC_API_KEY_ID`/`BOOKSYNC_API_SECRET` 填对，见
  [orderbook-sync.md](orderbook-sync.md)）。没有它订单簿是空的，没有指数价就没有标记价（市价单、强平都不能用）；超过 30 秒没喂价强平会暂停。
  不开 `PERP_MARK_REQUIRE_INDEX` 的话没喂过指数价的合约标记价退回最新成交价，两个账户对敲一笔就能推动别人的强平线，见 [mark-price.md](mark-price.md)。
  **部署地区要能稳定访问币安的行情接口**（部分地区返回 451）
- [ ] **指数价的服务端跳变保护已开**：`PERP_INDEX_MAX_JUMP=0.05`（`.env.prod.example`已经设了）；orderbook-sync 日志里的
  `[ERROR] 币安数据已经超过…撤掉全部挂单、暂停报价` 接进了告警（此时订单簿是空的），见 [orderbook-sync.md](orderbook-sync.md)
- [ ] MySQL、Redis 密码已经覆盖默认值
- [ ] MySQL、Redis、Kafka 只对应用所在网络开放，不暴露公网
- [ ] `deploy/.env` 没有提交到 git
- [ ] TLS 在网关或负载均衡层终止（应用本身不处理 TLS）
- [ ] `sql/schema.sql` 末尾的演示合约参数已经核对
- [ ] MySQL 自动备份已经开启，并演练过一次恢复

## 日常运维

### 健康检查

`contract-api` 和 `contract-engine` 都有 `GET /health`，Compose 里已经配好，容器状态里能直接看到 `healthy`。
`engine` 的探针通过就代表订单簿已经恢复完整。

### 日志

应用日志输出到 stdout，Compose 用 `json-file` 驱动，单个文件 50MB、保留 5 个。生产建议接日志采集。
引擎恢复订单簿时的关键日志：

```
订单簿重建完成，重放N笔活跃委托(共查到N笔待恢复)，其中M笔重放时补上了撮合
```

`M` 大于 0
说明引擎宕机期间有订单落库但没来得及处理，恢复时已经补上了撮合，见 [order-book-recovery.md](order-book-recovery.md)。

### 发布（升级版本）

```
make docker-build IMAGE_TAG=<新版本>
# 把 deploy/.env 里的 IMAGE_TAG 改成 <新版本>
docker compose --env-file deploy/.env -f deploy/docker-compose.yml up -d
```

Compose 会重建有变化的服务。几点说明：

- **两个服务收到 SIGTERM 都会优雅退出**（`contract-api` 等在途请求处理完，最多 15 秒），实测 `docker stop` 耗时不到 1 秒、
  退出码 0
- **`contract-engine` 重启期间有短暂的撮合空窗**：`contract-api` 照常受理下单、落库、把事件发到 Kafka，引擎起来后
  会自动补处理（真实验证过：停引擎期间下的两笔同价买卖单，引擎启动后正确成交）。窗口一般是几秒
- **`contract-api` 单实例重启期间接口不可用**。要不停机发布，前面放负载均衡、跑两个 api 实例（见下面"扩容"），滚动重启
- 发布前先在测试环境用同一个镜像 tag 跑一遍冒烟。代码层面的全链路冒烟有 `make test-e2e`（拉一套一次性的完整系统，
  验证下单/撤单/冻结/结束本轮/WebSocket 推送/引擎重启恢复，约 1.5 分钟，见 [testing.md](testing.md)），发布前跑一次

### 回滚

把 `IMAGE_TAG` 改回上一个版本再 `up -d`。 **数据库结构变更不会自动回滚**：`schema.sql` 只是建表脚本，没有迁移机制
（见 [architecture.md](architecture.md)"数据库"），涉及表结构变更的版本回滚要单独评估。

**已经有数据的库升级到带新表结构的版本**：`schema.sql` 只有最终形态的 `CREATE TABLE`，不含 `ALTER`，
对已存在的表不会补新列。测试环境直接重建库（`make compose-test-down` 连卷删掉再起）；生产环境要人工对照
`schema.sql` 写迁移。举例：账户冻结功能给 `accounts` 加了 `status`/`status_reason`/`status_time` 三列，
并新增了 `account_status_history` 表。

### 备份

以 MySQL 为准：自动备份加 binlog，账户、委托、成交、资金流水都在里面。订单簿在引擎重启时从 MySQL 重建。Kafka 里的
消息是"待处理的事件"，不是权威数据。

## 扩容

- **`contract-api`**：无状态，多实例 + 负载均衡。 **每个实例的 `PERP_NODE_ID` 必须不同**（雪花 ID 靠它避免重复，默认 0），
  同一个值跑多个实例理论上会生成重复 ID。Compose 里加第二个实例的做法：复制 `api` 服务，改服务名、
  `PERP_NODE_ID`、宿主机端口
- **`contract-engine`**：有状态，同一个 symbol 的订单簿只能在一个实例里。默认单实例；要扩容按 symbol 分片，设
  `PERP_ENGINE_SYMBOLS` 和 **每个实例显式且互不相同的 `PERP_NODE_ID`**
  （缺了启动直接失败），详见 [engine-sharding.md](engine-sharding.md)

## 已知限制

- **`contract-engine` 是单点**：不管用什么编排，它宕机期间撮合停止。容器的 `restart: unless-stopped` 能让它快速重启，
  但不能让它不中断。真要高可用需要按 symbol 分片加热备，是后续的事
- **Compose 本身不是高可用方案**：单机部署，宿主机宕机就全停。上生产建议至少把数据服务放在托管服务里（本文的做法），
  应用层再评估是否需要多机 + 负载均衡
- **限流、IP 白名单、多合作方的 uid 归属没有做**，见 [auth-design.md](auth-design.md)
  "还没做"、[known-limitations.md](known-limitations.md)
- 健康检查每 10 秒一次，会在访问日志里留下 `GET /health` 的记录，属于正常现象

## 故障排查

| 现象                                                      | 原因 / 处理                                                                                                                                                                                 |
|-----------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `docker compose up` 报 `required variable ... is missing` | `deploy/.env` 里缺必填变量，按报错补上。这是有意的：缺配置时拒绝启动，而不是悄悄用默认值                                                                                                    |
| 构建卡在 `go mod download`                                | 构建机访问官方 Go 模块代理慢或不通。换代理：`--build-arg GOPROXY=https://goproxy.cn,direct`                                                                                                 |
| engine 一直重启                                           | 看 `docker logs`：连不上 MySQL/Redis（检查地址、密码、网络）；`恢复订单簿失败`（数据库异常，进程会主动退出）；分片时没设 `PERP_NODE_ID`                                                     |
| 下单成功但一直不成交                                      | 看 engine 日志和消费者组：`kafka-consumer-groups.sh --describe --group contract-engine`。topic 没预建时消费者需要几秒发现（已自动处理）；engine 没起来则事件在 Kafka 里积压，起来后会补处理 |
| 订单簿里买卖价格交叉挂着                                  | 已修复的缺陷（旧版本恢复订单簿的问题），升级到当前版本即可，见 [order-book-recovery.md](order-book-recovery.md)                                                                             |
| 所有请求返回 `auth_*` 错误                                | `auth_expired`：调用方服务器时钟偏差超过 30 秒（开 NTP）。`auth_invalid_signature`：签名算法对不上，先用 auth-design.md 里的固定测试向量校验；网关改写了路径/查询串/请求体也会导致这个错误  |
| 进程启动就退出，日志提示配置密钥                          | 鉴权默认开启，必须配置 `PERP_API_KEYS`；secret 还是模板里的 `CHANGE_ME` 占位值也会拒绝启动（防止带着写在仓库里的密钥上线）；本地开发才用 `PERP_AUTH_DISABLED=true`                          |
| 端口被占用                                                | 改 `deploy/.env` 里的 `API_PORT`、`ENGINE_PORT`                                                                                                                                             |
| MySQL 容器起来了但表不存在                                | `schema.sql` 只在数据卷为空时才自动执行。测试环境 `make compose-test-down` 连卷删掉再起；生产环境手工执行                                                                                   |
