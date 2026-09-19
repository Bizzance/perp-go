# 测试

两层测试，用构建标签分开：

| 层     | 怎么跑                 | 依赖                     | 测什么                                                         |
|--------|------------------------|--------------------------|----------------------------------------------------------------|
| 单元   | `make test`            | 无                       | 纯逻辑：撮合订单簿、签名/鉴权、参数解析、JSON格式、配置校验     |
| 集成   | `make test-integration`| docker（自动拉容器）     | 碰数据库/Redis的逻辑：仓库的事务、引擎撮合前的检查、接口的完整处理链路 |

普通的`go test ./...`只跑单元测试，速度快、不需要任何外部依赖。集成测试文件顶部有
`//go:build integration`，不加标签编译不进去。

## 跑集成测试

```bash
make test-integration                                   # 全部，约15秒
make test-integration ARGS='-v ./internal/service/'     # 只跑一个包，ARGS原样传给go test
make test-integration ARGS='-v -run TestEngineFreeze ./internal/service/'
```

`scripts/test-integration.sh`做的事：起一个一次性的MySQL 8.4和Redis 7容器（端口随机、只绑
`127.0.0.1`），等它们真的能接连接，设好环境变量，跑`go test -tags=integration -count=1 -p 1`，
结束后不管成败都删掉容器。需要docker和go，不需要提前建库。

直接`go test -tags=integration`而没设环境变量时测试会**失败**并提示用`make test-integration`，
不会悄悄跳过——标了integration就是明确要跑，"跳过却显示通过"会给人错误的安心。

| 环境变量               | 含义                                                       |
|------------------------|------------------------------------------------------------|
| `PERP_TEST_MYSQL_DSN`  | 管理员DSN，不带库名，要有建库/删库权限                     |
| `PERP_TEST_REDIS_ADDR` | Redis地址                                                  |
| `PERP_TEST_REDIS_PASS` | Redis密码，没有就不设                                      |

## 隔离方式

- **MySQL：每个测试一个独立的库**。`testutil.NewDB(t)`建一个随机名字的库，执行`sql/schema.sql`
  （去掉里面的`CREATE DATABASE`/`USE`），返回连接，测试结束自动删库。所以测试跑的表结构和种子数据
  就是生产用的那份，改了`schema.sql`测试自动跟着变；测试之间互不影响，可以放心写任意脏数据。
- **Redis：共用一个实例，靠随机uid隔离**。锁、nonce这类键里都带uid，`testutil.UIDBase()`给每个
  测试一段随机的大uid，同一个测试里的多个账户用它加偏移。标记价格这类按symbol的键是共享的，
  所以不同包的测试用`-p 1`串行，同一个包里的集成测试不要用`t.Parallel()`。
- **Kafka：不起**。API层的`Server.producer`是个小接口（`eventPublisher`），测试里换成
  `fakePublisher`，只记录发了哪些事件、可以让它模拟失败。引擎服务本来就是直接调用，不经过Kafka。

## 测试夹具

- `internal/testutil`：`NewDB`、`NewCache`、`UIDBase`。
- `internal/service/testenv_integration_test.go`：`newEngineEnv`，按`cmd/contract-engine`的接线方式
  搭一整套引擎服务，加上造账户、造委托、造条件单的小工具。`restartEngine`会换一个全新的空订单簿、
  沿用数据库，用来测重启恢复。
- `internal/api/freeze_integration_test.go`：`newAPIEnv`，带真实仓库的API服务（鉴权关闭，
  `Kafka`换成替身），通过`httptest`直接调路由。

造委托的工具是**直接落库**，不走下单接口，也不发事件——测引擎时由测试决定什么时候把委托交给
`SubmitOrder`，能精确构造"落库了但引擎还没处理"这类时序。

## 写新的集成测试

1. 文件顶部加`//go:build integration`
2. 用`newEngineEnv(t)`或`newAPIEnv(t)`拿夹具，不要自己建连接
3. 用`testutil.UIDBase()`加偏移生成uid，别写死
4. **写完做一次变异检查**：临时把被测的判断条件改坏（比如`if false && ...`），确认对应的测试会
   变红，再改回来。第一次跑就全绿的测试不代表它能抓到问题——账户冻结那组测试就靠这个发现过一个
   假阳性：条件单用市价单时，"因为冻结被撤销"和"没有对手盘被撤销"结果一样，关掉冻结检查测试照样通过，
   改成限价单才分辨得出来。

## 已经覆盖的和没覆盖的

集成测试目前覆盖账户冻结这条链路：仓库的状态变更事务和并发、引擎撮合前的冻结兜底（正常挂单、被撤销
退款、平仓放行、强平放行、不成交、失败关闭、重启恢复、条件单触发）、`POST /account/status`的清理逻辑
和部分失败重试、三个拦截点、幂等重放优先于冻结检查。

没覆盖的：

- 条件开仓单创建后的"落库后再查一次冻结状态"：要制造"冻结刚好卡在检查和落库之间"的竞态，需要
  在代码里埋钩子，先没做
- 撮合成交、结算、强平、资金费这些老功能：过去是靠隔离环境里手工端到端验证的，还没有自动化的
  集成测试。夹具已经能搭出完整的引擎服务，后续可以按同样的方式补
- Kafka消费者、WebSocket：没有起Kafka，这两块仍然靠手工验证
