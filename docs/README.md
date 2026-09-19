# perp-go 设计文档

U本位永续合约交易系统，MVP阶段。文档按主题拆分，方便单独查阅和后续维护：

- [code-reading-guide.md](code-reading-guide.md) —— 第一次看这个代码库从哪进去、按什么
  顺序看、贯穿全代码库的设计模式速览
- [architecture.md](architecture.md) —— 进程划分、数据流、技术选型
- [account-and-margin.md](account-and-margin.md) —— 账户模型、全仓保证金、冻结/结算规则
- [matching-and-settlement.md](matching-and-settlement.md) —— 撮合引擎、下单校验、成交结算
- [order-book.md](order-book.md) —— 订单簿数据结构、自成交保护、深度查询
- [order-book-recovery.md](order-book-recovery.md) —— 订单簿的进程重启恢复机制
- [engine-sharding.md](engine-sharding.md) —— engine横向扩展（按symbol静态分片）
- [kline.md](kline.md) —— K线聚合
- [websocket.md](websocket.md) —— WebSocket实时推送
- [message-dedup.md](message-dedup.md) —— Kafka消息级去重
- [conditional-orders.md](conditional-orders.md) —— 条件单（止盈止损/条件开仓）
- [risk-limit-tiers.md](risk-limit-tiers.md) —— 保证金分档（风险限额）
- [leverage.md](leverage.md) —— 独立杠杆设置接口
- [funding-rate.md](funding-rate.md) —— 资金费率机制
- [liquidation.md](liquidation.md) —— 强平与保险基金
- [deployment.md](deployment.md) —— 部署：Docker镜像、Compose编排、生产环境清单、发布与故障排查
- [api.md](api.md) —— HTTP接口说明（合作方对接文档：约定、错误码、每个接口的请求/响应）
- [idempotency.md](idempotency.md) —— 接口幂等性：哪些接口需要、`requestId`/`round`怎么做、业界方案对比
- [auth-design.md](auth-design.md) —— 鉴权方案（设计稿，尚未实现）
- [known-limitations.md](known-limitations.md) —— 已知限制、明确排除项、下一步计划

这些文档记录的是 **当前代码的实际行为**，不是需求文档或历史演进记录——改代码时如果行为变了，要顺手把对应文档改掉，不要让文档和代码分叉。
