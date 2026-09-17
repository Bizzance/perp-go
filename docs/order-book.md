# 订单簿（`internal/matching`）

## 数据结构

`Book`（一个symbol一个）内部是"档位数组+组内链表"的组合，不是单纯的一个大切片：

```
bids/asks []*priceLevel   // 按价格排序的档位数组，sort.Search二分定位
                          // bids: 价格从高到低；asks: 价格从低到高
priceLevel {
    head, tail *orderNode  // 这一档挂单的FIFO双向链表（价格-时间优先里"时间"的体现）
    totalVolume, count      // 这一档的汇总量/笔数，O(1)读取
}
byID map[uint64]*orderNode // orderID → 链表节点，O(1)定位
```

这是真实撮合引擎常见的组合方式，不是MVP简化：活跃价格档位数量（m）远小于挂单总笔数（n），
best price永远在数组端点，撮合热路径（从最优价往外扫）直接读数组是O(1)；档位数组本身的
增删（只在某个价格第一次/最后一次有单时发生）频率远低于订单级别的操作，用`sort.Search`
二分查找+切片插入删除（O(log m)+O(m)）完全够用，不需要为此手写平衡树/跳表或引入第三方
依赖——这个取舍在"不过度设计"和"复杂度达标"之间是刻意选的一个点，不是偷懒。

**复杂度对照**（旧实现是`[]*RestingOrder`+每次插入排序，全部O(n)扫描+O(n)切片重建）：

| 操作                 | 旧实现 | 新实现                                    |
|----------------------|--------|--------------------------------------------|
| 按orderID撤单        | O(n)   | O(1)（map查找+链表摘除；档位空了才有O(log m)+O(m)） |
| 挂单进已有价位        | O(n)   | O(log m)二分+O(1)链表尾插                  |
| 挂单进新价位          | O(n)   | O(log m)二分+O(m)数组插入                  |
| 撮合吃掉最优价一笔单   | O(1)   | O(1)                                       |
| 深度快照(前N档)       | 不支持 | O(N)                                       |

## 自成交保护（STP）：取消maker

撮合（`Book.Match`）过程中，如果即将成交的对手（maker，簿子上挂着的那笔）跟主动吃单方
（taker）是**同一个uid**，不产生这笔成交——把这个maker当正常撤单摘掉，taker继续往下
尝试撮合（同价位下一笔，或者下一个价位），直到吃满自己的量或者没有非自己的对手盘为止。

这是主流交易所（如Binance）的默认STP行为，理由：

- **不打乱其他人的排队顺序**：自己的maker被摘掉后，同一档后面排队的其他人自然前移，
  不影响他们的价格-时间优先级
- **taker仍然能被非自己的流动性成交**：不会因为撞到自己的挂单就让整笔taker作废

`Match`的返回值是`(fills []Fill, selfCanceled []*RestingOrder)`——第二个返回值就是被
STP摘掉的maker列表，调用方（`EngineService.SubmitOrder`）要对每一笔都做跟正常撤单
完全一样的收尾（DB标记CANCELED、按剩余量退回冻结保证金），复用`finalizeOrderCancel`
（`internal/service/engine.go`），不是另写一套。

这不是假设性场景——本次会话测试信用额度/条件单功能时，因为测试脚本让同一个uid在买卖
两边都挂了单，实际触发过自成交，当时当成"测试数据设计问题"处理，现在已经在撮合引擎层面
根治，不再依赖调用方/测试数据自己小心避免。

## 深度查询：`Book.Depth(maxLevels int) DepthSnapshot`

按价格档位聚合的快照，只暴露价格/该档总量/该档笔数，**不暴露单笔委托的uid/orderID**——
公开的深度数据不该泄露个人挂单归属，这是有意的设计，不是遗漏。

```go
type PriceLevel struct {
    Price  decimal.Decimal
    Volume decimal.Decimal
    Count  int
}
type DepthSnapshot struct {
    Bids []PriceLevel // 价格从高到低
    Asks []PriceLevel // 价格从低到高
}
```

`maxLevels<=0`表示不限（返回全部档位）。

### 为什么深度接口开在`contract-engine`而不是`contract-api`

订单簿（`matching.Engine`）只存在于`contract-engine`进程的内存里，`contract-api`完全
看不到。要在`contract-api`上暴露深度接口，要么①在`contract-api`里也维护一份订单簿并跟
`contract-engine`保持同步（引入跨进程状态一致性问题，复杂且容易出错）②直接把接口开在
拥有这份内存状态的进程上。选②：`contract-engine`新增了一个独立的轻量HTTP服务
（`internal/api/engine_server.go`，`EngineServer`，默认监听`:7002`，
`PERP_ENGINE_HTTP_ADDR`可配），目前只有`GET /depth?symbol=BTCUSDT&levels=20`一个接口，
跟`contract-api`那个对外业务API是完全独立的两个Gin实例/端口。

## 后续（这次没做，记录以便后续迭代）

- K线/行情聚合（基于`trades`表的原始成交记录）
- WebSocket推送：公开频道（深度/成交/K线/标记价格）+ 私有频道（订单/持仓/账户变化）
