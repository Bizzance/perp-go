# 撮合与结算

## 订单簿

`internal/matching`包实现每个symbol一个订单簿，价格-时间优先：

- LIMIT单未完全成交的剩余部分挂在簿子上等下一笔对手单
- MARKET单不挂簿，吃多少算多少，剩余直接释放
- 撮合只关心"买/卖"方向，不关心`side`/`action`这两个业务维度：
    - `LONG+OPEN`、`SHORT+CLOSE` 都是买方（要拉高价格才能成交）
    - `SHORT+OPEN`、`LONG+CLOSE` 都是卖方
    - 见`matching.DirectionOf`

订单簿用简单切片+每次插入排序实现，MVP阶段成交量级不需要更高级的数据结构（跳表/红黑树）。

## 下单校验链（`POST /order/add`）

按顺序做以下校验（任何一步不通过就直接拒绝，不冻结保证金）：

1. `leverage`基本合法性 + 不超过`maxSaneLeverage`（1000，纯粹是防止`uint32`转换溢出的
   兜底，不是真正的业务上限）
2. 合约是否存在、是否启用
3. LIMIT单：价格是否符合`price_tick`最小变动单位
4. **价格保护带**（只对开仓单生效）：委托价格不能偏离参考价（标记价格，缺失则退回指数
   价格）超过`price_protection_ratio`，防止胖手指和"吃单价"钻空子——详见下面单独一节
5. 数量是否满足`min_volume`/`max_volume`/`volume_step`
6. 开仓单： **保证金分档杠杆校验**，见 [risk-limit-tiers.md](risk-limit-tiers.md)
7. 开仓单：`FreezeMargin`

全部通过后落库（`status=NEW`），发Kafka事件给engine处理撮合。

## 价格保护带

限价单的价格必须落在`参考价 × (1 ± price_protection_ratio)`区间内。这么做防两类问题：

1. **胖手指**：用户瞎填价格
2. **吃单价钻分档空子**：一笔报价远离市价的"吃单"实际会按盘口对手的真实价格成交，不是
   按填的这个价格——分档校验、保证金冻结用委托价格算的话会严重低估真实风险，见下面
   "冻结保证金的保守估计"和`SettlementService.SettleFill`的多退少补机制

只对 **开仓**单生效，平仓单不受限制——理由跟下面杠杆校验对平仓单的处理一样：平仓不创建
新仓位/新风险，不需要这层保护，反而如果限制平仓单，行情剧烈波动、标记价格滞后时会把用户
想平仓离场的委托也挡在外面。

参考价优先用标记价格，标记价格不存在（这个symbol从没成交过）就退回用指数价格；两个都
没有（全新symbol、也没人喂过指数价）就没有参考基准，放行不校验——这是唯一防不住的缺口。

## 冻结保证金的保守估计

`contract-api`下单时同步冻结保证金，但订单实际成交价由`contract-engine`那边的订单簿
撮合决定——不是按委托价格`price`本身，而是按`orderNotionalPrice`这个保守参考价来算
`requiredMargin`：

- **SHORT+OPEN**：`max(委托价, 标记价格)`。SHORT+OPEN在撮合引擎里是卖方向，报价远低于
  市价属于"吃单价"，会立刻按盘口对手的真实（更高）价格成交，用委托价格算冻结金额会
  严重低估
- **LONG+OPEN**：直接用委托价格。远低于市价的价格是完全合法的被动挂单（买跌），只会
  按这个低价成交，不需要也不应该套用标记价格

这个保守估计只是"先冻多一点"，真实该占用多少保证金要等成交后才知道，见下面的多退少补。

## 成交结算（`SettlementService.SettleFill`）

一笔成交对某一方（maker或taker）的影响，分两个分支：

**开仓分支（`action == OPEN`）**：

1. 按订单总冻结保证金比例，算出这一笔成交对应释放多少冻结保证金（`filledMargin`，
   基于下单时的保守估计）
2. 按真实成交价算这一笔成交本该占用多少保证金（`properMargin`，真实的风险敞口，决定
   强平价该在哪，也是这笔仓位真正该从`available`净扣掉的钱）
3. 加权平均开仓价、累加仓位（`ApplyOpenFill`），记账用的`position_margin`是`properMargin`
4. **多退少补**：`available`不是把`filledMargin`原样退回，而是退`filledMargin - properMargin`
   这个差额——冻结时按保守估计多冻了（`filledMargin > properMargin`，比如SHORT+OPEN报了
   吃单价、真实按对手更高的价格成交），就把多冻的部分还给用户；冻结不够（较少见，比如
   挂单挂了很久、真实成交时标记价格已经比下单时更高），就从`available`里再扣差额。这样
   `available`最终净扣掉的正好是`properMargin`，不会把该占用的保证金错误地留在
   `available`里、变相凭空多出一部分可用余额——这正是[之前那个用极端报价的吃单几乎不
   冻结保证金就开出大仓位的漏洞](known-limitations.md)的根治方式

**平仓分支（`action == CLOSE`）**：

1. 按加权平均开仓价算已实现盈亏，释放对应比例的仓位保证金（`ApplyCloseFill`）
2. 已实现盈亏直接结算到`available`，可正可负

两个分支之后都会扣手续费（maker/taker两档费率，MVP不区分强平单的清算费率）。

## 撮合结果的名义价值不是固定的

一个仓位的名义价值 = `volume × 当前标记价格`，会随行情波动变化，所以杠杆上限判断、维持
保证金计算都是 **动态**的——同一个仓位，标记价格涨了名义价值变大，可能从低档滑到高档，
维持保证金要求也跟着变。见 [risk-limit-tiers.md](risk-limit-tiers.md)。
