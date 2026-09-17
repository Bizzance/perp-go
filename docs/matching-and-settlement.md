# 撮合与结算

## 订单簿

`internal/matching`包实现每个symbol一个订单簿，价格-时间优先：

- LIMIT单未完全成交的剩余部分挂在簿子上等下一笔对手单
- MARKET单不挂簿，吃多少算多少，剩余直接释放
- 撮合只关心"买/卖"方向，不关心`side`/`action`这两个业务维度：
    - `LONG+OPEN`、`SHORT+CLOSE` 都是买方（要拉高价格才能成交）
    - `SHORT+OPEN`、`LONG+CLOSE` 都是卖方
    - 见`matching.DirectionOf`
- 同一个uid的买卖单不会互相撮合（自成交保护），撮合过程中撞上自己的挂单会把那笔挂单摘掉
  当撤单处理，taker继续往下吃非自己的流动性

数据结构（价格档位数组二分定位+组内FIFO链表+orderID索引map，O(1)撤单）、自成交保护的
具体行为、深度查询接口，详见 [order-book.md](order-book.md)。

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

`orderNotionalPrice`和上面价格保护带用的是**同一个**`referencePrice`（标记价格优先，
缺失退回指数价格），不是分别各自判断——两处防的是同一类问题，用不一致的参考价判断口径
会留出"有指数价但从没成交过"这个中间状态的防护缺口。

## 成交结算（`SettlementService.SettleFill`）

一笔成交对某一方（maker或taker）的影响，分两个分支：

**开仓分支（`action == OPEN`）**：

1. 按订单总冻结保证金比例，算出这一笔成交对应释放多少冻结保证金（`filledMargin`，
   基于下单时的保守估计，`filledFromAvailable`/`filledFromCredit`两部分分开算——见
   [account-and-margin.md](account-and-margin.md)为什么冻结要分来源记账）
2. 按真实成交价算这一笔成交本该占用多少保证金（`properMargin`，真实的风险敞口，决定
   强平价该在哪，也是这笔仓位真正该从`available`/`credit`净扣掉的钱），按
   `filledFromAvailable`/`filledFromCredit`的比例拆成`properFromAvailable`/
   `properFromCredit`两部分
3. 加权平均开仓价、累加仓位（`ApplyOpenFill`），记账用的`position_margin`是`properMargin`，
   `credit_margin`是其中`properFromCredit`的部分——一个仓位可能由多笔成交累积而成，
   `credit_margin`要按金额持续累加，不能只记最后一笔成交的来源比例
4. **多退少补**：`available`/`credit`不是把`filledFromAvailable`/`filledFromCredit`
   原样退回，而是分别退`filledFromAvailable - properFromAvailable`/
   `filledFromCredit - properFromCredit`这两个差额——冻结时按保守估计多冻了
   （`filledMargin > properMargin`，比如SHORT+OPEN报了吃单价、真实按对手更高的价格
   成交），就把多冻的部分还给各自来源；冻结不够（较少见，比如挂单挂了很久、真实成交时
   标记价格已经比下单时更高），就从各自来源里再扣差额。这样`available`/`credit`最终
   净扣掉的正好是`properFromAvailable`/`properFromCredit`，不会把该占用的保证金错误地
   留在`available`/`credit`里、变相凭空多出一部分可用余额——这正是[之前那个用极端报价的
   吃单几乎不冻结保证金就开出大仓位的漏洞](known-limitations.md)的根治方式

**平仓分支（`action == CLOSE`）**：

1. 按加权平均开仓价算已实现盈亏，按比例释放`position_margin`/`credit_margin`
   （`ApplyCloseFill`返回`releasedMargin`/`releasedCreditMargin`）
2. **释放的仓位保证金必须还给账户**：`releasedMargin - releasedCreditMargin`还给
   `available`，`releasedCreditMargin`还给`credit`——这部分钱在开仓时(上面第4步)已经
   永久从`available`/`credit`里扣掉、记进了`position_margin`/`credit_margin`，平仓时
   不还回来的话，这笔钱会凭空消失（即使是一笔盈亏为0的平仓，账户也会永久损失一笔仓位
   保证金）。这一步不能走`SettlePnl`的"先available后credit"亏损兜底顺序——那套顺序是
   给真实亏损设计的，归还本金必须精确按来源退回，不能被那套逻辑误吞
3. 已实现盈亏走`SettlePnl`结算，可正可负：盈利只进`available`，亏损走"先available后
   credit"顺序（见 [account-and-margin.md](account-and-margin.md)）

两个分支之后都会扣手续费（maker/taker两档费率，MVP不区分强平单的清算费率），手续费扣款
也走"先available后credit"顺序。

## 撮合结果的名义价值不是固定的

一个仓位的名义价值 = `volume × 当前标记价格`，会随行情波动变化，所以杠杆上限判断、维持
保证金计算都是 **动态**的——同一个仓位，标记价格涨了名义价值变大，可能从低档滑到高档，
维持保证金要求也跟着变。见 [risk-limit-tiers.md](risk-limit-tiers.md)。
