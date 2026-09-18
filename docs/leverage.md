# 独立杠杆设置接口

## 范围：只支持修改已有仓位的杠杆

`POST /position/leverage`只对已经有仓位（`volume > 0`）的`uid+symbol+side`生效——这个
系统里杠杆本来就是`POST /order/add`下单时传的参数（见 [api.md](api.md)），不存在
"先设置这个合约的杠杆倍数、再下单沿用它"这种预先声明场景，也没有让这个接口在没有仓位时
凭空存一个"以后下单默认用这个杠杆"的设置——那会引入一套新的默认值解析逻辑，跟现有"杠杆
必须每次下单显式传"的既定设计（`docs/api.md`"必须是整数，显式传0或小数都会报错，不会被
当成'没传'或被截断"）相冲突，不在这次范围内。

## 校验：跟开仓同一套分档逻辑，用同一把锁防并发

用当前标记价格给仓位估值（`notional = position.Volume * markPrice`，缺标记价格直接拒绝），
查`TierFor`拿到这个名义价值对应的档位，新杠杆超过`tier.MaxLeverage`就拒绝——这跟
[risk-limit-tiers.md](risk-limit-tiers.md)"开仓杠杆校验"用的是同一套分档表，同一种拒绝
文案，只是名义价值的口径变成"这个仓位现在实际有多大"，不需要像开仓校验那样再叠加"排队中
的挂单"（修改杠杆不影响挂单，挂单该占多少保证金是它们自己下单时独立算好、冻结好的，不
因为持仓杠杆变了而重新计算）。

"读仓位现有状态→算新档位→冻结/释放差额"这段临界区，跟并发下单校验分档是同一类问题——
两个并发请求都读到同一份旧`PositionMargin`，可能都各自通过校验但合起来又绕开了限制。
复用`service.OrderLockKey(uid, symbol, side)`同一把分布式锁（见
[risk-limit-tiers.md](risk-limit-tiers.md#并发下单的原子性按uidsymbolside的分布式锁)），
跟并发下单挤在同一把锁里序列化——这本来就是同一个uid+symbol+side"名义价值/保证金占用"
临界区的两个不同入口，不需要为这个接口单独发明一套锁。

## 保证金调整：不能走`FreezeMargin`/`UnfreezeMargin`的frozen_margin路径

这是这个功能里最容易写错的地方。全仓模式下，**已经开仓的仓位的保证金不记在`accounts.
frozen_margin`/`frozen_credit`这两列里**——这两列只对应"还在排队等成交的挂单"，一笔委托
成交之后，`EngineService.settleOneFill`（`internal/service/settlement.go`）就会用
`DecreaseFrozenMargin`把对应金额从这两列转出，同时用`SettleToAvailable`/`SettleToCredit`
把"占用这个仓位的真实保证金"永久体现为`available`/`credit`余额的降低——`position.
PositionMargin`/`CreditMargin`只是记账用的名义值，用来在平仓时知道该退回多少、退给
`available`还是`credit`，不对应`frozen_margin`/`frozen_credit`里的任何一分钱（见
[account-and-margin.md](account-and-margin.md)"资金流转的几个关键操作"）。

第一版实现直接照抄了开仓下单校验那段代码，杠杆调低时调`FreezeMargin`、杠杆调高时调
`UnfreezeMargin`——**实测直接暴露问题**：对一个刚成交、`frozen_margin`已经是0的账户
调高杠杆（应该释放保证金），`UnfreezeMargin`去扣一个本来就是0的`frozen_margin`，
返回"冻结保证金不足"这个完全文不对题的假错误，操作直接失败。

正确的做法是分别复用两条既有链路各自的"直接改`available`/`credit`，不碰`frozen_margin`/
`frozen_credit`"的那一半：

- **杠杆调高（需要的保证金变少）**：直接照抄`ApplyCloseFill`释放持仓保证金那一段——按
  仓位现有`CreditMargin/PositionMargin`的比例把差额拆成`available`/`credit`两部分，
  分别调`SettleToAvailable`/`SettleToCredit`直接退，不经过`frozen_margin`。
- **杠杆调低（需要的保证金变多）**：不能只加"直接扣`available`/`credit`"这么简单——
  还需要`FreezeMargin`那套四级路径（`available`够不够 → 不够就查`available+credit`
  够不够 → 还不够就查加上浮盈够不够 → 都不够就拒绝）来判断这笔新增的保证金到底该从哪个
  来源出、以及要不要拒绝。做法是先调`FreezeMargin`（复用完整的四级判断逻辑，副作用是
  会把这笔钱记进`frozen_margin`/`frozen_credit`），紧接着立刻调`DecreaseFrozenMargin`
  把它从这两列转出——两步连续执行，净效果是`available`/`credit`被永久扣掉差额、
  `frozen_margin`/`frozen_credit`不变，效果上完全等价于`SubmitOrder`"先冻结、成交时
  转移到仓位记账"这套流程，只是这里没有异步撮合的中间状态，写成一次请求里连续完成的
  两步。

## 实测验证

用真实成交开出一个仓位，覆盖了这几种场景，均符合预期：

- 纯`available`来源的仓位调高/调低杠杆：保证金按新杠杆重算，`available`增减金额精确
  对应差额。
- `available`+`credit`混合来源的仓位（先充少量`available`、发一笔`credit`，开仓时
  `available`不够、自动spill到`credit`）调高杠杆：差额按仓位当前`CreditMargin/
  PositionMargin`的比例精确拆分，分别退回两个来源，不会把`credit`那一份错误地退到
  `available`（那样等于把不能提现的信用额度洗成了可提现的余额）。
- 同一个仓位反向操作（调低杠杆后又调回去）：拿到的最终状态跟没做过这次往返完全一致，
  `frozen_margin`/`frozen_credit`全程保持0，没有意外残留。
- 拒绝路径：新杠杆超过分档允许的最大值、没有对应方向的持仓、`available+credit`不够
  覆盖调低杠杆后的新增保证金——三种情况都正确拒绝且不产生任何副作用（拒绝前后仓位/
  账户状态完全不变，用真实请求逐条验证过）。

## 明确没做的

- 不支持"没有仓位时预先声明杠杆"，见上面"范围"一节。
- 不影响这个方向上正在排队的挂单——挂单的保证金/杠杆是它们各自下单时独立冻结好的，修改
  持仓杠杆不会回头重新校验/调整它们。
- 没有WS推送：这个接口完全在`contract-api`内部同步完成，不经过Kafka/`contract-engine`，
  跟`adjustBalance`/`grantCredit`这些`contract-api`自己的资金类接口一样——WS私有频道的
  推送只从`contract-engine`那边发出（见 [websocket.md](websocket.md)），客户端拿到的
  HTTP响应本身就是最新状态。
