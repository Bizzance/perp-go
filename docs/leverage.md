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

## 保证金调整：直接复用`FreezeMargin`/`UnfreezeMargin`

全仓模式下，`accounts.frozen_margin`/`frozen_credit`这两列覆盖挂单**和**持仓两种锁定：
一笔委托成交之后，`EngineService.settleOneFill`（`internal/service/settlement.go`）不会
把这笔钱从`frozen_margin`/`frozen_credit`转出、退回`balance`/`credit`，而是把锁定量从
"下单时的保守估算"调整到"按真实成交价算出的真实保证金"，继续锁着直到平仓——`position.
PositionMargin`/`CreditMargin`是这份锁定按仓位维度的分账（用于查询展示、按symbol算强平
价），跟`accounts.frozen_margin`/`frozen_credit`里的锁定是同一份钱，不是另外记的独立
金额（见 [account-and-margin.md](account-and-margin.md)）。

这个设计下，改杠杆需要的保证金调整就是单纯的"锁多一点"或"解锁一点"，跟下单冻结、撤单
解锁走的是完全相同的机制，不需要为这个接口单独发明一套逻辑：

- **杠杆调高（需要的保证金变少）**：按仓位现有`CreditMargin/PositionMargin`的比例把
  差额拆成两部分，直接调`UnfreezeMargin`解锁——跟`ApplyCloseFill`释放持仓保证金时用的
  是同一个函数。
- **杠杆调低（需要的保证金变多）**：直接调`FreezeMargin`（复用完整的四级路径：自由余额
  够不够 → 不够就查自由余额+自由信用额度够不够 → 还不够就查加上浮盈够不够 → 都不够就
  拒绝），锁定的钱就留在`frozen_margin`/`frozen_credit`里，不需要像旧版本那样"冻结完
  立刻转出"——因为这里的`frozen_margin`本来就是仓位要一直锁着的地方，不是临时中转站。

## 实测验证

用真实成交开出一个仓位，覆盖了这几种场景，均符合预期：

- 纯`balance`来源的仓位调高/调低杠杆：`frozen_margin`按新杠杆重算，增减金额精确
  对应差额，`balance`全程不变。
- `balance`+`credit`混合来源的仓位（先充少量`balance`、发一笔`credit`，开仓时
  `balance`不够、自动spill到`credit`）调高杠杆：差额按仓位当前`CreditMargin/
  PositionMargin`的比例精确拆分，分别解锁`frozen_margin`/`frozen_credit`，不会把
  `credit`那一份错误地解到`frozen_margin`（那样等于把不能提现的信用额度洗成了balance
  一侧的锁定）。
- 同一个仓位反向操作（调低杠杆后又调回去）：拿到的最终状态跟没做过这次往返完全一致，
  没有意外残留。
- 拒绝路径：新杠杆超过分档允许的最大值、没有对应方向的持仓、自由余额+自由信用额度
  不够覆盖调低杠杆后的新增保证金——三种情况都正确拒绝且不产生任何副作用（拒绝前后仓位/
  账户状态完全不变，用真实请求逐条验证过）。

## 明确没做的

- 不支持"没有仓位时预先声明杠杆"，见上面"范围"一节。
- 不影响这个方向上正在排队的挂单——挂单的保证金/杠杆是它们各自下单时独立冻结好的，修改
  持仓杠杆不会回头重新校验/调整它们。
- 没有WS推送：这个接口完全在`contract-api`内部同步完成，不经过Kafka/`contract-engine`，
  跟`adjustBalance`/`grantCredit`这些`contract-api`自己的资金类接口一样——WS私有频道的
  推送只从`contract-engine`那边发出（见 [websocket.md](websocket.md)），客户端拿到的
  HTTP响应本身就是最新状态。
