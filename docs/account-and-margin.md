# 账户与保证金模型

## 全仓模式

账户级别共享一个资金池，不做逐仓隔离（逐仓是后续阶段的计划，见
[known-limitations.md](known-limitations.md)）。一个uid只有一行`accounts`记录：

| 字段            | 含义                                                                  |
|-----------------|-----------------------------------------------------------------------|
| `is_insured`    | 本轮是否投保，运营通过`POST /account/insured`单独设置                 |
| `round`         | 轮数，结束本轮时+1                                                    |
| `credit`        | 信用额度余额（用户买保险后的赔付），只能当开仓保证金用，不能转出/提现 |
| `available`     | 可用余额，**可能为负**                                                |
| `frozen_margin` | 挂单冻结的保证金（来自`available`的部分）                             |
| `frozen_credit` | 挂单冻结的保证金（来自`credit`的部分）                                |

`available`允许为负，这是全仓模式下的合法状态，不是bug——两种情况会让它变负：

1. 用持仓浮盈当买力开新仓（见下面"冻结保证金的四级路径"第3级）
2. 强平穿仓垫付之前，账户余额被打成负数

## 为什么`frozen_margin`/`frozen_credit`要分开记账

`credit`不能提现，`available`可以（通过合作方接口转出）。如果冻结保证金不区分来源、
笼统记一笔`frozen_margin`，"冻结→撤单退回"或"冻结→成交多退少补"这些环节退钱的时候，
没法知道这笔钱本该退回`available`还是`credit`——一旦退错，就等于让`credit`经过"冻结
再释放"这条渠道被洗成了可提现的`available`。所以`orders`表和`accounts`表都各自把
`frozen_margin`（来自available）和`frozen_credit`（来自credit）分开存，资金流转的每一步
（冻结、撤单释放、成交转正、多退少补）都按各自来源精确对应，不允许混用。

## 冻结保证金的四级路径（`FreezeMargin`）

开仓下单时，`requiredMargin`按四级路径依次尝试冻结，返回`FreezeResult{FromAvailable,
FromCredit}`告诉调用方这笔钱分别从两个来源各拿了多少：

1. **`available`够** → 全部从`available`冻结，`FromCredit`为0
2. **`available`不够，但`available + credit`够** → 缺口部分从`credit`冻结
   （`FreezeSpillToCredit`）
3. **前两级都不够，但`available + credit + 全部持仓未实现盈亏`够** → 币安式"持仓浮盈
   也能当买力开新仓"，强制冻结、允许`available`变负； **不动`credit`**——浮盈是不确定的，
   不该跟运营已经真金白银给出的保险赔付混在一起算作"已用掉"
4. **都不够** → 拒绝，返回`ErrInsufficientMargin`

任何一个持仓缺标记价格，未实现盈亏就按0算（不计入买力）——这是保守方向：算少了买力
顶多让开仓更容易被拒绝，不会让账户透支。

第3级强制冻结（`FreezeForceIntoNegative`）用的`available`/`credit`基准，是紧邻着这次
调用之前重新读的最新值，不是复用第2级判断时更早读到的快照——两次读之间账户可能被
并发改过。这一步同时改成CAS写法：`UPDATE ... WHERE available=? AND credit=?`带上
读到的旧值做守卫，读到的快照跟真正生效的这次扣减对不上时返回失败，让上层按余额不足
拒绝，而不是拿一个过期基准悄悄执行扣减。

## "先available后credit"的扣款顺序

已实现盈亏（`SettlePnl`）、手续费（`DeductFee`）这些"从账户里往外扣钱"的场景，统一走
`deductWithCreditFallback`：先扣`available`，`available`扣完了（含扣到负数为止都优先扣
`available`）再扣`credit`。这是刻意的业务取舍：`credit`是运营发放的保险赔付，尽量少被
真实亏损/手续费吃掉，能控制运营的赔付成本。盈利只进`available`，不会误加回`credit`。

## 资金流转的几个关键操作

| 操作                   | 说明                                                                                                                                                                                                                                               |
|------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `FreezeMargin`         | 挂单开仓冻结保证金，见上面四级路径                                                                                                                                                                                                                 |
| `UnfreezeMargin`       | 撤单/未成交部分释放冻结的保证金，`availableAmount`/`creditAmount`分别按来源精确归还                                                                                                                                                                |
| `DecreaseFrozenMargin` | 开仓成交：冻结保证金转移到仓位（全仓下`position_margin`只是记账用的名义值，不是真的锁住）。归还的不是原样冻结的钱，是按真实成交价"多退少补"之后的金额，分别退回`available`/`credit`，详见 [matching-and-settlement.md](matching-and-settlement.md) |
| `SettleToAvailable`    | 保证金原样归还到`available`——只用于明确知道钱该回`available`的场景，不用来结算亏损                                                                                                                                                                 |
| `SettleToCredit`       | 跟`SettleToAvailable`对称，归还冻结时属于`credit`的那一份                                                                                                                                                                                          |
| `SettlePnl`            | 已实现盈亏结算，可正可负：盈利只进`available`；亏损走"先available后credit"顺序                                                                                                                                                                     |
| `DeductFee`            | 手续费扣款，跟`SettlePnl`亏损分支同样的"先available后credit"顺序，无守卫——这笔手续费对应的成交已经真实发生，不能因为差一点钱扣不出来就不扣                                                                                                         |
| `GrantCredit`          | 运营发放/追加信用额度，同一轮内可以多次调用、直接累加                                                                                                                                                                                              |
| `SetInsured`           | 单独设置本轮是否投保，跟`GrantCredit`是两个独立动作，互不联动                                                                                                                                                                                      |
| `CloseRound`           | 结束本轮的资金收尾：`credit`清零（没用完的赔付额度不追讨，不退给运营）、`is_insured`重置、`round`+1                                                                                                                                                |

## 并发控制：原子条件UPDATE，不用悲观锁

账户余额的加减全部走"UPDATE ... WHERE 字段 >= 金额"这种数据库层面的原子条件更新。
涉及`credit`的多分支逻辑（比如`FreezeSpillToCredit`要同时判断`available`不够、
`available+credit`够、`available`可能已经是负数）用SQL的`GREATEST`/`LEAST`函数把
分支判断内嵌进一条`UPDATE`语句，保证整个多字段读-判断-写是原子的，不需要应用层加锁。
`UPDATE`受影响行数为0就代表"条件不满足"，调用方判断`RowsAffected() > 0`即可。这一层
保证的是单次`available`/`credit`加减操作本身的原子性，不覆盖"先读一批状态、再决定要
冻结多少"这种跨越多次读写的复合决策——开仓时"读现有仓位/挂单→算分档→冻结保证金"这段
复合临界区另外用分布式锁保护，见 [risk-limit-tiers.md](risk-limit-tiers.md#并发下单的原子性按uidsymbolside的分布式锁)。
其它没有额外加锁的极端并发场景（比如同一个uid同时触发强平结算与主动撤单）仍然只靠
这套原子UPDATE兜底，属于MVP阶段已知、接受的简化——详见 [known-limitations.md](known-limitations.md)。

## 账户查询视图（`AccountView`）

`GET /account/info`返回的是现算现填的视图，不是`accounts`表原始字段：

```
equity = available + credit + totalUnrealizedPnl
```

`credit`要算进权益，信用额度才能真正起到"扛住浮亏、推迟强平"的作用——强平联合判断
（见 [liquidation.md](liquidation.md)）用的也是这个口径。`totalUnrealizedPnl`是这个uid
名下全部持仓当前未实现盈亏之和，跟`FreezeMargin`第三级路径共用同一份计算逻辑。

## 轮次（round）生命周期

交易以"轮"为单位，一轮在client主动调用`POST /account/round/close`结束之前，一直是同一轮
（`round`字段不变）。结束本轮时（`EngineService.CloseRound`）：

1. 撤销这个uid名下全部symbol上还在排队的委托，按正常撤单逻辑释放冻结保证金
2. 撤销这个uid名下全部还没触发的条件单（止盈止损/条件开仓），按条件单自己的撤销逻辑
   释放冻结保证金——见 [conditional-orders.md](conditional-orders.md)。不撤的话，
   `credit`已经在下一步清零，之后如果条件单又触发，会用到不属于这一轮的额度
3. 对全部仍有持仓的symbol，按当前标记价立即强制平仓—— **不走**
   [liquidation.md](liquidation.md)里那套"挂保护价排队+超时兜底"机制：这是用户/合作方
   主动结束本轮，不是风险触发的强平，没必要走保护价滑点缓冲、也没必要等撮合
4. 调用`AccountService.CloseRound`清零`credit`、重置`is_insured`、`round`+1——清零前的
   `credit`值不是提前单独`SELECT`出来的，是`CloseRoundIfRound`用MySQL会话变量在同一条
   `UPDATE`里原子捕获后返回（写法跟`FreezeSpillToCredit`一致），再拿这个原子返回值记
   `TxRoundClose`审计流水。早期实现是先单独读一次`credit`、再执行清零的`UPDATE`，这两步
   之间如果有并发的`GrantCredit`把`credit`改大，`UPDATE`清零的是并发写入后的真实值，
   但审计流水记的是清零前更早读到的、偏小的旧值，两者会永久对不上（不影响账户实际余额，
   只影响审计流水这一个数字）——已用真实并发场景验证过：发放和清零的金额在
   `member_transactions`里精确对应

上面1-3步只要有任何一笔没成功（撤单失败、强平缺标记价格等），就不会执行第4步——
`AccountService.CloseRound`的前提是这个uid名下已经没有持仓/挂单/待触发条件单，不满足
就不清算资金状态，需要人工介入或等条件满足后（比如标记价格恢复）重新调用一次结束本轮
接口。

这套编排要摸`contract-engine`内存里的订单簿/撮合状态，`contract-api`看不到，所以
`POST /account/round/close`跟撤单接口一样，只是把事件发到Kafka（
`perpgo.round.close`）异步路由过去执行，HTTP响应只表示"请求已提交"，不代表已经处理完。
