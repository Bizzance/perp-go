# 账户与保证金模型

## 账户的创建

账户必须显式创建（`POST /account/create`，`uid`直接沿用合作方自己体系里的用户ID），其它接口遇到没创建过的
`uid`返回`account_not_found`，不会自动建——否则`uid`手误写错的充值会成功地充给一个没人认领的账户，
只读接口也会往库里塞垃圾账户。创建天然幂等：`AccountRepo.CreateIfAbsent`用`INSERT IGNORE`，撞了
`uid`唯一约束就是no-op，并发创建同一个`uid`只有一个得到`created=true`。

内部流程（成交结算、强平、资金费率、WS推送）操作的账户一定已经存在，内部的`GetOrCreate`保留不变；
面向合作方的入口（API层）一律先校验账户存在。

## 全仓模式

账户级别共享一个资金池，不做逐仓隔离（逐仓是后续阶段的计划，见
[known-limitations.md](known-limitations.md)）。一个uid只有一行`accounts`记录：

| 字段            | 含义                                                                  |
|-----------------|-----------------------------------------------------------------------|
| `is_insured`    | 本轮是否投保，运营通过`POST /account/insured`单独设置                 |
| `status`        | 账户状态`active`/`frozen`，运营通过`POST /account/status`设置，见下面"账户状态" |
| `round`         | 轮数，结束本轮时+1                                                    |
| `credit`        | 信用额度余额（用户买保险后的赔付），只能当开仓保证金用，不能转出/提现 |
| `available`     | 可用余额，**可能为负**                                                |
| `frozen_margin` | 挂单冻结的保证金（来自`available`的部分）                             |
| `frozen_credit` | 挂单冻结的保证金（来自`credit`的部分）                                |

`available`允许为负，这是全仓模式下的合法状态，不是bug——两种情况会让它变负：

1. 用持仓浮盈当买力开新仓（见下面"冻结保证金的四级路径"第3级）
2. 强平穿仓垫付之前，账户余额被打成负数

## 账户状态（冻结/解冻）

`accounts.status`有两个取值：`active`（默认）和`frozen`。**账户冻结跟挂单的"冻结保证金"（`frozen_margin`）是两回事**，
前者是运营对账户的管控，后者是资金记账。

冻结的语义是"禁止新增风险，不禁止降低风险"：

- **拒绝**：开仓委托、创建条件开仓单、修改杠杆（降杠杆要补冻结保证金，等于新增风险）
- **照常**：平仓委托、撤单、结束本轮、查询、运营的资金操作（加钱扣钱、发额度、设投保）
- **系统动作不受影响**：强平、ADL、资金费、成交结算。冻结不能让账户躲过强平，也不能让存量挂单的成交结算卡住

拦截分两层：

1. **API层**（`addOrder`/`addConditionalOrder`/`setLeverage`）：请求进来先看账户状态，冻结了直接返回`account_frozen`。
   `requestId`幂等重放检查在冻结检查之前，冻结前已经成功的请求重试仍然返回原结果
2. **引擎层兜底**（`EngineService.submitOrder`，撮合前）：开仓且不是强平单的委托，先查账户状态，冻结了就
   `cancelAndReleaseMargin`撤掉并退回保证金，不进订单簿。这一层堵的是API层拦不住的窗口：冻结前已经落库、还在Kafka
   里排队的开仓委托；条件单在冻结的同时被触发落地的开仓委托；重启恢复订单簿时重放的开仓委托（冻结清理没做完就重启）。
   查状态出错时返回错误、不往下撮合（失败关闭）

冻结接口本身还会顺带清理存量的开仓类挂单和条件开仓单（见 [api.md](api.md)），所以正常路径下引擎层兜底是用不上的，
它是为了让"清理漏掉某笔单"这类边角情况也不会让冻结账户的开仓单成交。创建条件开仓单落库后还会再看一次
账户状态，冻结接口刚好扫完、这笔才落库的话直接撤销并返回`account_frozen`。结束本轮（`round/close`）**不会**
重置账户状态，冻结的账户进入下一轮仍然是冻结的，只有运营显式解冻才恢复。每次状态变更（只记真正发生变化的）写一行
`account_status_history`：变更前后的状态、原因、操作的API密钥id、时间。

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

## 合作方调整余额（`POST /account/balance`）

`requestId`必填（幂等键，见 [idempotency.md](idempotency.md)），"写资金流水+改余额"在同一个数据库事务里
完成（`AccountRepo.ApplyFundOp`）。正数是入账，直接加到`available`；负数是扣款，只在`available >= 扣减额`
时才扣，用一条`UPDATE ... WHERE available >= ?`原子完成"检查余额+扣减"，不够返回`insufficient_balance`、
事务回滚（流水一起撤掉，`requestId`不被占用）。事务开头先`SELECT ... FOR UPDATE`锁账户行，同一个账户的资金
操作串行执行，避免并发的同一个`requestId`在唯一索引上死锁，见 [idempotency.md](idempotency.md)。`POST /account/credit`发额度同理，也是必填`requestId`+同一个事务。
早期实现负数分支也是无条件的`available = available + ?`，文档写着"扣的时候必须有足够`available`"
但代码根本没校验，扣款能把余额扣成负数。注意这里只看`available`，不看信用额度和浮盈——这个接口是
合作方的资金划转（模拟提现），信用额度不能转出，浮盈没有兑现，都不该被这个接口扣走。每次成功的调整
都会写一条`deposit`类型的资金流水（`GET /account/transactions`可查，带上`requestId`方便对账）。

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
（`round`字段不变）。调用时必须带上`round`——要结束的那一轮，它同时是幂等键：账户当前已经不在这一轮
就什么都不做，防止超时重试把下一轮又结束一次，见 [idempotency.md](idempotency.md)。结束本轮时
（`EngineService.CloseRound`，事件里带`round`，跟账户当前`round`不一致直接忽略）：

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
