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

| 字段            | 含义                                                                            |
|-----------------|---------------------------------------------------------------------------------|
| `is_insured`    | 本轮是否投保，运营通过`POST /account/insured`单独设置                           |
| `status`        | 账户状态`active`/`frozen`，运营通过`POST /account/status`设置，见下面"账户状态" |
| `round`         | 轮数，结束本轮时+1                                                              |
| `credit`        | 信用额度总额（用户买保险后的赔付），只能当开仓保证金用，不能转出/提现           |
| `balance`       | 余额总额，**可能为负**。不随下单/开仓锁定而变化，只有充值/提现/已实现盈亏/手续费这些真实改变总资产的操作才会改它 |
| `frozen_margin` | 来自`balance`的锁定额——挂单占用+持仓占用的合计，可用余额=`balance - frozen_margin` |
| `frozen_credit` | 来自`credit`的锁定额，跟`frozen_margin`对称，可用信用额度=`credit - frozen_credit` |

`balance`允许为负，这是全仓模式下的合法状态，不是bug——两种情况会让它变负：

1. 用持仓浮盈当买力开新仓（见下面"冻结保证金的四级路径"第3级）
2. 强平穿仓垫付之前，账户余额被打成负数

`frozen_margin`/`frozen_credit`覆盖挂单和持仓两种锁定：下单时先按`FreezeMargin`的四级路径
把保证金锁进这两列；成交之后，这笔钱不是"退回余额、再记到仓位上"，而是留在原地——只是把
锁定的估算值从"下单时的保守估计"调整成"按真实成交价算出的真实保证金"（多退少补，见
[matching-and-settlement.md](matching-and-settlement.md)），继续锁着直到这笔仓位平仓才
释放。`positions`表的`position_margin`/`credit_margin`是同一份锁定按仓位维度的分账（用于
按symbol算强平价、展示给合作方），不是另一笔独立的钱。

## 账户状态（冻结/解冻）

`accounts.status`有两个取值：`active`（默认）和`frozen`。 **账户冻结跟挂单的"冻结保证金"（`frozen_margin`）是两回事**，
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
账户状态，冻结接口刚好扫完、这笔才落库的话直接撤销并返回`account_frozen`。结束本轮（`round/close`） **不会**
重置账户状态，冻结的账户进入下一轮仍然是冻结的，只有运营显式解冻才恢复。每次状态变更（只记真正发生变化的）写一行
`account_status_history`：变更前后的状态、原因、操作的API密钥id、时间。

## 为什么`frozen_margin`/`frozen_credit`要分开记账

`credit`不能提现，`balance`可以（通过合作方接口转出）。如果锁定的保证金不区分来源、
笼统记一笔，"撤单解锁"或"成交多退少补"这些环节的时候，没法知道这笔钱本该解到`balance`
那一侧还是`credit`那一侧——一旦弄混，就等于让`credit`经过"锁定再解锁"这条渠道被洗成了
可提现的`balance`。所以`orders`表和`accounts`表都各自把`frozen_margin`（来自balance）
和`frozen_credit`（来自credit）分开存，资金流转的每一步（锁定、撤单解锁、成交多退少补、
平仓解锁）都按各自来源精确对应，不允许混用。

## 冻结保证金的四级路径（`FreezeMargin`）

开仓下单时，`requiredMargin`先过一道 **买力预检**，再按四级路径依次尝试锁定，返回`FreezeResult{FromAvailable,
FromCredit}`告诉调用方这笔钱分别从两个来源各拿了多少。下面的"自由余额"/"自由信用额度"
指`balance - frozen_margin`/`credit - frozen_credit`——`balance`/`credit`本身是不随锁定
变化的总额，见上面"全仓模式"一节。

**买力预检：账户有浮亏时，买力是自由余额+自由信用额度减掉浮亏，不够就直接拒绝**，不管自由余额本身够不够。
币安的可用余额 = 钱包余额 − 初始保证金 +
未实现盈亏（[币安说明](https://www.binance.com/en/blog/futures/what-is-the-available-balance-margin-balance-and-total-balance-on-binance-futures-457299340443288694)），
浮亏直接减少可用余额；OKX的可用保证金也是从计入未实现盈亏的调整后权益算起。早期实现前两级只看余额、
不扣浮亏，账户浮亏累累甚至已经满足强平条件，只要自由余额还是正数就能继续锁定保证金开新仓（探针复现：
权益21.75 <= 维持保证金22.1、浮亏975、自由余额346.75时能锁定340）。按这个口径，账户进入强平条件
（权益 <= 维持保证金 < 初始保证金）时买力必然为负，新开仓自然被拒，所以不需要单独加"强平期间拒绝新单"的规则，
代码里也没有按仓位`liquidating`状态拦截。下单、创建条件单、降杠杆补保证金都走这个检查。
浮盈不在预检里放宽，只在第3级才能当买力。

预检之后的四级路径：

1. **自由余额够** → 全部从`balance`锁定（只加`frozen_margin`，不动`balance`），`FromCredit`为0
2. **自由余额不够，但自由余额+自由信用额度够** → 缺口部分从`credit`锁定
   （`FreezeSpillToCredit`）
3. **前两级都不够，但自由余额+自由信用额度+全部持仓未实现盈亏够** → 币安式"持仓浮盈
   也能当买力开新仓"，强制锁定、允许自由余额变负； **不动`credit`**——浮盈是不确定的，
   不该跟运营已经真金白银给出的保险赔付混在一起算作"已用掉"
4. **都不够** → 拒绝，返回`ErrInsufficientMargin`

任何一个持仓缺标记价格，未实现盈亏就按0算（不计入买力）——这是保守方向：算少了买力
顶多让开仓更容易被拒绝，不会让账户透支。

第3级强制锁定（`FreezeForceIntoNegative`）用的`balance`/`credit`/`frozen_margin`/
`frozen_credit`基准，是紧邻着这次调用之前重新读的最新快照，不是复用第2级判断时更早读到的
快照——两次读之间账户可能被并发改过。这一步是CAS写法：`UPDATE ... WHERE balance=? AND
credit=? AND frozen_margin=? AND frozen_credit=?`带上读到的旧值做守卫，读到的快照跟真正
生效的这次锁定对不上时返回失败，让上层按余额不足拒绝，而不是拿一个过期基准悄悄执行锁定。

## 合作方调整余额（`POST /account/balance`）

`requestId`必填（幂等键，见 [idempotency.md](idempotency.md)），"写资金流水+改余额"在同一个数据库事务里
完成（`AccountRepo.ApplyFundOp`）。正数是入账，直接加到`balance`；负数是扣款，只在自由余额
（`balance - frozen_margin`）够扣时才扣，用一条`UPDATE ... WHERE balance - frozen_margin >= ?`
原子完成"检查余额+扣减"，不够返回`insufficient_balance`、
事务回滚（流水一起撤掉，`requestId`不被占用）。事务开头先`SELECT ... FOR UPDATE`锁账户行，同一个账户的资金
操作串行执行，避免并发的同一个`requestId`在唯一索引上死锁，见 [idempotency.md](idempotency.md)。`POST /account/credit`
发额度同理，也是必填`requestId`+同一个事务。
注意这里的扣款检查只看自由余额，不看浮盈——这个接口是合作方的资金划转（模拟提现），锁在
挂单/仓位里的钱和浮盈都没有兑现，都不该被这个接口扣走。每次成功的调整都会写一条`deposit`
类型的资金流水（`GET /account/transactions`可查，带上`requestId`方便对账）。

## "先balance后credit"的扣款顺序

已实现盈亏（`SettlePnl`）、手续费（`DeductFee`）这些"从账户里往外扣钱"的场景，统一走
`deductWithCreditFallback`：先扣`balance`，`balance`扣完了（含扣到负数为止都优先扣
`balance`）再扣`credit`。这是刻意的业务取舍：`credit`是运营发放的保险赔付，尽量少被
真实亏损/手续费吃掉，能控制运营的赔付成本。盈利只进`balance`，不会误加回`credit`。

## 资金流转的几个关键操作

| 操作                   | 说明                                                                                                                                                                                                                                               |
|------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `FreezeMargin`         | 挂单/开仓锁定保证金，见上面四级路径                                                                                                                                                                                                                |
| `UnfreezeMargin`       | 解锁`frozen_margin`/`frozen_credit`，不touch`balance`/`credit`——撤单、条件单撤销、平仓解锁、降杠杆释放共用，`availableAmount`/`creditAmount`按来源精确释放。settlement.go开仓成交时也用它把锁定从"下单时的保守估算"调整到"按真实成交价算出的真实保证金"，这个delta偶尔是负数(需要补锁而不是释放)，同一个函数处理两种方向 |
| `SettleToBalance`      | 直接改`balance`——只用于真实改变总资产的场景（资金费、ADL划转保险基金），不用来结算已实现盈亏                                                                                                                                                       |
| `SettleToCredit`       | 直接改`credit`，无守卫——只用于强平穿仓垫付/维持保证金缓冲清算这类"把自由信用额度清零"的场景                                                                                                                                                       |
| `SettlePnl`            | 已实现盈亏结算，可正可负：盈利只进`balance`；亏损走"先balance后credit"顺序                                                                                                                                                                         |
| `DeductFee`            | 手续费扣款，跟`SettlePnl`亏损分支同样的"先balance后credit"顺序，无守卫——这笔手续费对应的成交已经真实发生，不能因为差一点钱扣不出来就不扣                                                                                                           |
| `GrantCredit`          | 运营发放/追加信用额度，同一轮内可以多次调用、直接累加                                                                                                                                                                                              |
| `SetInsured`           | 单独设置本轮是否投保，跟`GrantCredit`是两个独立动作，互不联动                                                                                                                                                                                      |
| `CloseRound`           | 结束本轮的资金收尾：`credit`清零（没用完的赔付额度不追讨，不退给运营）、`is_insured`重置、`round`+1                                                                                                                                                |

## 并发控制：原子条件UPDATE，不用悲观锁

账户余额的加减全部走"UPDATE ... WHERE 字段 >= 金额"这种数据库层面的原子条件更新。
涉及`credit`的多分支逻辑（比如`FreezeSpillToCredit`要同时判断自由余额不够、
自由余额+自由信用额度够、自由余额可能已经是负数）用SQL的`GREATEST`/`LEAST`函数把
分支判断内嵌进一条`UPDATE`语句，保证整个多字段读-判断-写是原子的，不需要应用层加锁。
`UPDATE`受影响行数为0就代表"条件不满足"，调用方判断`RowsAffected() > 0`即可——这也是
数据库连接要开`clientFoundRows`的原因：结算时`UnfreezeMargin`传的delta经常刚好是0
（下单时的保守估算跟真实成交价算出来的保证金完全相等），这种情况下`SET`子句没有让任何
列的值发生变化，MySQL默认的"受影响行数=值变化的行数"语义会把这个完全正常的no-op误判成
"没匹配到、乐观锁冲突"，`clientFoundRows`让"匹配到WHERE条件的行数"才是`RowsAffected()`
的含义，这才是这里各处判断逻辑真正想要的语义（见`internal/db/db.go`）。这一层
保证的是单次`balance`/`credit`加减操作本身的原子性，不覆盖"先读一批状态、再决定要
锁定多少"这种跨越多次读写的复合决策——开仓时"读现有仓位/挂单→算分档→锁定保证金"这段
复合临界区另外用分布式锁保护，见 [risk-limit-tiers.md](risk-limit-tiers.md#并发下单的原子性按uidsymbolside的分布式锁)。
其它没有额外加锁的极端并发场景（比如同一个uid同时触发强平结算与主动撤单）仍然只靠
这套原子UPDATE兜底，属于MVP阶段已知、接受的简化——详见 [known-limitations.md](known-limitations.md)。

## 账户查询视图（`AccountView`）

`GET /account/info`返回的是现算现填的视图，不是`accounts`表原始字段：

```
equity = balance + credit + totalUnrealizedPnl
```

`credit`要算进权益，信用额度才能真正起到"扛住浮亏、推迟强平"的作用——强平联合判断
（见 [liquidation.md](liquidation.md)）用的也是这个口径。`balance`是不随锁定变化的总额，
本身已经包含了挂单/仓位占用的那部分钱，不需要再单独加`frozenMargin`/`positionMargin`——
开仓/挂单只是让`frozenMargin`变多，`balance`不变，价格不动权益只会被手续费拉低。
`positionMargin`（这个uid全部持仓占用的保证金之和，含来自信用额度的部分）只是查询视图上
的展示字段，不参与权益计算。 **买力**（开仓够不够钱，见"冻结保证金的四级路径"）
是另一个口径，只看自由余额`balance - frozenMargin`（加自由信用额度和浮盈），不含已经
占用的保证金。`totalUnrealizedPnl`是这个uid名下全部持仓当前未实现盈亏之和，跟
`FreezeMargin`第三级路径共用同一份计算逻辑。

## 轮次（round）生命周期

交易以"轮"为单位，一轮在client主动调用`POST /account/round/close`结束之前，一直是同一轮
（`round`字段不变）。调用时必须带上`round`——要结束的那一轮，它同时是幂等键：账户当前已经不在这一轮
就什么都不做，防止超时重试把下一轮又结束一次，见 [idempotency.md](idempotency.md)。结束本轮时
（`EngineService.CloseRound`，事件里带`round`，跟账户当前`round`不一致直接忽略）：

1. 撤销这个uid名下全部symbol上还在排队的委托，按正常撤单逻辑释放冻结保证金
2. 撤销这个uid名下全部还没触发的条件单（止盈止损/条件开仓），按条件单自己的撤销逻辑
   释放冻结保证金——见 [conditional-orders.md](conditional-orders.md)。不撤的话，
   `balance`/`credit`已经在下一步清零，之后如果条件单又触发，会用到不属于这一轮的资金
3. 对全部仍有持仓的symbol，按当前标记价立即强制平仓—— **不走**
   [liquidation.md](liquidation.md)里那套"挂保护价排队+超时兜底"机制：这是用户/合作方
   主动结束本轮，不是风险触发的强平，没必要走保护价滑点缓冲、也没必要等撮合。这一步的
   平仓结算完全走正常的`SettlementService.SettleFill`，不涉及强平那套"穿仓由保险基金
   垫付/正数结余留给用户"的逻辑——结束本轮时不会再有仓位，谈不上穿仓
4. 调用`AccountService.CloseRound`清零`balance`、`credit`、重置`is_insured`、`round`+1——
   每一轮都是完全独立的资金周期，不跨轮结转：`balance`清零对应"这笔钱该退给用户了"，退款
   本身是合作方在系统外处理的业务，我们这边只负责把账清零；`credit`清零是回收没用完的
   赔付额度，不追讨。清零前的`balance`/`credit`值不是提前单独`SELECT`出来的，是
   `CloseRoundIfRound`用MySQL会话变量在同一条`UPDATE`里原子捕获后返回（写法跟
   `FreezeSpillToCredit`一致），再拿这两个原子返回值各记一条`TxRoundClose`审计流水。早期
   实现是先单独读一次`credit`、再执行清零的`UPDATE`，这两步之间如果有并发的`GrantCredit`
   把`credit`改大，`UPDATE`清零的是并发写入后的真实值，但审计流水记的是清零前更早读到的、
   偏小的旧值，两者会永久对不上（不影响账户实际余额，只影响审计流水这一个数字）——已用
   真实并发场景验证过：发放和清零的金额在`member_transactions`里精确对应

上面1-3步只要有任何一笔没成功（撤单失败、强平缺标记价格等），就不会执行第4步——
`AccountService.CloseRound`的前提是这个uid名下已经没有持仓/挂单/待触发条件单，不满足
就不清算资金状态，需要人工介入或等条件满足后（比如标记价格恢复）重新调用一次结束本轮
接口。

这套编排要摸`contract-engine`内存里的订单簿/撮合状态，`contract-api`看不到，所以
`POST /account/round/close`跟撤单接口一样，只是把事件发到Kafka（
`perpgo.round.close`）异步路由过去执行，HTTP响应只表示"请求已提交"，不代表已经处理完。
