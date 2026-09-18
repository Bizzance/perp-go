# 条件单（止盈止损/条件开仓）

## 设计思路

条件单不是普通委托的变体，而是"记着一个触发条件，条件满足了才转成一笔真正的普通委托"。
触发前完全不进撮合引擎的内存订单簿，只是`conditional_orders`表里的一行；触发后按
**同一个`order_id`**落地到`orders`表，走跟普通委托完全相同的撮合+结算路径（包括
`SettlementService.SettleFill`的多退少补机制）。客户端不需要维护"条件单id"和"委托id"
两套编号。

这个设计避免了给撮合引擎的内存订单簿增加"暂不参与撮合的委托"这种新状态，触发之后
条件单的生命周期就完全并入普通委托，后续查询、撤销都用现成的`/order/*`接口。

## 触发方向：显式`triggerDirection`，不是"止盈/止损"标签

条件单只有`triggerPrice`（触发价）+`triggerDirection`（`gte`=标记价格涨到/超过触发价
才触发，`lte`=跌到/低于触发价才触发）两个字段，不区分"这是止盈单还是止损单"。止盈止损
只是这个通用机制的两种常见用法：

- 给多头仓位设止盈：`side=long, action=close, triggerDirection=gte`（涨到目标价止盈离场）
- 给多头仓位设止损：`side=long, action=close, triggerDirection=lte`（跌到止损价离场）
- 给空头仓位反过来

这样实现比"系统自动判断这是止盈还是止损"更简单、边界情况更少，调用方自己决定往哪个方向
触发即可。同样的机制也能用来做条件开仓（突破单/回调入场），不局限于平仓场景。

## 创建（`POST /order/conditional/add`）

校验链基本复用`addOrder`（枚举校验、价格步长、数量范围、整数杠杆，`validateSideAction`/
`resolveOrderType`/`resolveLeverage`这几个校验规则两边直接共用同一份实现，避免各写一份
以后改一边漏改另一边），两个关键差异：

1. **不做价格保护带校验**：LIMIT类型的`price`字段是"触发后要执行的价格"，本来就该
   接近触发价、可能远离当前标记价——这正是条件单存在的意义，拿当前标记价去校验没有意义。
2. **名义价值/保证金的估值基准分两层**：`estimatePrice`（LIMIT用`price`本身，MARKET
   退回`triggerPrice`）只用来把`marginAmount`换算成数量，这一层必须尊重调用方自己填的值，
   不能被放大。但分档校验/`FreezeMargin`实际冻结用的是`orderNotionalPrice`——
   SHORT+OPEN时是`max(estimatePrice, 参考价)`，理由跟`addOrder`的`orderNotionalPrice`
   完全一样（见 [matching-and-settlement.md](matching-and-settlement.md#冻结保证金的保守估计)）：
   LIMIT类型的`price`是调用方自己填的执行价，可以故意填得远低于参考价，触发后这笔委托会
   以真正的LIMIT SELL身份提交撮合，立刻按盘口对手的真实（更高）价格成交，不是按这个填的
   低价——如果不加这道保守估计，条件单这条路径会重新打开`addOrder`那次已经修复过的漏洞
   （commit 5cece54）。

**开仓方向**（`action=open`）在创建时就做分档杠杆校验+`FreezeMargin`，冻结的
`frozen_margin`/`frozen_credit`记在`conditional_orders`表自己的行上（不是`orders`表，
因为这时候还没有对应的`orders`行）。这跟普通LIMIT开仓单的语义一致：委托一旦挂出去，
保证金就该被冻结，不等到真正成交才冻结。

**分档校验的名义价值统计**（`Server.existingOpenNotional`）把三类"已经占用/即将占用"
的名义价值都算进去：已成交仓位 + 排队中的普通开仓委托 + 排队中的条件开仓委托。只统计
其中一两类会被"普通单+条件单混着下"绕开分档上限——这是对已修复的"排队单不计入分档"
漏洞（见 [known-limitations.md](known-limitations.md)）的延伸，条件单是后加的委托类型，
必须覆盖到同一个口径。

**平仓方向**（`action=close`）不冻结保证金、不做分档校验，跟普通CLOSE委托的处理一致。

## 触发扫描（`ConditionalOrderService.ScanOnce`）

运行在`contract-engine`里，定时（`ConditionalScanIntervalMs`，默认2秒）扫一遍全部
`status=pending`的条件单，跟当前标记价格比较。这个设计选择是因为：

- 创建/撤销只需要读写MySQL，不需要碰撞合引擎的内存状态，放在`contract-api`同步完成，
  跟普通委托的落库时机保持一致
- 触发要把条件单转成一笔真正的委托并提交撮合，这一步必须在`contract-engine`进程内
  完成（跟撤单、结束本轮走Kafka路由过去是同样的道理——`contract-api`摸不到内存订单簿）
- 用定时ticker而不是"每次标记价格更新就检查"，是为了不把触发检查耦合进成交结算的
  热路径，跟`LiquidationService.RiskScanOnce`是同一种模式

触发时（`trigger`方法）：

1. 原子标记`status: pending → triggered`（`MarkTriggered`的`WHERE status='pending'`
   守卫），失败说明撤单请求并发赢了，跳过——**不会**出现"已撤销的条件单又被触发"
2. 把条件单的字段落地成一笔`orders`表记录，`order_id`复用条件单自己的，MARKET类型的
   `price`用触发时刻的标记价（LIMIT类型用条件单自己指定的`price`）
3. 调用`EngineService.SubmitOrder`，走跟普通委托完全一样的撮合流程

触发后是否立刻成交，取决于撮合引擎里有没有对手盘——如果是MARKET类型但当时没有对手盘，
会跟普通市价单缺流动性时一样停留在`未成交`状态（这是MARKET单本身的既有行为，不是条件单
特有的问题，见下面"已知限制"）。

## 撤销（`POST /order/conditional/cancel/:orderId`）

全程只碰MySQL，不需要经Kafka路由给`contract-engine`——条件单触发前从来没进过撮合引擎
的内存订单簿，没有"挂单簿"这一步要摘，直接原子标记取消+退回冻结保证金即可，比普通委托
撤单更简单。`MarkCanceled`同样用`WHERE status='pending'`守卫，跟触发扫描互斥。

条件单从来没有"部分成交"这一说（触发之前压根没提交撮合），撤销就是把`frozen_margin`/
`frozen_credit`整笔退回，不需要像普通委托撤单那样按剩余量比例计算。

## 已知限制

- **触发时不重新校验分档/杠杆**：创建时已经按当时的持仓+挂单状态做过分档校验、冻结好
  保证金，触发（可能是很久之后）时不会重新校验——这跟普通LIMIT委托挂出去之后、真正成交
  前也不会重新校验是同一个道理，是一致的设计，不是遗漏。
- **同一uid在创建/触发/撤销之间的并发竞态**：不是新问题，属于整个系统已经接受的MVP
  简化，见 [known-limitations.md](known-limitations.md)。这里有一个变体：条件单触发时
  "标记`triggered`"和"落地到`orders`表"这两步之间有一个很窄的时间窗口，这个窗口内
  `existingOpenNotional`（分档校验用）既不会在`conditional_orders`里看到它（状态已经
  不是`pending`），也不会在`orders`里看到它（还没插入），理论上这个窗口内并发提交的
  另一笔开仓委托可能漏算这部分名义价值。跟其它已接受的并发简化同一类风险、同一个不修的
  理由——真正的根治需要事务或锁，这套系统的一贯取舍是不引入。
- **`FindAllPending`没有分页，每次扫描都是全表**：条件单数量级还小的MVP阶段影响有限，
  规模变大后需要重新评估，参考`TierFor`缓存那条已知限制的取舍逻辑（见
  [known-limitations.md](known-limitations.md)）。
- **结束本轮（`POST /account/round/close`）会撤销全部pending条件单**：包括还没触发的
  止盈止损——`round`推进之后`credit`已经清零，留着的条件单如果继续存在、以后又触发，
  可能用到已经不属于这一轮的`credit`额度。所以`EngineService.CloseRound`把pending
  条件单的撤销和普通挂单的撤销一起，作为"清算credit之前必须先清空"的前置条件之一，
  见 [account-and-margin.md](account-and-margin.md#轮次round生命周期)。
