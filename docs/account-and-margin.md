# 账户与保证金模型

## 全仓模式

账户级别共享一个资金池，不做逐仓隔离（逐仓是后续阶段的计划，见
[known-limitations.md](known-limitations.md)）。一个uid只有一行`accounts`记录：

| 字段 | 含义 |
|---|---|
| `available` | 可用余额，**可能为负** |
| `frozen_margin` | 挂单冻结的保证金 |

`available`允许为负，这是全仓模式下的合法状态，不是bug——两种情况会让它变负：
1. 用持仓浮盈当买力开新仓（见下面"冻结保证金的三级路径"）
2. 强平穿仓垫付之前，账户余额被打成负数

## 并发控制：原子条件UPDATE，不用悲观锁

账户余额的加减全部走"UPDATE ... WHERE 字段 >= 金额"这种数据库层面的原子条件更新，
例如：

```sql
UPDATE accounts SET available = available - ?, frozen_margin = frozen_margin + ?
WHERE id = ? AND available >= ?
```

这个模式下，`UPDATE`受影响行数为0就代表"条件不满足"（比如余额不够），调用方判断
`RowsAffected() > 0`即可，不需要先查询再判断再更新的悲观锁流程。极端并发下（同一个
uid同时提交多笔请求）这套方式没有完全的一致性保证，属于MVP阶段已知、接受的简化——
详见 [known-limitations.md](known-limitations.md)。

## 冻结保证金的三级路径（`FreezeMargin`）

开仓下单时，`requiredMargin`按三级路径尝试冻结：

1. **`available`够** → 直接从`available`划到`frozen_margin`
2. **`available`不够，但`available + 全部持仓未实现盈亏`够** → 强制冻结，允许
   `available`变负（币安式"持仓浮盈也能当买力开新仓"，不需要先平仓变现）
3. **两条都不够** → 拒绝，返回`ErrInsufficientMargin`

任何一个持仓缺标记价格，未实现盈亏就按0算（不计入买力）——这是保守方向：算少了买力
顶多让开仓更容易被拒绝，不会让账户透支。

## 资金流转的几个关键操作

| 操作 | 说明 |
|---|---|
| `FreezeMargin` | 挂单开仓冻结保证金，见上面三级路径 |
| `UnfreezeMargin` | 撤单/未成交部分释放冻结的保证金 |
| `DecreaseFrozenMargin` | 开仓成交：冻结保证金转移到仓位（全仓下`position_margin`只是记账用的名义值，这笔钱会立刻通过`SettleToAvailable`还回`available`，不是真的锁住） |
| `SettleToAvailable` | 已实现盈亏/保证金归还/强平清算，可正可负，无守卫 |
| `DeductFee` | 手续费扣款，无守卫，允许扣成负数——这笔手续费对应的成交已经真实发生，不能因为差一点钱扣不出来就不扣 |

## 账户查询视图（`AccountView`）

`GET /account/info`返回的是现算现填的视图，不是`accounts`表原始字段：

```
equity = available + totalUnrealizedPnl
```

`totalUnrealizedPnl`是这个uid名下全部持仓当前未实现盈亏之和，跟`FreezeMargin`第二级路径
共用同一份计算逻辑。

## 信用额度：明确没做

CLAUDE.md提到的"信用额度"（用户买保险后的赔付，可当保证金但不能提现）目前**没有实现**，
`Account`结构体和数据库表里都没有相关字段。这是一个还在讨论设计方案、暂缓实现的功能，
不是遗漏。
