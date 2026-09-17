# 强平与保险基金

## 触发条件：全仓联合强平

`LiquidationService.RiskScanOnce`定时扫描全部有仓位的账户（`RiskScanIntervalMs`，默认2秒
一次），判断标准：

```
账户权益(available + 全部持仓未实现盈亏) <= 全部仓位维持保证金要求之和
```

一旦触发，这个uid名下**所有**仓位（不分symbol）一起强平，不是只平触发条件的那一个仓位——
这是"全仓"模式的定义：风险共享，也共同承担强平。

维持保证金要求之和的计算见 [risk-limit-tiers.md](risk-limit-tiers.md)。

## 两级强平机制

1. **挂保护价限价单排队**（`queueLiquidation`）：按标记价格±维持保证金率×2倍缓冲算一个
   保护价，挂一张`reduce_only=true, liquidation=true`的限价单，走正常撮合流程排队成交。
   保护价缓冲的思路借鉴真实交易所"破产价附近留保护价"——不是按最优价格甩卖，给一点滑点
   空间换成交确定性。
2. **超时兜底直接结算**（`settleTimeoutFallback`）：挂出去`LiquidationOrderTimeoutMs`
   （默认10秒）还没成交完，撤掉剩余部分，按当前标记价直接结算，不再等真实撮合。

`positions.status`在强平期间是`LIQUIDATING`，`MarkLiquidating`是一次原子guard，保证同一个
仓位不会被同一轮/连续几轮扫描重复挂出强平单。

## 强平结算后的两个分支（`HandleLiquidationSettleAftermath`）

结算完之后看账户`available`：

1. **穿仓（`available < 0`）**：保险基金垫付缺口，基金余额允许变负（代表系统亏空），
   MVP阶段只记日志告警，不做熔断、不做自动减仓（ADL）。
2. **有维持保证金缓冲（`available > 0`，且这个uid已经没有剩余仓位）**：这部分是维持保证金
   要求留下的缓冲，**不退给用户**——真实交易所是按破产价结算、多出来的差价当清算费进保险
   基金；这里不改结算价格/撮合逻辑，改成结算完直接把这部分正数余额扫进保险基金、账户清零，
   经济结果等价。

缓冲清算的扣款方向跟日常亏损（手续费、已实现盈亏）保持一致：都是先动`available`，不单独
为强平缓冲设计不同的清算顺序。

## 保险基金

`insurance_fund`表全局唯一一行，`insurance_fund_ledger`记流水。`InsuranceFundService.Adjust`
的`amount`正数=强平盈余注入，负数=基金垫付穿仓亏损，无熔断机制——余额变负只打
`[WARN]`日志。

## 明确没做的

- **自动减仓（ADL）**：保险基金不够覆盖穿仓时，真实交易所会强制减仓对手方最赚钱的仓位来
  补窟窿，这里没做，穿仓全靠保险基金硬扛。
- **大仓位分批强平**：现在是一次性把整个仓位数量挂一张保护价单，没有像真实交易所那样对
  大仓位分批限价平仓来减少对盘口的冲击。

详见 [known-limitations.md](known-limitations.md)。
