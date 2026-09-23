package service

import (
	"context"
	"log"
	"sort"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
)

// 保险基金不够覆盖这次强平穿仓缺口时，强制减仓这个symbol上跟被强平方向相反、
// 当前最赚钱（按ROE排序）的仓位，把它们刚实现的那部分盈利划一部分给保险基金，补上基金
// 覆盖不了的差额——见docs/liquidation.md"自动减仓(ADL)"一节。
//
// 这不是让被强平仓位跟这些ADL目标在盘口上真的撮合成交一笔（那样需要在撮合链路里插入一种
// 新的委托来源，复杂得多），而是两次独立的强制结算分别完成：被强平方已经在上一步按标记价
// 结算过了，这里对ADL目标也按标记价强制平掉需要的量、正常结算它自己的已实现盈亏，然后单独
// 从这笔刚到账的盈利里划一部分给保险基金。两次结算加起来的经济结果，跟"两者直接在盘口按
// 标记价成交一笔"是等价的（都是：被强平方在标记价了结、对手方也在标记价了结、差额去处
// 一致），只是实现上不用改撮合链路。
//
// 返回实际筹到的金额，可能小于needed（反向仓位不够多，或者反向仓位当前都在亏钱、没有可以
// 强减的）——筹不够的部分仍然由保险基金自己硬扛，这是明确接受的取舍：极端单边行情下，
// 亏钱的一方和赚钱的一方本来就可能严重不对称，ADL不是万能的，基金兜底始终是最后一道防线。
func (e *EngineService) runADL(ctx context.Context, symbol string, targetSide model.Side, needed decimal.Decimal) decimal.Decimal {
	mark, hasMark := e.markPrice.Get(ctx, symbol)
	if !hasMark {
		return decimal.Zero
	}
	candidates, err := e.positionSvc.FindOpenBySymbol(ctx, symbol)
	if err != nil {
		log.Printf("[ERROR] ADL查询候选仓位失败, symbol=%s: %v", symbol, err)
		return decimal.Zero
	}

	type adlCandidate struct {
		p   model.Position
		roe decimal.Decimal
		pnl decimal.Decimal
	}
	pool := make([]adlCandidate, 0, len(candidates))
	for _, p := range candidates {
		if p.Side != targetSide || p.Volume.Sign() <= 0 || p.Status != model.PositionStatusNormal || p.PositionMargin.Sign() <= 0 {
			continue
		}
		pnl := p.UnrealizedPnl(mark)
		if pnl.Sign() <= 0 {
			continue // 只强减真正在赚钱的仓位，亏钱的不该被ADL选中——减了也筹不到钱
		}
		pool = append(pool, adlCandidate{p: p, roe: pnl.Div(p.PositionMargin), pnl: pnl})
	}
	// 按ROE(单位保证金的盈利比例)降序——跟真实交易所ADL队列的排序思路一致：同样的名义
	// 盈利，杠杆越高/占用保证金越少，ROE越高，越优先被选中，因为这类仓位"性价比"最高
	sort.Slice(pool, func(i, j int) bool { return pool[i].roe.GreaterThan(pool[j].roe) })

	raised := decimal.Zero
	remaining := needed
	for _, cand := range pool {
		if remaining.Sign() <= 0 {
			break
		}
		pnlPerUnit := cand.pnl.Div(cand.p.Volume)
		closeVolume := cand.p.Volume
		if neededVolume := remaining.Div(pnlPerUnit); neededVolume.LessThan(closeVolume) {
			closeVolume = neededVolume
		}
		result, err := e.forceCloseOnePosition(ctx, cand.p, closeVolume, mark)
		if err != nil {
			log.Printf("[ERROR] ADL强制减仓失败, uid=%d, symbol=%s, side=%s: %v", cand.p.UID, symbol, cand.p.Side, err)
			continue
		}
		contribution := decimal.Min(result.RealizedPnl, remaining)
		if contribution.Sign() > 0 {
			// 从这笔刚结算到balance的盈利里划出贡献部分给保险基金——不能划credit(这个
			// account.SettleToBalance只动balance，跟SettlePnl结算已实现盈亏时的落点一致，
			// ADL捐出去的必须是刚实现的那部分盈利本身，不能牵连到这个账户完全无关的credit额度)
			if err := e.accounts.SettleToBalance(ctx, cand.p.UID, contribution.Neg()); err != nil {
				log.Printf("[ERROR] ADL划转盈利失败, uid=%d: %v", cand.p.UID, err)
			} else if err := e.fund.Adjust(ctx, symbol, cand.p.UID, cand.p.ID, contribution, "ADL强制减仓注入保险基金"); err != nil {
				log.Printf("[ERROR] ADL盈利入基金失败, uid=%d: %v", cand.p.UID, err)
			} else {
				raised = raised.Add(contribution)
				remaining = remaining.Sub(contribution)
			}
		}
		log.Printf("[WARN] 触发ADL强制减仓, uid=%d, symbol=%s, side=%s, 减仓量=%s, 已实现盈亏=%s, 划转保险基金=%s",
			cand.p.UID, symbol, cand.p.Side, closeVolume, result.RealizedPnl, contribution)
		e.push.PublishUserSnapshot(ctx, cand.p.UID)
	}
	return raised
}
