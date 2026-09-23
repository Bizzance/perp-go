package repo

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"
	"github.com/shopspring/decimal"

	"perp-go/internal/model"
)

type PositionRepo struct{ db *sqlx.DB }

func NewPositionRepo(db *sqlx.DB) *PositionRepo { return &PositionRepo{db: db} }

func (r *PositionRepo) Find(ctx context.Context, uid uint64, symbol string, side model.Side) (*model.Position, error) {
	var p model.Position
	err := r.db.GetContext(ctx, &p, `SELECT * FROM positions WHERE uid = ? AND symbol = ? AND side = ?`, uid, symbol, side)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &p, err
}

func (r *PositionRepo) FindByUID(ctx context.Context, uid uint64) ([]model.Position, error) {
	var positions []model.Position
	err := r.db.SelectContext(ctx, &positions, `SELECT * FROM positions WHERE uid = ? AND volume > 0`, uid)
	return positions, err
}

// 风控扫描用：全部还有仓位的账户uid去重列表
func (r *PositionRepo) FindAllOpenUIDs(ctx context.Context) ([]uint64, error) {
	var uids []uint64
	err := r.db.SelectContext(ctx, &uids, `SELECT DISTINCT uid FROM positions WHERE volume > 0`)
	return uids, err
}

// 资金费率结算用：这个symbol下全部还有仓位的记录，不分uid
func (r *PositionRepo) FindOpenBySymbol(ctx context.Context, symbol string) ([]model.Position, error) {
	var positions []model.Position
	err := r.db.SelectContext(ctx, &positions, `SELECT * FROM positions WHERE symbol = ? AND volume > 0`, symbol)
	return positions, err
}

// 开仓/加仓：加权平均开仓价、累加保证金，不存在就先插入一行空仓位再累加
// addedMargin是这笔成交真实占用的保证金(properMargin)，addedCreditMargin是其中来自credit
// 的部分——一个仓位可能由多笔成交(甚至多笔不同订单)累积而成，每笔成交的balance/credit
// 来源比例可能不一样，必须在仓位上按金额累加着记credit_margin，平仓时才能按仓位整体精确
// 的来源比例释放，不能只看最后一笔成交的比例
//
// 用"读现有状态→算新值→按读到的旧值做WHERE条件更新"的乐观并发重试循环，不是读一次就直接
// 无条件UPDATE——同一个仓位可能被多个独立的goroutine并发触碰(Kafka下单/撤单消费者、
// LiquidationService每个仓位各自的强平goroutine、ConditionalOrderService的触发扫描)，
// 比如用户自己提交了一笔减仓单的同时这个仓位正好也在被强平处理中，两个goroutine都读到
// 同一份旧状态、都算出各自的新值再各自UPDATE，后写的会把先写的贡献静默覆盖掉——WHERE
// 条件核对全部相关字段没有被改过，改过了就重读最新状态重算重试，不会静默丢update
func (r *PositionRepo) ApplyOpenFill(
	ctx context.Context,
	uid uint64,
	symbol string,
	side model.Side,
	dealVolume, dealPrice, addedMargin, addedCreditMargin decimal.Decimal,
	leverage uint32,
	updateTime int64,
) error {
	for {
		existing, err := r.Find(ctx, uid, symbol, side)
		if err != nil {
			return err
		}
		if existing == nil {
			res, err := r.db.ExecContext(ctx,
				`INSERT IGNORE INTO positions (uid, symbol, side, volume, avg_entry_price, position_margin, credit_margin, leverage, status, update_time)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'normal', ?)`,
				uid, symbol, side, dealVolume, dealPrice, addedMargin, addedCreditMargin, leverage, updateTime)
			ok, err := affected(res, err)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
			// INSERT IGNORE没插进去，说明刚才判断"不存在"和现在插入之间，被另一个并发
			// 调用抢先插入了同一行——重新循环，这次会走到下面的UPDATE分支
			continue
		}
		newVolume := existing.Volume.Add(dealVolume)
		newAvgEntry := existing.AvgEntryPrice.Mul(existing.Volume).Add(dealPrice.Mul(dealVolume)).Div(newVolume)
		newMargin := existing.PositionMargin.Add(addedMargin)
		newCreditMargin := existing.CreditMargin.Add(addedCreditMargin)
		res, err := r.db.ExecContext(ctx,
			`UPDATE positions SET volume = ?, avg_entry_price = ?, position_margin = ?, credit_margin = ?, leverage = ?, status = 'normal', update_time = ?
			 WHERE id = ? AND volume = ? AND position_margin = ? AND credit_margin = ?`,
			newVolume, newAvgEntry, newMargin, newCreditMargin, leverage, updateTime,
			existing.ID, existing.Volume, existing.PositionMargin, existing.CreditMargin)
		ok, err := affected(res, err)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		// 命中了并发写冲突，重新读最新状态重试
	}
}

// 平仓/减仓：按比例释放保证金、结算已实现盈亏——返回(realizedPnl, releasedMargin,
// releasedCreditMargin, closeVolume)。releasedMargin是这次平仓比例释放的position_margin
// (开仓时按真实成交价占用、全仓下没有真的搬钱，但这部分钱这之前一直被算作"已用掉"，平仓时
// 必须还给账户，不能只结算已实现盈亏——见settlement.go对这两个返回值的用法)，
// releasedCreditMargin是其中来自credit的部分，两者的差额才是该解锁frozen_margin的部分。
// closeVolume是按现有持仓量截断后的真实平仓量(防御性处理，reduce-only在上游本该保证不会超)
//
// 跟ApplyOpenFill一样，用乐观并发重试循环——同一个仓位在分批强平(docs/liquidation.md)期间
// 会被同一个goroutine连续多次ApplyCloseFill，但也可能同时被别的路径(用户自己提交的减仓单、
// 甚至另一个强平goroutine)并发触碰，读到的旧状态之间必须靠WHERE条件核对，不能无条件覆盖。
//
// status字段保留调用前的值(可能是normal也可能是liquidating)，只有volume真的归零才强制
// 改成closed——这不是随手的选择：早期实现这里无条件把status写成normal，导致分批强平
// 处理到一半(仓位还有剩余量、但这一批已经结算完)时，position.status被静默改回normal，
// MarkLiquidating的原子guard(WHERE status='normal')会在下一次风控扫描(RiskScanOnce)时
// 误判"这个仓位没在强平"、重新成功抢到guard，给同一个仓位又并发起一个liquidateInClips
// goroutine——实测复现过：同一个positionID在一次强平流程里MarkLiquidating连续成功3次，
// 3个liquidateInClips goroutine同时对着同一个仓位提交重叠的强平批次。保留原状态、只在
// volume真的清零时才终结，从根上让这类中途的"正常"结算不再能撤销"仍在强平中"这个标记
func (r *PositionRepo) ApplyCloseFill(
	ctx context.Context,
	uid uint64,
	symbol string,
	side model.Side,
	dealVolume, dealPrice decimal.Decimal,
	updateTime int64,
) (realizedPnl, releasedMargin, releasedCreditMargin, closeVolume decimal.Decimal, err error) {
	for {
		p, err := r.Find(ctx, uid, symbol, side)
		if err != nil || p == nil || p.Volume.Sign() <= 0 {
			return decimal.Zero, decimal.Zero, decimal.Zero, decimal.Zero, err
		}
		closeVolume = decimal.Min(dealVolume, p.Volume)
		var priceDiff decimal.Decimal
		if side == model.SideLong {
			priceDiff = dealPrice.Sub(p.AvgEntryPrice)
		} else {
			priceDiff = p.AvgEntryPrice.Sub(dealPrice)
		}
		realizedPnl = priceDiff.Mul(closeVolume)
		releasedMargin = decimal.Zero
		releasedCreditMargin = decimal.Zero
		if p.Volume.Sign() > 0 {
			releasedMargin = p.PositionMargin.Mul(closeVolume).Div(p.Volume)
			releasedCreditMargin = p.CreditMargin.Mul(closeVolume).Div(p.Volume)
		}
		newVolume := p.Volume.Sub(closeVolume)
		newMargin := p.PositionMargin.Sub(releasedMargin)
		newCreditMargin := p.CreditMargin.Sub(releasedCreditMargin)
		status := p.Status
		if newVolume.Sign() <= 0 {
			status = model.PositionStatusClosed
			newVolume = decimal.Zero
			newMargin = decimal.Zero
			newCreditMargin = decimal.Zero
		}
		res, err := r.db.ExecContext(ctx,
			`UPDATE positions SET volume = ?, position_margin = ?, credit_margin = ?, status = ?, update_time = ?
			 WHERE id = ? AND volume = ? AND position_margin = ? AND credit_margin = ? AND status = ?`,
			newVolume, newMargin, newCreditMargin, status, updateTime,
			p.ID, p.Volume, p.PositionMargin, p.CreditMargin, p.Status)
		ok, err := affected(res, err)
		if err != nil {
			return decimal.Zero, decimal.Zero, decimal.Zero, decimal.Zero, err
		}
		if ok {
			return realizedPnl, releasedMargin, releasedCreditMargin, closeVolume, nil
		}
		// 命中了并发写冲突，重新读最新状态重试
	}
}

// 修改仓位杠杆：newMargin/newCreditMargin是按新杠杆重新算好的、这个仓位
// 应该占用的保证金(及其来自credit的部分)，覆盖写回——调用方已经按新旧保证金的差额完成了
// FreezeMargin/UnfreezeMargin，这里只负责把仓位自己的记账字段同步成新值。expectedVolume
// 是调用方读取仓位时看到的volume，WHERE volume=?是乐观并发保护：调用方(router.go的
// setLeverage)本身已经用LockService按uid+symbol+side加锁序列化了，理论上不会触发，这里
// 是防御性的第二层
func (r *PositionRepo) UpdateLeverage(ctx context.Context, id uint64, newMargin, newCreditMargin decimal.Decimal, newLeverage uint32, expectedVolume decimal.Decimal, updateTime int64) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE positions SET position_margin = ?, credit_margin = ?, leverage = ?, update_time = ? WHERE id = ? AND volume = ?`,
		newMargin, newCreditMargin, newLeverage, updateTime, id, expectedVolume)
	return affected(res, err)
}

// 强平挂盘口排队成交：把仓位原子标记LIQUIDATING，成功才可以往下挂强平单，
// 失败说明上一轮已经挂出去了、还没成交完，本轮扫描跳过这个仓位
func (r *PositionRepo) MarkLiquidating(ctx context.Context, id uint64) (bool, error) {
	res, err := r.db.ExecContext(ctx, `UPDATE positions SET status = 'liquidating' WHERE id = ? AND status = 'normal'`, id)
	return affected(res, err)
}

func (r *PositionRepo) ClearLiquidating(ctx context.Context, id uint64) error {
	_, err := r.db.ExecContext(ctx, `UPDATE positions SET status = 'normal' WHERE id = ? AND status = 'liquidating'`, id)
	return err
}
