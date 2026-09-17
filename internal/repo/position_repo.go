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
// 的部分——一个仓位可能由多笔成交(甚至多笔不同订单)累积而成，每笔成交的available/credit
// 来源比例可能不一样，必须在仓位上按金额累加着记credit_margin，平仓时才能按仓位整体精确
// 的来源比例释放，不能只看最后一笔成交的比例
func (r *PositionRepo) ApplyOpenFill(
	ctx context.Context,
	uid uint64,
	symbol string,
	side model.Side,
	dealVolume, dealPrice, addedMargin, addedCreditMargin decimal.Decimal,
	leverage uint32,
	updateTime int64,
) error {
	existing, err := r.Find(ctx, uid, symbol, side)
	if err != nil {
		return err
	}
	if existing == nil {
		_, err = r.db.ExecContext(ctx,
			`INSERT INTO positions (uid, symbol, side, volume, avg_entry_price, position_margin, credit_margin, leverage, status, update_time)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'normal', ?)`,
			uid, symbol, side, dealVolume, dealPrice, addedMargin, addedCreditMargin, leverage, updateTime)
		return err
	}
	newVolume := existing.Volume.Add(dealVolume)
	newAvgEntry := existing.AvgEntryPrice.Mul(existing.Volume).Add(dealPrice.Mul(dealVolume)).Div(newVolume)
	_, err = r.db.ExecContext(ctx,
		`UPDATE positions SET volume = ?, avg_entry_price = ?, position_margin = position_margin + ?, credit_margin = credit_margin + ?, leverage = ?, status = 'normal', update_time = ?
		 WHERE id = ?`,
		newVolume, newAvgEntry, addedMargin, addedCreditMargin, leverage, updateTime, existing.ID)
	return err
}

// 平仓/减仓：按比例释放保证金、结算已实现盈亏——返回(realizedPnl, releasedMargin,
// releasedCreditMargin, closeVolume)。releasedMargin是这次平仓比例释放的position_margin
// (开仓时按真实成交价占用、全仓下没有真的搬钱，但这部分钱这之前一直被算作"已用掉"，平仓时
// 必须还给账户，不能只结算已实现盈亏——见settlement.go对这两个返回值的用法)，
// releasedCreditMargin是其中来自credit的部分，两者的差额才是该还回available的部分。
// closeVolume是按现有持仓量截断后的真实平仓量(防御性处理，reduce-only在上游本该保证不会超)
func (r *PositionRepo) ApplyCloseFill(
	ctx context.Context,
	uid uint64,
	symbol string,
	side model.Side,
	dealVolume, dealPrice decimal.Decimal,
	updateTime int64,
) (realizedPnl, releasedMargin, releasedCreditMargin, closeVolume decimal.Decimal, err error) {
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
	status := model.PositionStatusNormal
	if newVolume.Sign() <= 0 {
		status = model.PositionStatusClosed
		newVolume = decimal.Zero
		newMargin = decimal.Zero
		newCreditMargin = decimal.Zero
	}
	_, err = r.db.ExecContext(ctx, `UPDATE positions SET volume = ?, position_margin = ?, credit_margin = ?, status = ?, update_time = ? WHERE id = ?`,
		newVolume, newMargin, newCreditMargin, status, updateTime, p.ID)
	return realizedPnl, releasedMargin, releasedCreditMargin, closeVolume, err
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
