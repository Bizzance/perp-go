package repo

import (
	"context"

	"github.com/jmoiron/sqlx"

	"perp-go/internal/model"
)

type FundingRepo struct{ db *sqlx.DB }

func NewFundingRepo(db *sqlx.DB) *FundingRepo { return &FundingRepo{db: db} }

// LastFundingTime 这个symbol最近一次结算落库的周期时间点，从没结算过返回0——
// FundingService.SettleIfDue靠这个判断"当前的结算周期边界是不是已经结算过了"
func (r *FundingRepo) LastFundingTime(ctx context.Context, symbol string) (int64, error) {
	var t int64
	err := r.db.GetContext(ctx, &t, `SELECT COALESCE(MAX(funding_time), 0) FROM funding_rate_history WHERE symbol = ?`, symbol)
	return t, err
}

func (r *FundingRepo) Insert(ctx context.Context, rec *model.FundingRateRecord) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO funding_rate_history (symbol, funding_time, rate, mark_price, index_price, create_time)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		rec.Symbol, rec.FundingTime, rec.Rate, rec.MarkPrice, rec.IndexPrice, rec.CreateTime)
	return err
}

func (r *FundingRepo) FindHistory(ctx context.Context, symbol string, limit int) ([]model.FundingRateRecord, error) {
	var records []model.FundingRateRecord
	err := r.db.SelectContext(ctx, &records,
		`SELECT * FROM funding_rate_history WHERE symbol = ? ORDER BY funding_time DESC LIMIT ?`, symbol, limit)
	return records, err
}
