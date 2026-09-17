package repo

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/shopspring/decimal"
)

const insuranceFundID = 1

type InsuranceFundRepo struct{ db *sqlx.DB }

func NewInsuranceFundRepo(db *sqlx.DB) *InsuranceFundRepo { return &InsuranceFundRepo{db: db} }

func (r *InsuranceFundRepo) FreshBalance(ctx context.Context) (decimal.Decimal, error) {
	var v decimal.Decimal
	err := r.db.GetContext(ctx, &v, `SELECT balance FROM insurance_fund WHERE id = ?`, insuranceFundID)
	return v, err
}

// Adjust 调整基金余额+写一条流水，允许余额变负(代表系统亏空)，MVP阶段只记日志告警，
// 不做熔断——调用方(service层)负责在余额变负时打日志
func (r *InsuranceFundRepo) Adjust(ctx context.Context, symbol string, uid, positionID uint64, amount decimal.Decimal, remark string, now int64) (decimal.Decimal, error) {
	if _, err := r.db.ExecContext(ctx, `UPDATE insurance_fund SET balance = balance + ? WHERE id = ?`, amount, insuranceFundID); err != nil {
		return decimal.Zero, err
	}
	balanceAfter, err := r.FreshBalance(ctx)
	if err != nil {
		return decimal.Zero, err
	}
	_, err = r.db.ExecContext(ctx, `INSERT INTO insurance_fund_ledger
		(symbol, uid, position_id, amount, balance_after, remark, create_time) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		symbol, uid, positionID, amount, balanceAfter, remark, now)
	return balanceAfter, err
}
