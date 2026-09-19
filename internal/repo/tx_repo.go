package repo

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/shopspring/decimal"

	"perp-go/internal/model"
)

type TxRepo struct{ db *sqlx.DB }

func NewTxRepo(db *sqlx.DB) *TxRepo { return &TxRepo{db: db} }

func (r *TxRepo) Insert(ctx context.Context, uid uint64, symbol, txType string, amount decimal.Decimal, now int64) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO member_transactions (uid, symbol, amount, type, create_time) VALUES (?, ?, ?, ?, ?)`,
		uid, symbol, amount, txType, now)
	return err
}

// 资金流水，id倒序。txType非空时只查这个类型(见model.Tx*常量)，before>0时只返回
// id<before的行，翻页规则见OrderRepo.FindHistoryByUID
func (r *TxRepo) FindByUID(ctx context.Context, uid uint64, txType string, limit int, before uint64) ([]model.Transaction, error) {
	query := `SELECT id, uid, symbol, amount, type, create_time, request_id FROM member_transactions WHERE uid = ?`
	args := []any{uid}
	if txType != "" {
		query += ` AND type = ?`
		args = append(args, txType)
	}
	if before > 0 {
		query += ` AND id < ?`
		args = append(args, before)
	}
	query += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)
	var txs []model.Transaction
	err := r.db.SelectContext(ctx, &txs, query, args...)
	return txs, err
}
