package repo

import (
	"context"

	"github.com/jmoiron/sqlx"
	"github.com/shopspring/decimal"
)

type TxRepo struct{ db *sqlx.DB }

func NewTxRepo(db *sqlx.DB) *TxRepo { return &TxRepo{db: db} }

func (r *TxRepo) Insert(ctx context.Context, uid uint64, symbol, txType string, amount decimal.Decimal, now int64) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO member_transactions (uid, symbol, amount, type, create_time) VALUES (?, ?, ?, ?, ?)`,
		uid, symbol, amount, txType, now)
	return err
}
