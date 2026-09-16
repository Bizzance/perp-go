package repo

import (
	"context"

	"github.com/jmoiron/sqlx"

	"perp-go/internal/model"
)

type TradeRepo struct{ db *sqlx.DB }

func NewTradeRepo(db *sqlx.DB) *TradeRepo { return &TradeRepo{db: db} }

func (r *TradeRepo) Insert(ctx context.Context, t *model.Trade) error {
	_, err := r.db.NamedExecContext(ctx, `INSERT INTO trades
		(trade_id, symbol, price, volume, buy_order_id, sell_order_id, buy_uid, sell_uid, maker_order_id, create_time)
		VALUES (:trade_id, :symbol, :price, :volume, :buy_order_id, :sell_order_id, :buy_uid, :sell_uid, :maker_order_id, :create_time)`, t)
	return err
}

func (r *TradeRepo) FindByUID(ctx context.Context, uid uint64, limit int) ([]model.Trade, error) {
	var trades []model.Trade
	err := r.db.SelectContext(ctx, &trades,
		`SELECT * FROM trades WHERE buy_uid = ? OR sell_uid = ? ORDER BY trade_id DESC LIMIT ?`, uid, uid, limit)
	return trades, err
}
