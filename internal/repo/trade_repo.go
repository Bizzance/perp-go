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

// 这个uid参与的成交(不论是买方还是卖方)，trade_id倒序。before>0只返回trade_id<before
// 的行，翻页规则见OrderRepo.FindHistoryByUID
func (r *TradeRepo) FindByUID(ctx context.Context, uid uint64, limit int, before uint64) ([]model.Trade, error) {
	var trades []model.Trade
	if before > 0 {
		err := r.db.SelectContext(ctx, &trades,
			`SELECT * FROM trades WHERE (buy_uid = ? OR sell_uid = ?) AND trade_id < ? ORDER BY trade_id DESC LIMIT ?`,
			uid, uid, before, limit)
		return trades, err
	}
	err := r.db.SelectContext(ctx, &trades,
		`SELECT * FROM trades WHERE buy_uid = ? OR sell_uid = ? ORDER BY trade_id DESC LIMIT ?`, uid, uid, limit)
	return trades, err
}

// 这个symbol的公开成交(不区分参与者)，trade_id倒序，翻页规则同FindByUID
func (r *TradeRepo) FindBySymbol(ctx context.Context, symbol string, limit int, before uint64) ([]model.Trade, error) {
	var trades []model.Trade
	if before > 0 {
		err := r.db.SelectContext(ctx, &trades,
			`SELECT * FROM trades WHERE symbol = ? AND trade_id < ? ORDER BY trade_id DESC LIMIT ?`, symbol, before, limit)
		return trades, err
	}
	err := r.db.SelectContext(ctx, &trades,
		`SELECT * FROM trades WHERE symbol = ? ORDER BY trade_id DESC LIMIT ?`, symbol, limit)
	return trades, err
}
