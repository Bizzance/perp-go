package repo

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"

	"perp-go/internal/model"
)

type CoinRepo struct{ db *sqlx.DB }

func NewCoinRepo(db *sqlx.DB) *CoinRepo { return &CoinRepo{db: db} }

func (r *CoinRepo) FindBySymbol(ctx context.Context, symbol string) (*model.Coin, error) {
	var c model.Coin
	err := r.db.GetContext(ctx, &c, `SELECT symbol, base_coin_scale, price_scale, enable, max_leverage,
		maker_fee, taker_fee, maintenance_margin_rate, price_tick, volume_step, min_volume, max_volume
		FROM coins WHERE symbol = ?`, symbol)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &c, err
}

func (r *CoinRepo) FindAllEnabled(ctx context.Context) ([]model.Coin, error) {
	var coins []model.Coin
	err := r.db.SelectContext(ctx, &coins, `SELECT symbol, base_coin_scale, price_scale, enable, max_leverage,
		maker_fee, taker_fee, maintenance_margin_rate, price_tick, volume_step, min_volume, max_volume
		FROM coins WHERE enable = 1`)
	return coins, err
}
