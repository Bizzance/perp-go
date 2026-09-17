package repo

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"
	"github.com/shopspring/decimal"

	"perp-go/internal/model"
)

type OrderRepo struct{ db *sqlx.DB }

func NewOrderRepo(db *sqlx.DB) *OrderRepo { return &OrderRepo{db: db} }

func (r *OrderRepo) Insert(ctx context.Context, o *model.Order) error {
	_, err := r.db.NamedExecContext(ctx, `INSERT INTO orders
		(order_id, uid, symbol, side, action, type, price, amount, traded_amount, avg_deal_price,
		 frozen_margin, frozen_credit, leverage, reduce_only, liquidation, status, create_time, update_time)
		VALUES (:order_id, :uid, :symbol, :side, :action, :type, :price, :amount, :traded_amount, :avg_deal_price,
		 :frozen_margin, :frozen_credit, :leverage, :reduce_only, :liquidation, :status, :create_time, :update_time)`, o)
	return err
}

func (r *OrderRepo) FindByOrderID(ctx context.Context, orderID uint64) (*model.Order, error) {
	var o model.Order
	err := r.db.GetContext(ctx, &o, `SELECT * FROM orders WHERE order_id = ?`, orderID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &o, err
}

// FindByOrderIDs 批量按order_id查询——自成交保护一次撮合可能摘掉好几笔自己的挂单，
// 批量查一次比每笔单独查一次(N次DB往返)更快，见EngineService.SubmitOrder
func (r *OrderRepo) FindByOrderIDs(ctx context.Context, orderIDs []uint64) ([]model.Order, error) {
	if len(orderIDs) == 0 {
		return nil, nil
	}
	query, args, err := sqlx.In(`SELECT * FROM orders WHERE order_id IN (?)`, orderIDs)
	if err != nil {
		return nil, err
	}
	query = r.db.Rebind(query)
	var orders []model.Order
	err = r.db.SelectContext(ctx, &orders, query, args...)
	return orders, err
}

func (r *OrderRepo) FindActiveByUID(ctx context.Context, uid uint64, symbol string) ([]model.Order, error) {
	var orders []model.Order
	if symbol == "" {
		err := r.db.SelectContext(ctx, &orders,
			`SELECT * FROM orders WHERE uid = ? AND status IN ('open','partially_filled') ORDER BY order_id DESC`, uid)
		return orders, err
	}
	err := r.db.SelectContext(ctx, &orders,
		`SELECT * FROM orders WHERE uid = ? AND symbol = ? AND status IN ('open','partially_filled') ORDER BY order_id DESC`, uid, symbol)
	return orders, err
}

func (r *OrderRepo) FindHistoryByUID(ctx context.Context, uid uint64, limit int) ([]model.Order, error) {
	var orders []model.Order
	err := r.db.SelectContext(ctx, &orders, `SELECT * FROM orders WHERE uid = ? ORDER BY order_id DESC LIMIT ?`, uid, limit)
	return orders, err
}

// 一笔成交对这个委托的影响：累加tradedAmount、重算加权平均成交价、按剩余量更新状态
func (r *OrderRepo) ApplyFill(ctx context.Context, orderID uint64, dealVolume, dealPrice decimal.Decimal, newStatus model.OrderStatus, updateTime int64) error {
	o, err := r.FindByOrderID(ctx, orderID)
	if err != nil || o == nil {
		return err
	}
	newTraded := o.TradedAmount.Add(dealVolume)
	newAvgDeal := o.AvgDealPrice
	if newTraded.Sign() > 0 {
		newAvgDeal = o.AvgDealPrice.Mul(o.TradedAmount).Add(dealPrice.Mul(dealVolume)).Div(newTraded)
	}
	_, err = r.db.ExecContext(ctx, `UPDATE orders SET traded_amount = ?, avg_deal_price = ?, status = ?, update_time = ? WHERE order_id = ?`,
		newTraded, newAvgDeal, newStatus, updateTime, orderID)
	return err
}

func (r *OrderRepo) MarkCanceled(ctx context.Context, orderID uint64, updateTime int64) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE orders SET status = 'canceled', update_time = ? WHERE order_id = ? AND status IN ('open','partially_filled')`,
		updateTime, orderID)
	return affected(res, err)
}
