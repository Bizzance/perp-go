package repo

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"

	"perp-go/internal/model"
)

type ConditionalOrderRepo struct{ db *sqlx.DB }

func NewConditionalOrderRepo(db *sqlx.DB) *ConditionalOrderRepo { return &ConditionalOrderRepo{db: db} }

func (r *ConditionalOrderRepo) Insert(ctx context.Context, co *model.ConditionalOrder) error {
	_, err := r.db.NamedExecContext(ctx, `INSERT INTO conditional_orders
		(order_id, uid, symbol, side, action, trigger_price, trigger_direction, type, price, amount,
		 leverage, reduce_only, frozen_margin, frozen_credit, status, create_time, update_time)
		VALUES (:order_id, :uid, :symbol, :side, :action, :trigger_price, :trigger_direction, :type, :price, :amount,
		 :leverage, :reduce_only, :frozen_margin, :frozen_credit, :status, :create_time, :update_time)`, co)
	return err
}

func (r *ConditionalOrderRepo) FindByOrderID(ctx context.Context, orderID uint64) (*model.ConditionalOrder, error) {
	var co model.ConditionalOrder
	err := r.db.GetContext(ctx, &co, `SELECT * FROM conditional_orders WHERE order_id = ?`, orderID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &co, err
}

// FindActiveByUID pending状态的条件单，symbol为空查全部——用法跟OrderRepo.FindActiveByUID一致
func (r *ConditionalOrderRepo) FindActiveByUID(ctx context.Context, uid uint64, symbol string) ([]model.ConditionalOrder, error) {
	var orders []model.ConditionalOrder
	if symbol == "" {
		err := r.db.SelectContext(ctx, &orders,
			`SELECT * FROM conditional_orders WHERE uid = ? AND status = 'pending' ORDER BY order_id DESC`, uid)
		return orders, err
	}
	err := r.db.SelectContext(ctx, &orders,
		`SELECT * FROM conditional_orders WHERE uid = ? AND symbol = ? AND status = 'pending' ORDER BY order_id DESC`, uid, symbol)
	return orders, err
}

func (r *ConditionalOrderRepo) FindHistoryByUID(ctx context.Context, uid uint64, limit int) ([]model.ConditionalOrder, error) {
	var orders []model.ConditionalOrder
	err := r.db.SelectContext(ctx, &orders, `SELECT * FROM conditional_orders WHERE uid = ? ORDER BY order_id DESC LIMIT ?`, uid, limit)
	return orders, err
}

// FindAllPending 全部还没触发的条件单，engine端定时扫描用——量级不大，一次性拉出来
// 在应用层按symbol分组跟当前标记价格比较，不用为每个symbol单独查一次
func (r *ConditionalOrderRepo) FindAllPending(ctx context.Context) ([]model.ConditionalOrder, error) {
	var orders []model.ConditionalOrder
	err := r.db.SelectContext(ctx, &orders, `SELECT * FROM conditional_orders WHERE status = 'pending'`)
	return orders, err
}

// MarkTriggered 原子标记触发——WHERE status='pending'保证跟撤单(MarkCanceled)并发时
// 只有一个能成功，不会出现"已经撤销的条件单又被触发"或者反过来的情况
func (r *ConditionalOrderRepo) MarkTriggered(ctx context.Context, orderID uint64, updateTime int64) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE conditional_orders SET status = 'triggered', update_time = ? WHERE order_id = ? AND status = 'pending'`,
		updateTime, orderID)
	return affected(res, err)
}

func (r *ConditionalOrderRepo) MarkCanceled(ctx context.Context, orderID uint64, updateTime int64) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE conditional_orders SET status = 'canceled', update_time = ? WHERE order_id = ? AND status = 'pending'`,
		updateTime, orderID)
	return affected(res, err)
}
