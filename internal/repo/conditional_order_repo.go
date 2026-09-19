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
		 leverage, reduce_only, frozen_margin, frozen_credit, status, create_time, update_time, request_id, request_hash)
		VALUES (:order_id, :uid, :symbol, :side, :action, :trigger_price, :trigger_direction, :type, :price, :amount,
		 :leverage, :reduce_only, :frozen_margin, :frozen_credit, :status, :create_time, :update_time, :request_id, :request_hash)`, co)
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

// pending状态的条件单，symbol为空查全部——用法跟OrderRepo.FindActiveByUID一致
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

// 按合作方指定的幂等键查这个uid名下的条件单，没有返回(nil, nil)
func (r *ConditionalOrderRepo) FindByRequestID(ctx context.Context, uid uint64, requestID string) (*model.ConditionalOrder, error) {
	var co model.ConditionalOrder
	err := r.db.GetContext(ctx, &co, `SELECT * FROM conditional_orders WHERE uid = ? AND request_id = ?`, uid, requestID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &co, err
}

// 条件单历史，翻页规则见OrderRepo.FindHistoryByUID
func (r *ConditionalOrderRepo) FindHistoryByUID(ctx context.Context, uid uint64, limit int, before uint64) ([]model.ConditionalOrder, error) {
	var orders []model.ConditionalOrder
	if before > 0 {
		err := r.db.SelectContext(ctx, &orders,
			`SELECT * FROM conditional_orders WHERE uid = ? AND order_id < ? ORDER BY order_id DESC LIMIT ?`, uid, before, limit)
		return orders, err
	}
	err := r.db.SelectContext(ctx, &orders, `SELECT * FROM conditional_orders WHERE uid = ? ORDER BY order_id DESC LIMIT ?`, uid, limit)
	return orders, err
}

// 全部还没触发的条件单，engine端定时扫描用——量级不大，一次性拉出来
// 在应用层按symbol分组跟当前标记价格比较，不用为每个symbol单独查一次
func (r *ConditionalOrderRepo) FindAllPending(ctx context.Context) ([]model.ConditionalOrder, error) {
	var orders []model.ConditionalOrder
	err := r.db.SelectContext(ctx, &orders, `SELECT * FROM conditional_orders WHERE status = 'pending'`)
	return orders, err
}

// 原子标记触发——WHERE status='pending'保证跟撤单(MarkCanceled)并发时
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

// 只有MarkTriggered已经成功、但触发后落地成真正委托这一步失败
// (见ConditionalOrderService.trigger)时才会用到——跟MarkCanceled几乎一样，唯一区别是
// WHERE条件是status='triggered'而不是'pending'，这两个方法故意不合并成一个，是为了让
// 调用方在代码层面就清楚自己是在撤销"还没触发的条件单"还是在为"触发失败"这种异常情况
// 兜底，不能靠一个通用方法掩盖这两种场景在语义上的区别
func (r *ConditionalOrderRepo) MarkCanceledFromTriggered(ctx context.Context, orderID uint64, updateTime int64) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE conditional_orders SET status = 'canceled', update_time = ? WHERE order_id = ? AND status = 'triggered'`,
		updateTime, orderID)
	return affected(res, err)
}
