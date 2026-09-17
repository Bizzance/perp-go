package repo

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jmoiron/sqlx"
	"github.com/shopspring/decimal"

	"perp-go/internal/model"
)

type AccountRepo struct{ db *sqlx.DB }

func NewAccountRepo(db *sqlx.DB) *AccountRepo { return &AccountRepo{db: db} }

func (r *AccountRepo) FindByUID(ctx context.Context, uid uint64) (*model.Account, error) {
	var a model.Account
	err := r.db.GetContext(ctx, &a, `SELECT id, uid, available, frozen_margin, version FROM accounts WHERE uid = ?`, uid)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &a, err
}

// 首次访问时自动建账户——插入撞uid唯一约束时说明并发下对方已经建好了，直接
// 重新查一次用对方那行，不是错误：Go这边单条INSERT本身就是原子的，不需要额外包一层事务
func (r *AccountRepo) GetOrCreate(ctx context.Context, uid uint64) (*model.Account, error) {
	if a, err := r.FindByUID(ctx, uid); err != nil {
		return nil, err
	} else if a != nil {
		return a, nil
	}
	_, err := r.db.ExecContext(ctx, `INSERT IGNORE INTO accounts (uid, available, frozen_margin) VALUES (?, 0, 0)`, uid)
	if err != nil {
		return nil, err
	}
	return r.FindByUID(ctx, uid)
}

func (r *AccountRepo) FindFreshAvailable(ctx context.Context, id uint64) (decimal.Decimal, error) {
	var v decimal.Decimal
	err := r.db.GetContext(ctx, &v, `SELECT available FROM accounts WHERE id = ?`, id)
	return v, err
}

// FreezeFromAvailable 快路径：available单独够用，整笔从available划到frozenMargin
func (r *AccountRepo) FreezeFromAvailable(ctx context.Context, id uint64, amount decimal.Decimal) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE accounts SET available = available - ?, frozen_margin = frozen_margin + ? WHERE id = ? AND available >= ?`,
		amount, amount, id, amount)
	return affected(res, err)
}

// available+frozenMargin都不够、但账户权益(含持仓浮盈)够覆盖时的
// 最后一条路径：币安式"持仓浮盈也能当买力开新仓"——没有WHERE守卫，调用方已经在service层用
// 未实现盈亏验证过权益足够，这里只是把"允许借用浮盈"这个决定落地，available可能因此变负，
// 全仓模式下这是合法状态(强平穿仓/保险基金垫付走的就是这套)
func (r *AccountRepo) FreezeForceIntoNegative(ctx context.Context, id uint64, amount decimal.Decimal) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE accounts SET available = available - ?, frozen_margin = frozen_margin + ? WHERE id = ?`,
		amount, amount, id)
	return err
}

// UnfreezeMargin 撤单/未成交部分释放冻结的保证金
func (r *AccountRepo) UnfreezeMargin(ctx context.Context, id uint64, amount decimal.Decimal) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE accounts SET available = available + ?, frozen_margin = frozen_margin - ? WHERE id = ? AND frozen_margin >= ?`,
		amount, amount, id, amount)
	return affected(res, err)
}

// 开仓成交：冻结的保证金转移到仓位(只扣frozenMargin，全仓下不是真
// 锁定的钱，转正的这笔钱紧接着由调用方调SettleToAvailable还回available——全仓模式下
// positionMargin只是记账用的名义值，不需要真的搬钱
func (r *AccountRepo) DecreaseFrozenMargin(ctx context.Context, id uint64, amount decimal.Decimal) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE accounts SET frozen_margin = frozen_margin - ? WHERE id = ? AND frozen_margin >= ?`,
		amount, id, amount)
	return affected(res, err)
}

// 已实现盈亏/保证金归还/强平清算——amount可正可负，无守卫(全仓下
// available允许暂时为负，这是已经接受的合法状态，不是bug)
func (r *AccountRepo) SettleToAvailable(ctx context.Context, id uint64, amount decimal.Decimal) error {
	_, err := r.db.ExecContext(ctx, `UPDATE accounts SET available = available + ? WHERE id = ?`, amount, id)
	return err
}

// 手续费扣款：无守卫，允许扣成负数——这笔手续费对应的成交已经真实发生，不能因为
// 差一点钱扣不出来就不扣
func (r *AccountRepo) DeductFee(ctx context.Context, id uint64, fee decimal.Decimal) error {
	_, err := r.db.ExecContext(ctx, `UPDATE accounts SET available = available - ? WHERE id = ?`, fee, id)
	return err
}

func affected(res sql.Result, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}
