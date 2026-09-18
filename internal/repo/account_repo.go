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
	err := r.db.GetContext(ctx, &a,
		`SELECT id, uid, is_insured, round, credit, available, frozen_margin, frozen_credit, version
		 FROM accounts WHERE uid = ?`, uid)
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

func (r *AccountRepo) FindFreshCredit(ctx context.Context, id uint64) (decimal.Decimal, error) {
	var v decimal.Decimal
	err := r.db.GetContext(ctx, &v, `SELECT credit FROM accounts WHERE id = ?`, id)
	return v, err
}

// FreezeFromAvailable 快路径：available单独够用，整笔从available划到frozenMargin，不动credit
func (r *AccountRepo) FreezeFromAvailable(ctx context.Context, id uint64, amount decimal.Decimal) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE accounts SET available = available - ?, frozen_margin = frozen_margin + ? WHERE id = ? AND available >= ?`,
		amount, amount, id, amount)
	return affected(res, err)
}

// FreezeSpillToCredit available单独不够、但available+credit够时的中间路径：available里
// "属于自己的正数部分"全部冻结完，缺口从credit冻结(frozen_credit记账)。用GREATEST(available,0)
// 而不是裸的available，是因为这个方法可能在available已经因为FreezeForceIntoNegative变负
// 之后被调用——这种情况下available没有"正数部分"可以贡献，全部缺口应该整笔从credit冻结，
// 不能让负的available把frozen_margin/frozen_credit的计算搅乱。用一条UPDATE原子完成，SET里
// 引用的available/credit都是这一行更新前的值(available的赋值放在最后一条子句，前面几条
// 子句引用的都还是原始值，MySQL对同一条UPDATE语句的SET子句是从左到右求值、后面的子句会
// 看到前面已经写入的新值——这一点在deductWithCreditFallback上踩过坑，这里的写法没有这个
// 问题是因为available的重新赋值特意放在最后)。
//
// 返回值里的(fromAvailable, fromCredit)必须是这条UPDATE语句真正应用的那个拆分，不能在
// 调用方另外读一次available/credit、事后拿这份读到的快照去反推——两次读写之间账户可能被
// 别的请求并发改过，反推出来的拆分会跟这条UPDATE实际写入的frozen_margin/frozen_credit不一致，
// 导致这笔委托记录的来源比例和账户里真实冻结的来源比例对不上，后续撤单/成交按这个错误比例
// 释放，会让frozen_margin/frozen_credit这两个跨订单共享的资金池分账逐渐失衡。做法是用
// MySQL的用户变量在UPDATE语句内部把中间计算结果记下来，同一个连接上紧接着SELECT出来——
// 必须用同一个*sql.Conn(而不是db.ExecContext/QueryContext各自可能从连接池拿到不同的物理
// 连接)，用户变量是连接级别的会话状态，换一条连接就读不到/读到别的调用留下的脏值
func (r *AccountRepo) FreezeSpillToCredit(ctx context.Context, id uint64, amount decimal.Decimal) (fromAvailable, fromCredit decimal.Decimal, ok bool, err error) {
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return decimal.Zero, decimal.Zero, false, err
	}
	defer conn.Close()

	res, err := conn.ExecContext(ctx,
		`UPDATE accounts SET
			frozen_margin = frozen_margin + (@perpgo_avail_part := GREATEST(available, 0)),
			frozen_credit = frozen_credit + (@perpgo_credit_part := (? - GREATEST(available, 0))),
			credit = credit - @perpgo_credit_part,
			available = available - @perpgo_avail_part
		 WHERE id = ? AND available < ? AND GREATEST(available, 0) + credit >= ?`,
		amount, id, amount, amount)
	if err != nil {
		return decimal.Zero, decimal.Zero, false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return decimal.Zero, decimal.Zero, false, err
	}
	// 只有上面UPDATE真的影响了一行才能读这两个用户变量——WHERE guard没匹配到行时，
	// SET里的:=赋值表达式根本不会被求值，这时候读到的是这条连接上一次残留的脏值(哪怕
	// 是别的uid的调用留下的)，绝对不能在RowsAffected()==0的时候相信这两个变量
	if err := conn.QueryRowContext(ctx, `SELECT @perpgo_avail_part, @perpgo_credit_part`).
		Scan(&fromAvailable, &fromCredit); err != nil {
		return decimal.Zero, decimal.Zero, false, err
	}
	return fromAvailable, fromCredit, true, nil
}

// FreezeForceIntoNegative available+credit都不够、但账户权益(含持仓浮盈)够覆盖时的
// 最后一条路径：币安式"持仓浮盈也能当买力开新仓"——没有WHERE守卫，调用方已经在service层用
// 未实现盈亏验证过权益足够，这里只是把"允许借用浮盈"这个决定落地，available可能因此变负，
// 全仓模式下这是合法状态(强平穿仓/保险基金垫付走的就是这套)。不动credit：浮盈是不确定、
// 随时可能反转的钱，不应该跟"已经到账的保险赔付"混在一起算作已用掉
// FreezeForceIntoNegative 强制冻结、允许available变负——币安式"持仓浮盈也能当买力开新仓"
// 这条路径本身故意不设available/credit够不够的门槛(允许变负是设计意图，见
// service.AccountService.FreezeMargin)，但WHERE条件核对expectedAvailable/expectedCredit
// 没有被改过：调用方(FreezeMargin)判断"值不值得走这条路径"用的是available+credit+浮盈
// 的合计，其中浮盈来自跨symbol聚合持仓表+标记价格、没法用一条SQL条件表达，只能在Go层
// 算好门槛判断之后再调这个方法——这中间有一个没法完全消除的窗口，但至少可以保证"调用方
// 判断时读到的available/credit"和"这条UPDATE真正要改的available/credit"是同一份，没有
// 在窗口期被另一笔并发操作(比如同一个uid在另一个symbol+side上的FreezeMargin，
// OrderLockKey只按uid+symbol+side加锁，管不到跨symbol的并发)偷偷改过——改过了就返回
// ok=false，调用方按"这次没能安全地冻结"处理(不能假装冻结成功了)
func (r *AccountRepo) FreezeForceIntoNegative(ctx context.Context, id uint64, amount, expectedAvailable, expectedCredit decimal.Decimal) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE accounts SET available = available - ?, frozen_margin = frozen_margin + ?
		 WHERE id = ? AND available = ? AND credit = ?`,
		amount, amount, id, expectedAvailable, expectedCredit)
	return affected(res, err)
}

// UnfreezeMargin 撤单/未成交部分释放冻结的保证金——availableAmount/creditAmount分别对应
// 这笔委托当初从available/credit冻结的比例，必须分开还，不能笼统还到available，否则等于
// 让信用额度经过"冻结再撤单"这个渠道被洗成可提现的available
func (r *AccountRepo) UnfreezeMargin(ctx context.Context, id uint64, availableAmount, creditAmount decimal.Decimal) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE accounts SET
			available = available + ?,
			frozen_margin = frozen_margin - ?,
			credit = credit + ?,
			frozen_credit = frozen_credit - ?
		 WHERE id = ? AND frozen_margin >= ? AND frozen_credit >= ?`,
		availableAmount, availableAmount, creditAmount, creditAmount, id, availableAmount, creditAmount)
	return affected(res, err)
}

// DecreaseFrozenMargin 开仓成交：冻结的保证金转移到仓位(只扣frozen_margin/frozen_credit，
// 全仓下不是真锁定的钱)。availableAmount/creditAmount是这笔成交对应释放的两部分冻结额度，
// 转正之后这两部分该退回available还是credit，由调用方紧接着分别调SettleToAvailable/
// SettleToCredit处理——这里只负责解冻记账，不负责钱最终去哪
func (r *AccountRepo) DecreaseFrozenMargin(ctx context.Context, id uint64, availableAmount, creditAmount decimal.Decimal) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE accounts SET frozen_margin = frozen_margin - ?, frozen_credit = frozen_credit - ?
		 WHERE id = ? AND frozen_margin >= ? AND frozen_credit >= ?`,
		availableAmount, creditAmount, id, availableAmount, creditAmount)
	return affected(res, err)
}

// SettleToAvailable 保证金原样归还——amount可正可负，无守卫(全仓下available允许暂时为负，
// 这是已经接受的合法状态，不是bug)。只用于"退回原来冻结available的那部分"这种明确知道
// 钱该回available的场景，不要用来结算亏损——亏损要走SettlePnl，走先available后credit的顺序
func (r *AccountRepo) SettleToAvailable(ctx context.Context, id uint64, amount decimal.Decimal) error {
	_, err := r.db.ExecContext(ctx, `UPDATE accounts SET available = available + ? WHERE id = ?`, amount, id)
	return err
}

// SettleToCredit 退回原来冻结credit的那部分，跟SettleToAvailable对称
func (r *AccountRepo) SettleToCredit(ctx context.Context, id uint64, amount decimal.Decimal) error {
	_, err := r.db.ExecContext(ctx, `UPDATE accounts SET credit = credit + ? WHERE id = ?`, amount, id)
	return err
}

// SettlePnl 已实现盈亏/强平清算缓冲结算：盈利(amount>=0)直接进available，不动credit——赚的
// 是新钱，没有变现风险。亏损(amount<0)走"先扣available、available里属于自己的正数部分耗尽了
// 再扣credit、credit也耗尽了才让available继续变负"的顺序——让运营发放的信用额度尽量少被
// 真实亏损吃掉，用户自己的钱优先兜底，这是运营侧控制赔付成本的取舍，不是"保护用户"的取舍。
// 这个顺序对手续费扣款(DeductFee)同样适用，两者共用deductWithCreditFallback
func (r *AccountRepo) SettlePnl(ctx context.Context, id uint64, amount decimal.Decimal) error {
	if amount.Sign() >= 0 {
		return r.SettleToAvailable(ctx, id, amount)
	}
	return r.deductWithCreditFallback(ctx, id, amount.Neg())
}

// DeductFee 手续费扣款：这笔手续费对应的成交已经真实发生，不能因为差一点钱扣不出来就不扣，
// 走跟SettlePnl亏损分支同样的"先available后credit"顺序，credit也不够时allowed继续让
// available变负
func (r *AccountRepo) DeductFee(ctx context.Context, id uint64, fee decimal.Decimal) error {
	if fee.Sign() <= 0 {
		return nil
	}
	return r.deductWithCreditFallback(ctx, id, fee)
}

// deductWithCreditFallback 从这个账户扣掉loss(正数)，"先available后credit"：
// absorbedByCredit = min(credit, max(loss - max(available,0), 0))——available里"属于自己的
// 正数部分"(已经是负数就没有可用的部分)覆盖不了的缺口，先问credit要，credit给不了的剩余部分
// 由available兜底(可能变得更负)。这里必须用JOIN一份子查询快照(snap)来引用"更新前"的
// available/credit，不能直接在SET子句里互相引用对方——MySQL对同一条UPDATE语句里的SET
// 子句是按书写顺序从左到右求值的，后面的子句会看到前面子句已经写入的新值，不是这一行
// 更新前的快照(这点在其他一些数据库里可能不成立，但MySQL是这样，之前这里直接互相引用
// 一度写错过，导致credit被多扣，已经用真实MySQL实例验证过这个JOIN写法在多组边界数值下
// 都正确)
func (r *AccountRepo) deductWithCreditFallback(ctx context.Context, id uint64, loss decimal.Decimal) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE accounts a
		 JOIN (SELECT id, available, credit FROM accounts WHERE id = ?) snap ON snap.id = a.id
		 SET
			a.available = a.available - ? + LEAST(snap.credit, GREATEST(? - GREATEST(snap.available, 0), 0)),
			a.credit = a.credit - LEAST(snap.credit, GREATEST(? - GREATEST(snap.available, 0), 0))`,
		id, loss, loss, loss)
	return err
}

// GrantCredit 合作方发放/追加信用额度，直接累加——同一轮内允许多次调用
func (r *AccountRepo) GrantCredit(ctx context.Context, id uint64, amount decimal.Decimal) error {
	_, err := r.db.ExecContext(ctx, `UPDATE accounts SET credit = credit + ? WHERE id = ?`, amount, id)
	return err
}

// SetInsured 合作方单独设置这个账户本轮是否投保，跟发放信用额度是两个独立的动作
func (r *AccountRepo) SetInsured(ctx context.Context, id uint64, insured bool) error {
	_, err := r.db.ExecContext(ctx, `UPDATE accounts SET is_insured = ? WHERE id = ?`, insured, id)
	return err
}

// CloseRoundIfRound 结束本轮：credit清零(没用完的赔付额度不追讨，也不留到下一轮)、
// is_insured重置、round+1，为下一轮做准备。调用前调用方要保证这个uid名下已经没有持仓/
// 挂单(强平/撤单已经在更上层完成)，这里只做资金状态的收尾。多一个"当前round必须等于
// round参数"的原子条件(WHERE id=? AND round=?)——engine分片部署下，多个实例可能各自
// 独立观察到"这个uid的结束本轮所有symbol都处理完了"、都尝试做这最后一步，这个条件保证
// 只有第一个真正推进round的调用生效，返回false表示没有满足条件的行(round已经被别的调用
// 推进过)，是正常情况，不是错误，见docs/engine-sharding.md
//
// 清零前的credit值通过MySQL会话变量在同一条UPDATE里原子捕获后返回(用法跟
// FreezeSpillToCredit一致，见下面注释)，不能先单独SELECT credit再执行这条UPDATE——
// 两次读写之间如果有并发的GrantCredit把credit改大，UPDATE清零的是并发写入后的真实值，
// 但如果审计流水金额用的是UPDATE之前读到的旧值，就会跟实际清零的金额对不上
func (r *AccountRepo) CloseRoundIfRound(ctx context.Context, id, round uint64) (ok bool, clearedCredit decimal.Decimal, err error) {
	// 会话变量是连接级别的状态，必须让UPDATE和后面的SELECT @xxx用同一条物理连接，
	// 否则连接池可能把SELECT分派到另一条从来没执行过这条UPDATE的连接上，读到的是
	// 陌生会话里的旧值/NULL，而不是本次UPDATE刚写入的值
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return false, decimal.Zero, err
	}
	defer conn.Close()

	res, err := conn.ExecContext(ctx,
		`UPDATE accounts SET
			credit = (@perpgo_old_credit := credit) - credit,
			is_insured = 0,
			round = round + 1
		 WHERE id = ? AND round = ?`,
		id, round)
	if err != nil {
		return false, decimal.Zero, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, decimal.Zero, err
	}
	if n == 0 {
		// round已经被别的并发调用推进过，会话变量没有被这条UPDATE写过，不能读
		return false, decimal.Zero, nil
	}
	if err := conn.QueryRowContext(ctx, `SELECT @perpgo_old_credit`).Scan(&clearedCredit); err != nil {
		return false, decimal.Zero, err
	}
	return true, clearedCredit, nil
}

func affected(res sql.Result, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}
