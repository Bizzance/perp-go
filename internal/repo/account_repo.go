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
		`SELECT id, uid, is_insured, round, credit, balance, frozen_margin, frozen_credit, version,
		        status, status_reason, status_time
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
	_, err := r.db.ExecContext(ctx, `INSERT IGNORE INTO accounts (uid, balance, frozen_margin) VALUES (?, 0, 0)`, uid)
	if err != nil {
		return nil, err
	}
	return r.FindByUID(ctx, uid)
}

// 创建账户，已经存在就直接返回已有的那一行(不改任何字段)。created表示这次是不是真的新建了：
// INSERT IGNORE撞了uid唯一约束时受影响行数是0，并发下多个请求同时创建同一个uid，只有一个
// 会得到created=true
func (r *AccountRepo) CreateIfAbsent(ctx context.Context, uid uint64) (acc *model.Account, created bool, err error) {
	res, err := r.db.ExecContext(ctx, `INSERT IGNORE INTO accounts (uid, balance, frozen_margin) VALUES (?, 0, 0)`, uid)
	if err != nil {
		return nil, false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, false, err
	}
	acc, err = r.FindByUID(ctx, uid)
	return acc, n > 0, err
}

// 最新的"自由"额度——balance/credit这两列是不随冻结变化的总额，真正能拿去开新仓/被扣减的
// 是减掉frozen_margin/frozen_credit之后的部分，这条查询绕开一级缓存的account实体，跟
// FreezeMargin四级路径判断"要不要往下一级尝试"的场景配套用(见该方法注释)
func (r *AccountRepo) FindFreshFreeMargin(ctx context.Context, id uint64) (freeBalance, freeCredit decimal.Decimal, err error) {
	var balance, credit, frozenMargin, frozenCredit decimal.Decimal
	if err := r.db.QueryRowContext(ctx, `SELECT balance, credit, frozen_margin, frozen_credit FROM accounts WHERE id = ?`, id).
		Scan(&balance, &credit, &frozenMargin, &frozenCredit); err != nil {
		return decimal.Zero, decimal.Zero, err
	}
	return balance.Sub(frozenMargin), credit.Sub(frozenCredit), nil
}

// 强制冻结前拍的完整快照，FreezeForceIntoNegative要核对这4列都没被并发改过
func (r *AccountRepo) FindFreshMarginSnapshot(ctx context.Context, id uint64) (balance, credit, frozenMargin, frozenCredit decimal.Decimal, err error) {
	err = r.db.QueryRowContext(ctx, `SELECT balance, credit, frozen_margin, frozen_credit FROM accounts WHERE id = ?`, id).
		Scan(&balance, &credit, &frozenMargin, &frozenCredit)
	return
}

// 快路径：free balance(balance-frozen_margin)单独够用，只加frozen_margin，
// 不动balance——balance是不随冻结变化的总额，见model.Account.Balance
func (r *AccountRepo) FreezeFromBalance(ctx context.Context, id uint64, amount decimal.Decimal) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE accounts SET frozen_margin = frozen_margin + ? WHERE id = ? AND balance - frozen_margin >= ?`,
		amount, id, amount)
	return affected(res, err)
}

// free balance单独不够、但free balance+free credit够时的中间路径：free balance里
// "属于自己的正数部分"全部冻结完，缺口从credit冻结(frozen_credit记账)。用GREATEST(free,0)
// 而不是裸的free balance，是因为这个方法可能在free balance已经因为FreezeForceIntoNegative
// 变负之后被调用——这种情况下free balance没有"正数部分"可以贡献，全部缺口应该整笔从credit冻结，
// 不能让负的free balance把frozen_margin/frozen_credit的计算搅乱。balance/credit本身在这条语句里
// 完全不写(它们是总额，冻结不动它们)，只需要原子地把free balance/free credit这两个"读的时候
// 用来判断"的值和"真正写入frozen_margin/frozen_credit的值"锁在同一条UPDATE里，用MySQL的用户
// 变量把中间计算结果记下来，同一个连接上紧接着SELECT出来——必须用同一个*sql.Conn(而不是
// db.ExecContext/QueryContext各自可能从连接池拿到不同的物理连接)，用户变量是连接级别的会话
// 状态，换一条连接就读不到/读到别的调用留下的脏值
//
// 返回值里的(fromAvailable, fromCredit)必须是这条UPDATE语句真正应用的那个拆分，不能在
// 调用方另外读一次快照、事后拿这份读到的快照去反推——两次读写之间账户可能被别的请求并发
// 改过，反推出来的拆分会跟这条UPDATE实际写入的frozen_margin/frozen_credit不一致，导致这笔
// 委托记录的来源比例和账户里真实冻结的来源比例对不上，后续撤单/成交按这个错误比例释放，
// 会让frozen_margin/frozen_credit这两个跨订单共享的资金池分账逐渐失衡
func (r *AccountRepo) FreezeSpillToCredit(ctx context.Context, id uint64, amount decimal.Decimal) (fromAvailable, fromCredit decimal.Decimal, ok bool, err error) {
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return decimal.Zero, decimal.Zero, false, err
	}
	defer conn.Close()

	res, err := conn.ExecContext(ctx,
		`UPDATE accounts SET
			frozen_margin = frozen_margin + (@perpgo_avail_part := GREATEST(balance - frozen_margin, 0)),
			frozen_credit = frozen_credit + (@perpgo_credit_part := (? - GREATEST(balance - frozen_margin, 0)))
		 WHERE id = ? AND (balance - frozen_margin) < ? AND GREATEST(balance - frozen_margin, 0) + (credit - frozen_credit) >= ?`,
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

// free balance+free credit都不够、但账户权益(含持仓浮盈)够覆盖时的最后一条路径：币安式
// "持仓浮盈也能当买力开新仓"——只加frozen_margin，balance/free balance可能因此变负，全仓
// 模式下这是合法状态(强平穿仓/保险基金垫付走的就是这套)。不动credit：浮盈是不确定、随时
// 可能反转的钱，不应该跟"已经到账的保险赔付"混在一起算作已用掉。
// 这条路径本身故意不设"够不够"的门槛(允许变负是设计意图，见service.AccountService.FreezeMargin)，
// 但WHERE条件核对expected*没有被改过：调用方(FreezeMargin)判断"值不值得走这条路径"用的是
// free balance+free credit+浮盈的合计，其中浮盈来自跨symbol聚合持仓表+标记价格、没法用一条
// SQL条件表达，只能在Go层算好门槛判断之后再调这个方法——这中间有一个没法完全消除的窗口，但
// 至少可以保证"调用方判断时读到的balance/credit/frozen_margin/frozen_credit"和"这条UPDATE
// 真正要改的这4列"是同一份，没有在窗口期被另一笔并发操作(比如同一个uid在另一个symbol+side
// 上的FreezeMargin，OrderLockKey只按uid+symbol+side加锁，管不到跨symbol的并发；或者另一个
// symbol的仓位刚好在这个窗口期结算了已实现盈亏，改了balance)偷偷改过——改过了就返回
// ok=false，调用方按"这次没能安全地冻结"处理(不能假装冻结成功了)
func (r *AccountRepo) FreezeForceIntoNegative(ctx context.Context, id uint64, amount, expectedBalance, expectedCredit, expectedFrozenMargin, expectedFrozenCredit decimal.Decimal) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE accounts SET frozen_margin = frozen_margin + ?
		 WHERE id = ? AND balance = ? AND credit = ? AND frozen_margin = ? AND frozen_credit = ?`,
		amount, id, expectedBalance, expectedCredit, expectedFrozenMargin, expectedFrozenCredit)
	return affected(res, err)
}

// 释放锁定的保证金(减少frozen_margin/frozen_credit)，不touch balance/credit——这两列是
// 不随冻结变化的总额，见model.Account.Balance。用于撤单/条件单撤销/仓位平仓/降杠杆等
// 场景，availableAmount/creditAmount分别对应当初从balance/credit两侧冻结的比例，必须
// 分开释放，不能笼统释放成一边，否则等于让信用额度那一侧的锁定跟balance那一侧的锁定
// 混起来记账。
//
// 两个参数通常是正数(释放锁定)；唯一的例外是settlement.go开仓成交那一处，把"下单时的
// 保守估算"调整到"按真实成交价算出的properMargin"，如果真实成交价比保守估算更差，delta
// 会是负数，意味着这笔仓位需要补冻更多而不是释放——两种情况用同一条UPDATE处理：负数时
// 守卫条件frozen_margin>=负数恒成立，效果是frozen_margin/frozen_credit增加
func (r *AccountRepo) AdjustFrozenMargin(ctx context.Context, id uint64, availableAmount, creditAmount decimal.Decimal) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE accounts SET frozen_margin = frozen_margin - ?, frozen_credit = frozen_credit - ?
		 WHERE id = ? AND frozen_margin >= ? AND frozen_credit >= ?`,
		availableAmount, creditAmount, id, availableAmount, creditAmount)
	return affected(res, err)
}

// 已实现盈亏/强平清算缓冲结算：盈利(amount>=0)直接进balance，不动credit——赚的
// 是新钱，没有变现风险。亏损(amount<0)走"先扣balance、balance里属于自己的正数部分耗尽了
// 再扣credit、credit也耗尽了才让balance继续变负"的顺序——让运营发放的信用额度尽量少被
// 真实亏损吃掉，用户自己的钱优先兜底，这是运营侧控制赔付成本的取舍，不是"保护用户"的取舍。
// 这个顺序对手续费扣款(DeductFee)同样适用，两者共用deductWithCreditFallback
func (r *AccountRepo) SettlePnl(ctx context.Context, id uint64, amount decimal.Decimal) error {
	if amount.Sign() >= 0 {
		return r.SettleToBalance(ctx, id, amount)
	}
	return r.deductWithCreditFallback(ctx, id, amount.Neg())
}

// 直接改balance——只用于真实改变总资产的场景(已实现盈亏、资金费、ADL划转保险基金这些)，
// amount可正可负，无守卫(全仓下balance允许暂时为负，这是已经接受的合法状态，不是bug)
func (r *AccountRepo) SettleToBalance(ctx context.Context, id uint64, amount decimal.Decimal) error {
	_, err := r.db.ExecContext(ctx, `UPDATE accounts SET balance = balance + ? WHERE id = ?`, amount, id)
	return err
}

// 直接改credit，无守卫——只用于强平穿仓垫付/维持保证金缓冲清算这类"把free credit清零"的
// 场景(见EngineService.settleLiquidationAftermath)，不要用来结算已实现盈亏(走SettlePnl)
func (r *AccountRepo) SettleToCredit(ctx context.Context, id uint64, amount decimal.Decimal) error {
	_, err := r.db.ExecContext(ctx, `UPDATE accounts SET credit = credit + ? WHERE id = ?`, amount, id)
	return err
}

// 手续费扣款：这笔手续费对应的成交已经真实发生，不能因为差一点钱扣不出来就不扣，
// 走跟SettlePnl亏损分支同样的"先balance后credit"顺序，credit也不够时allowed继续让
// balance变负
func (r *AccountRepo) DeductFee(ctx context.Context, id uint64, fee decimal.Decimal) error {
	if fee.Sign() <= 0 {
		return nil
	}
	return r.deductWithCreditFallback(ctx, id, fee)
}

// 从这个账户扣掉loss(正数)，"先balance后credit"：
// absorbedByCredit = min(credit, max(loss - max(balance,0), 0))——balance里"属于自己的
// 正数部分"(已经是负数就没有可用的部分)覆盖不了的缺口，先问credit要，credit给不了的剩余部分
// 由balance兜底(可能变得更负)。这里必须用JOIN一份子查询快照(snap)来引用"更新前"的
// balance/credit，不能直接在SET子句里互相引用对方——MySQL对同一条UPDATE语句里的SET
// 子句是按书写顺序从左到右求值的，后面的子句会看到前面子句已经写入的新值，不是这一行
// 更新前的快照(这点在其他一些数据库里可能不成立，但MySQL是这样，之前这里直接互相引用
// 一度写错过，导致credit被多扣，已经用真实MySQL实例验证过这个JOIN写法在多组边界数值下
// 都正确)
func (r *AccountRepo) deductWithCreditFallback(ctx context.Context, id uint64, loss decimal.Decimal) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE accounts a
		 JOIN (SELECT id, balance, credit FROM accounts WHERE id = ?) snap ON snap.id = a.id
		 SET
			a.balance = a.balance - ? + LEAST(snap.credit, GREATEST(? - GREATEST(snap.balance, 0), 0)),
			a.credit = a.credit - LEAST(snap.credit, GREATEST(? - GREATEST(snap.balance, 0), 0))`,
		id, loss, loss, loss)
	return err
}

// 合作方单独设置这个账户本轮是否投保，跟发放信用额度是两个独立的动作
func (r *AccountRepo) SetInsured(ctx context.Context, id uint64, insured bool) error {
	_, err := r.db.ExecContext(ctx, `UPDATE accounts SET is_insured = ? WHERE id = ?`, insured, id)
	return err
}

// 结束本轮：credit清零(没用完的赔付额度不追讨，也不留到下一轮)、
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
// CloseRoundIfRound 结束本轮：balance/credit都清零——每一轮都是完全独立的资金周期，
// balance这部分对应退给用户的钱由合作方在系统外处理，我们这边只负责把账本清零。
// 清零前的两个值通过MySQL会话变量在同一条UPDATE里原子捕获后返回，不能先单独SELECT再执行
// 这条UPDATE——两次读写之间如果有并发的资金操作(GrantCredit/AdjustBalance/结算)把值改大，
// UPDATE清零的是并发写入后的真实值，但如果审计流水金额用的是UPDATE之前读到的旧值，就会跟
// 实际清零的金额对不上
func (r *AccountRepo) CloseRoundIfRound(ctx context.Context, id, round uint64) (ok bool, clearedCredit, clearedBalance decimal.Decimal, err error) {
	// 会话变量是连接级别的状态，必须让UPDATE和后面的SELECT @xxx用同一条物理连接，
	// 否则连接池可能把SELECT分派到另一条从来没执行过这条UPDATE的连接上，读到的是
	// 陌生会话里的旧值/NULL，而不是本次UPDATE刚写入的值
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return false, decimal.Zero, decimal.Zero, err
	}
	defer conn.Close()

	res, err := conn.ExecContext(ctx,
		`UPDATE accounts SET
			credit = (@perpgo_old_credit := credit) - credit,
			balance = (@perpgo_old_balance := balance) - balance,
			is_insured = 0,
			round = round + 1
		 WHERE id = ? AND round = ?`,
		id, round)
	if err != nil {
		return false, decimal.Zero, decimal.Zero, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, decimal.Zero, decimal.Zero, err
	}
	if n == 0 {
		// round已经被别的并发调用推进过，会话变量没有被这条UPDATE写过，不能读
		return false, decimal.Zero, decimal.Zero, nil
	}
	if err := conn.QueryRowContext(ctx, `SELECT @perpgo_old_credit, @perpgo_old_balance`).Scan(&clearedCredit, &clearedBalance); err != nil {
		return false, decimal.Zero, decimal.Zero, err
	}
	return true, clearedCredit, clearedBalance, nil
}

func affected(res sql.Result, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

type FundOpKind int

const (
	FundOpDeposit     FundOpKind = iota // 增加可用余额
	FundOpWithdraw                      // 扣减可用余额，余额不够就失败
	FundOpGrantCredit                   // 增加信用额度
)

// 合作方发起的一笔资金操作(充值/扣减/发放信用额度)
type FundOp struct {
	AccountID   uint64
	UID         uint64
	Kind        FundOpKind
	Amount      decimal.Decimal // 恒为正数，方向由Kind决定
	TxType      string          // 记进资金流水的类型，见model.Tx*常量
	RequestID   string          // 幂等键，同一uid内唯一
	RequestHash string          // 请求参数摘要，同一个RequestID再次提交时用来判断参数是否一致
	Now         int64
}

// 在一个事务里完成"占位幂等键(写流水)+改余额"，返回replayed=true表示这个requestId之前已经
// 成功处理过、这次什么都没做。两步必须在同一个事务里：先落流水再改余额，中间崩溃会让流水
// 记了钱却没动(重试还会被当成重复请求吞掉，钱永远补不上)；先改余额再落流水，中间崩溃
// 重试会再改一次余额(重复入账)。放进同一个事务，要么都生效要么都不生效
//
// 并发的同一个requestId靠(uid, request_id)唯一索引串行化：后到的INSERT会一直等到先到的
// 事务提交或回滚，提交了就撞唯一索引、按重放处理，回滚了(比如余额不足)就自己接着往下执行
func (r *AccountRepo) ApplyFundOp(ctx context.Context, op FundOp) (replayed bool, err error) {
	// 几个请求带着同一个requestId并发提交、而先到的那个又因为余额不足回滚时，后面几个会同时拿到
	// 唯一索引冲突上的共享锁、再一起尝试升级成排他锁，InnoDB会判定死锁并回滚其中一个。受害者
	// 整个事务重试即可(重试时先到的已经回滚干净，或者已经提交、按重放处理)，不能把这个内部
	// 细节当成500抛给合作方
	const maxAttempts = 3
	for attempt := 1; ; attempt++ {
		replayed, err = r.applyFundOpOnce(ctx, op)
		if err != nil && IsDeadlock(err) && attempt < maxAttempts {
			continue
		}
		return replayed, err
	}
}

func (r *AccountRepo) applyFundOpOnce(ctx context.Context, op FundOp) (replayed bool, err error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return false, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// 先锁住这个账户行，让同一个账户的资金操作串行执行。不加这一步，并发的同一个requestId
	// 会在(uid, request_id)唯一索引上互相等待，先到的回滚(比如余额不足)时后面几个会同时
	// 拿到共享锁再一起升级成排他锁，InnoDB判定死锁、随机回滚几个。资金操作本来就低频，串行
	// 的代价可以忽略；后面的重试只是兜底
	var lockedID uint64
	if err := tx.GetContext(ctx, &lockedID, `SELECT id FROM accounts WHERE id = ? FOR UPDATE`, op.AccountID); err != nil {
		return false, err
	}

	ledgerAmount := op.Amount
	if op.Kind == FundOpWithdraw {
		ledgerAmount = op.Amount.Neg()
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO member_transactions (uid, symbol, amount, type, create_time, request_id, request_hash)
		 VALUES (?, 'USDT', ?, ?, ?, ?, ?)`,
		op.UID, ledgerAmount, op.TxType, op.Now, op.RequestID, op.RequestHash)
	if err != nil {
		if !IsDuplicateKey(err) {
			return false, err
		}
		// 撞了唯一索引：这个requestId之前已经成功处理过。参数一致按重放返回，不一致是误用
		_ = tx.Rollback()
		committed = true
		var storedHash *string
		if err := r.db.GetContext(ctx, &storedHash,
			`SELECT request_hash FROM member_transactions WHERE uid = ? AND request_id = ?`, op.UID, op.RequestID); err != nil {
			return false, err
		}
		if storedHash != nil && *storedHash != op.RequestHash {
			return false, ErrIdempotencyConflict
		}
		return true, nil
	}

	var res sql.Result
	switch op.Kind {
	case FundOpDeposit:
		res, err = tx.ExecContext(ctx, `UPDATE accounts SET balance = balance + ? WHERE id = ?`, op.Amount, op.AccountID)
	case FundOpWithdraw:
		// 只能扣balance里"自由"的那部分(balance-frozen_margin)——锁在挂单/持仓里的钱不能被
		// 合作方这个资金接口直接划走，不然等于绕过冻结机制凭空抽走用户仓位的保证金
		res, err = tx.ExecContext(ctx,
			`UPDATE accounts SET balance = balance - ? WHERE id = ? AND balance - frozen_margin >= ?`, op.Amount, op.AccountID, op.Amount)
	case FundOpGrantCredit:
		res, err = tx.ExecContext(ctx, `UPDATE accounts SET credit = credit + ? WHERE id = ?`, op.Amount, op.AccountID)
	default:
		return false, errors.New("未知的资金操作类型")
	}
	if err != nil {
		return false, err
	}
	if n, err := res.RowsAffected(); err != nil {
		return false, err
	} else if n == 0 {
		// 只有扣减的WHERE balance-frozen_margin >= ?会走到这里(充值/发额度按id更新一定命中)，
		// 回滚掉刚才写的流水，这个requestId没有被占用，补足余额后可以用同一个requestId重试
		return false, ErrInsufficientBalance
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	committed = true
	return false, nil
}

// 只查账户状态，撮合热路径上每笔开仓委托都要查一次，比FindByUID少读大部分列。账户不存在返回空串
func (r *AccountRepo) FindStatus(ctx context.Context, uid uint64) (model.AccountStatus, error) {
	var status model.AccountStatus
	err := r.db.GetContext(ctx, &status, `SELECT status FROM accounts WHERE uid = ?`, uid)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return status, err
}

// 设置账户状态并记一行变更历史，在一个事务里完成。返回变更前的状态和这次有没有真的变化：
// 已经是目标状态就什么都不改、不记历史(设为目标值，天然幂等)，changed=false。事务里先
// SELECT ... FOR UPDATE锁住账户行，让同一个账户的并发状态变更串行，"读当前状态"和"写新状态"
// 之间不会被另一个变更插进来
func (r *AccountRepo) SetStatus(ctx context.Context, uid uint64, to model.AccountStatus, reason, operator string, now int64) (from model.AccountStatus, changed bool, err error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := tx.GetContext(ctx, &from, `SELECT status FROM accounts WHERE uid = ? FOR UPDATE`, uid); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, ErrAccountNotFound
		}
		return "", false, err
	}
	if from == to {
		return from, false, nil
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE accounts SET status = ?, status_reason = ?, status_time = ? WHERE uid = ?`, to, reason, now, uid); err != nil {
		return "", false, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO account_status_history (uid, from_status, to_status, reason, operator, create_time)
		 VALUES (?, ?, ?, ?, ?, ?)`, uid, from, to, reason, operator, now); err != nil {
		return "", false, err
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	committed = true
	return from, true, nil
}
