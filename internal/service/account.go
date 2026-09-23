package service

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
	"perp-go/internal/repo"
)

var (
	ErrInsufficientMargin = errors.New("可用余额不足，无法冻结保证金")
	// 账户不存在。合作方必须先调创建账户接口，其它接口不会替他们悄悄建
	ErrAccountNotFound = repo.ErrAccountNotFound
	// 合作方扣减账户余额(POST /account/balance负数)时自由余额(balance-frozenMargin)不够
	ErrInsufficientBalance = repo.ErrInsufficientBalance
	// 同一个requestId已经用于一笔参数不同的请求
	ErrIdempotencyConflict = repo.ErrIdempotencyConflict
	// 账户本轮没有投保(is_insured=false)，不能发放信用额度——信用额度就是投保后的赔付，没投保就不该有这笔钱
	ErrAccountNotInsured = errors.New("账户本轮没有投保，不能发放信用额度")
)

type AccountService struct {
	accounts  *repo.AccountRepo
	positions *PositionService
	tx        *repo.TxRepo
}

func NewAccountService(accounts *repo.AccountRepo, positions *PositionService, tx *repo.TxRepo) *AccountService {
	return &AccountService{
		accounts:  accounts,
		positions: positions,
		tx:        tx,
	}
}

// 设置账户状态(冻结/解冻)，返回变更前的状态和这次有没有真的变化，幂等：已经是目标状态就不改
// 也不记历史。只负责改状态本身，冻结后清理存量的开仓委托/条件单由调用方(API层)负责——
// 那需要往Kafka发撤单事件，这一层碰不到
func (s *AccountService) SetStatus(ctx context.Context, uid uint64, to model.AccountStatus, reason, operator string) (model.AccountStatus, bool, error) {
	return s.accounts.SetStatus(ctx, uid, to, reason, operator, NowMillis())
}

// 账户当前是不是冻结状态。账户不存在按未冻结处理(调用方各自有账户存在性检查)，
// 数据库出错返回error，调用方按失败关闭处理，不能当成"没冻结"放行
func (s *AccountService) IsFrozen(ctx context.Context, uid uint64) (bool, error) {
	status, err := s.accounts.FindStatus(ctx, uid)
	if err != nil {
		return false, err
	}
	return status == model.AccountStatusFrozen, nil
}

// 按uid查账户，不存在返回(nil, nil)，不会创建
func (s *AccountService) Find(ctx context.Context, uid uint64) (*model.Account, error) {
	return s.accounts.FindByUID(ctx, uid)
}

// 创建账户，已存在就返回已有账户(created=false)——uid是合作方自己体系里的用户ID，重复创建
// 天然幂等，超时重试没有风险
func (s *AccountService) Create(ctx context.Context, uid uint64) (view *AccountView, created bool, err error) {
	_, created, err = s.accounts.CreateIfAbsent(ctx, uid)
	if err != nil {
		return nil, false, err
	}
	view, err = s.View(ctx, uid)
	return view, created, err
}

func (s *AccountService) GetOrCreate(ctx context.Context, uid uint64) (*model.Account, error) {
	return s.accounts.GetOrCreate(ctx, uid)
}

// 合作方资金注入/扣减——amount正数=加钱，负数=扣钱(扣的时候自由余额必须够)。requestId是
// 必填的幂等键：同一个uid重复提交同一个requestId只会生效一次，replayed=true表示这次是重放、
// 什么都没做；同一个requestId带了不同金额返回ErrIdempotencyConflict。
// 由调用方(handler)在鉴权中间件补上之前先用明文uid参数占位，见docs/auth-design.md
func (s *AccountService) AdjustBalance(ctx context.Context, uid uint64, amount decimal.Decimal, requestID string) (replayed bool, err error) {
	if amount.IsZero() {
		return false, errors.New("调整金额不能为0")
	}
	acc, err := s.accounts.FindByUID(ctx, uid)
	if err != nil {
		return false, err
	}
	if acc == nil {
		return false, ErrAccountNotFound
	}
	kind := repo.FundOpDeposit
	if amount.Sign() < 0 {
		kind = repo.FundOpWithdraw
	}
	return s.accounts.ApplyFundOp(ctx, repo.FundOp{
		AccountID:   acc.ID,
		UID:         uid,
		Kind:        kind,
		Amount:      amount.Abs(),
		TxType:      model.TxDeposit,
		RequestID:   requestID,
		RequestHash: RequestFingerprint("balance", strconv.FormatUint(uid, 10), amount.String()),
		Now:         time.Now().UnixMilli(),
	})
}

// 冻结成功后告诉调用方这笔钱分别从balance/credit各拿了多少，调用方(下单接口)
// 要把这个拆分记到订单的frozen_margin/frozen_credit上，撤单/成交转正时才能精确退回来源
type FreezeResult struct {
	FromAvailable decimal.Decimal
	FromCredit    decimal.Decimal
}

// 挂单开仓冻结保证金。balance/credit是不随冻结变化的总额(见model.Account.Balance)，"自由"
// 的部分是减掉frozen_margin/frozen_credit之后的差额，下面统称free balance/free credit。
// 先做一道买力预检：账户有浮亏时，买力是free balance+free credit减掉浮亏，不够就直接拒绝，
// 不管free balance本身够不够——币安的可用余额=钱包余额-初始保证金+未实现盈亏，浮亏直接减少
// 可用余额，OKX也是从计入未实现盈亏的调整后权益算起。不这样的话，账户浮亏累累甚至已经满足
// 强平条件，只要free balance还是正数就能继续开新仓；反过来，账户进入强平条件时(权益<=维持
// 保证金<初始保证金)买力必然为负，新开仓自然被拒，不需要单独加"强平期间拒绝新单"的规则。
// 浮盈不在预检里放宽，只在下面的第3级才能当买力。预检之后，四级路径依次尝试：
//  1. free balance够 → 全部从balance冻结
//  2. free balance不够，free balance+free credit够 → 缺口从credit冻结
//  3. 前两级都不够，free balance+free credit+全部持仓未实现盈亏够 → 币安式"持仓浮盈也能当
//     买力开新仓"，强制冻结、允许free balance变负；不动credit——浮盈不确定，不该跟已经到账的
//     保险赔付混在一起算作已用掉
//  4. 都不够 → 拒绝
func (s *AccountService) FreezeMargin(ctx context.Context, uid uint64, amount decimal.Decimal) (FreezeResult, error) {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return FreezeResult{}, err
	}
	totalUnrealized, err := s.positions.TotalUnrealizedPnl(ctx, uid)
	if err != nil {
		return FreezeResult{}, err
	}
	freeBalance := account.Balance.Sub(account.FrozenMargin)
	freeCredit := account.Credit.Sub(account.FrozenCredit)
	if totalUnrealized.Sign() < 0 && freeBalance.Add(freeCredit).Add(totalUnrealized).LessThan(amount) {
		return FreezeResult{}, ErrInsufficientMargin
	}
	ok, err := s.accounts.FreezeFromBalance(ctx, account.ID, amount)
	if err != nil {
		return FreezeResult{}, err
	}
	if ok {
		return FreezeResult{FromAvailable: amount}, nil
	}

	freshFreeBalance, freshFreeCredit, err := s.accounts.FindFreshFreeMargin(ctx, account.ID)
	if err != nil {
		return FreezeResult{}, err
	}
	// freshFreeBalance/freshFreeCredit这两个读数只用来决定"值不值得走这条路径尝试"，不用来
	// 反推实际冻结的balance/credit拆分——两次读之间账户可能被并发改过，拆分必须直接
	// 用FreezeSpillToCredit返回的、这条UPDATE语句自己算出来的真实值，见该方法的注释
	if freshFreeBalance.Add(freshFreeCredit).GreaterThanOrEqual(amount) {
		fromAvailable, fromCredit, ok, err := s.accounts.FreezeSpillToCredit(ctx, account.ID, amount)
		if err != nil {
			return FreezeResult{}, err
		}
		if ok {
			return FreezeResult{FromAvailable: fromAvailable, FromCredit: fromCredit}, nil
		}
	}

	// 浮盈是跨symbol聚合持仓表+标记价格算出来的，没法像balance/credit那样表达成一条
	// SQL条件、交给数据库原子核对——这里能做的是在真正调用FreezeForceIntoNegative之前，
	// 尽量贴近地重新读一次快照(不复用上面更早读到的、可能已经过期的值)，缩小"判断当时的
	// 账户状态"和"真正冻结时的账户状态"之间的窗口——不能完全消除(totalUnrealized本身
	// 没法原子核对)，但比复用一个更旧的快照强
	freshBalance, freshCredit, freshFrozenMargin, freshFrozenCredit, err := s.accounts.FindFreshMarginSnapshot(ctx, account.ID)
	if err != nil {
		return FreezeResult{}, err
	}
	if freshBalance.Sub(freshFrozenMargin).Add(freshCredit.Sub(freshFrozenCredit)).Add(totalUnrealized).GreaterThanOrEqual(amount) {
		ok, err := s.accounts.FreezeForceIntoNegative(ctx, account.ID, amount, freshBalance, freshCredit, freshFrozenMargin, freshFrozenCredit)
		if err != nil {
			return FreezeResult{}, err
		}
		if ok {
			return FreezeResult{FromAvailable: amount}, nil
		}
		// balance/credit/frozen_margin/frozen_credit在"读出来判断够不够"和"真正冻结"这两步
		// 之间被别的并发操作改过了(比如同一个uid在另一个symbol+side上也在走FreezeMargin，
		// OrderLockKey管不到跨symbol的并发)，不能假装冻结成功——按"这次没能安全地冻结"处理，
		// 让调用方走正常的余额不足拒绝路径，不重试(重试的复杂度收益不成比例，极端并发下的
		// 这类边界情况交给用户重新发起请求即可)
	}
	return FreezeResult{}, ErrInsufficientMargin
}

// 释放锁定的保证金(减少frozen_margin/frozen_credit)，availableAmount/creditAmount分别是
// 这笔委托当初从balance/credit冻结的比例，必须分开还。撤单/条件单撤销/平仓/降杠杆共用
func (s *AccountService) UnfreezeMargin(ctx context.Context, uid uint64, availableAmount, creditAmount decimal.Decimal) error {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return err
	}
	ok, err := s.accounts.AdjustFrozenMargin(ctx, account.ID, availableAmount, creditAmount)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("冻结保证金不足")
	}
	return nil
}

// 保证金原样改balance——只用于真实改变总资产的场景(资金费、ADL划转保险基金这些)，
// 不要用来结算已实现盈亏(走SettlePnl，那条路径是先balance后credit)
func (s *AccountService) SettleToBalance(ctx context.Context, uid uint64, amount decimal.Decimal) error {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return err
	}
	return s.accounts.SettleToBalance(ctx, account.ID, amount)
}

// 跟SettleToBalance对称，直接改credit——只用于强平穿仓垫付/维持保证金缓冲清算这类场景
func (s *AccountService) SettleToCredit(ctx context.Context, uid uint64, amount decimal.Decimal) error {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return err
	}
	return s.accounts.SettleToCredit(ctx, account.ID, amount)
}

// 已实现盈亏/强平清算缓冲结算，可正可负：盈利只进balance；亏损先扣balance、
// 扣完了再扣credit——运营发放的信用额度尽量少被真实亏损吃掉，是控制赔付成本的取舍
func (s *AccountService) SettlePnl(ctx context.Context, uid uint64, amount decimal.Decimal) error {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return err
	}
	return s.accounts.SettlePnl(ctx, account.ID, amount)
}

// 扣手续费，跟SettlePnl的亏损分支同样的"先balance后credit"顺序
func (s *AccountService) DeductFee(ctx context.Context, uid uint64, fee decimal.Decimal) error {
	if fee.Sign() <= 0 {
		return nil
	}
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return err
	}
	return s.accounts.DeductFee(ctx, account.ID, fee)
}

// 最新的自由余额/自由信用额度(balance-frozen_margin / credit-frozen_credit)——强平联合
// 判断用，不能读缓存的account实体，要绕开一级缓存读最新值
func (s *AccountService) FindFreshFreeMargin(ctx context.Context, uid uint64) (freeBalance, freeCredit decimal.Decimal, err error) {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return decimal.Zero, decimal.Zero, err
	}
	return s.accounts.FindFreshFreeMargin(ctx, account.ID)
}

// 合作方发放/追加信用额度(用户买保险后的赔付)，同一轮内可以多次调用、直接累加。
func (s *AccountService) GrantCredit(ctx context.Context, uid uint64, amount decimal.Decimal, requestID string) (replayed bool, err error) {
	if amount.Sign() <= 0 {
		return false, errors.New("发放金额必须大于0")
	}
	account, err := s.accounts.FindByUID(ctx, uid)
	if err != nil {
		return false, err
	}
	if account == nil {
		return false, ErrAccountNotFound
	}
	if !account.IsInsured {
		return false, ErrAccountNotInsured
	}
	return s.accounts.ApplyFundOp(ctx, repo.FundOp{
		AccountID:   account.ID,
		UID:         uid,
		Kind:        repo.FundOpGrantCredit,
		Amount:      amount,
		TxType:      model.TxCreditGrant,
		RequestID:   requestID,
		RequestHash: RequestFingerprint("credit", strconv.FormatUint(uid, 10), amount.String()),
		Now:         time.Now().UnixMilli(),
	})
}

// 合作方单独设置这个账户本轮是否投保，跟发放信用额度是两个独立的动作，互不联动
func (s *AccountService) SetInsured(ctx context.Context, uid uint64, insured bool) error {
	account, err := s.accounts.FindByUID(ctx, uid)
	if err != nil {
		return err
	}
	if account == nil {
		return ErrAccountNotFound
	}
	return s.accounts.SetInsured(ctx, account.ID, insured)
}

// 结束本轮的资金收尾：balance/credit都清零(每一轮都是完全独立的资金周期，不跨轮结转)、
// is_insured重置、round+1。balance清零对应"这笔钱该退给用户了"，退款本身是合作方在系统外
// 处理的业务，我们这边只负责把账本归零；credit清零是回收没用完的赔付额度，不追讨。调用前
// 必须已经没有持仓/挂单——这里只做资金状态收尾，强平仓位/撤销挂单由更上层的编排负责(见
// EngineService.CloseRound)。round是调用方预期的"当前轮次"，只有account当前round还是这个
// 值才会真的执行，返回false表示round已经被别的调用推进过了(engine分片部署下多个实例可能
// 都观察到"这个uid可以结算了"，见docs/engine-sharding.md)，这种情况不是错误，调用方应该
// 当no-op处理，不能重复插入round-close的资金流水记录
func (s *AccountService) CloseRound(ctx context.Context, uid, round uint64) (bool, error) {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return false, err
	}
	// 清零前的balance/credit值由CloseRoundIfRound在同一条UPDATE里原子捕获返回，不在这里
	// 单独预读——预读的话，跟并发的资金操作(GrantCredit/结算/AdjustBalance)之间有竞态，
	// 审计流水金额可能跟实际清零的金额对不上，见account_repo.go里CloseRoundIfRound的注释
	ok, clearedCredit, clearedBalance, err := s.accounts.CloseRoundIfRound(ctx, account.ID, round)
	if err != nil || !ok {
		return ok, err
	}
	now := time.Now().UnixMilli()
	if clearedCredit.Sign() > 0 {
		if err := s.tx.Insert(ctx, uid, "USDT", model.TxRoundClose, clearedCredit.Neg(), now); err != nil {
			return true, err
		}
	}
	if clearedBalance.Sign() != 0 {
		if err := s.tx.Insert(ctx, uid, "USDT", model.TxRoundClose, clearedBalance.Neg(), now); err != nil {
			return true, err
		}
	}
	return true, nil
}

// 账户权益(全仓下的保证金余额)：全部属于用户的钱加上浮动盈亏——balance/credit是不随冻结
// 变化的总额(见model.Account.Balance)，不管这笔钱当前是自由的还是锁在挂单/仓位里，都已经
// 算在balance/credit里了，直接加上未实现盈亏就是权益，不需要再额外加frozen_margin/
// frozen_credit/仓位占用的保证金——那样会把已经在balance/credit里的钱重复算一遍。
// 强平判断和账户视图共用这一个口径。买力(开仓够不够钱)另有口径，见FreezeMargin，用的是
// 自由余额，不含已经占用的保证金
func Equity(acc *model.Account, totalUnrealized decimal.Decimal) decimal.Decimal {
	return acc.Balance.Add(acc.Credit).Add(totalUnrealized)
}

// 查询接口用：账户原始字段+现算的未实现盈亏/权益
type AccountView struct {
	UID                uint64          `json:"uid"`                // uid
	IsInsured          bool            `json:"isInsured"`          // 是否投保
	Status             string          `json:"status"`             // 账户状态
	Round              uint64          `json:"round"`              // 轮数
	Credit             decimal.Decimal `json:"credit"`             // 信用额度总额
	Balance            decimal.Decimal `json:"balance"`            // 余额总额
	FrozenMargin       decimal.Decimal `json:"frozenMargin"`       // 来自balance的锁定额(挂单+持仓占用)
	FrozenCredit       decimal.Decimal `json:"frozenCredit"`       // 来自credit的锁定额
	PositionMargin     decimal.Decimal `json:"positionMargin"`     // 全部持仓占用的保证金之和
	TotalUnrealizedPnl decimal.Decimal `json:"totalUnrealizedPnl"` // 未实现盈亏
	Equity             decimal.Decimal `json:"equity"`             // 权益
}

func (s *AccountService) View(ctx context.Context, uid uint64) (*AccountView, error) {
	acc, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return nil, err
	}
	total, err := s.positions.TotalUnrealizedPnl(ctx, uid)
	if err != nil {
		return nil, err
	}
	positionMargin, err := s.positions.TotalPositionMargin(ctx, uid)
	if err != nil {
		return nil, err
	}
	return &AccountView{
		UID:                uid,
		IsInsured:          acc.IsInsured,
		Status:             string(acc.Status),
		Round:              acc.Round,
		Credit:             acc.Credit,
		Balance:            acc.Balance,
		FrozenMargin:       acc.FrozenMargin,
		FrozenCredit:       acc.FrozenCredit,
		PositionMargin:     positionMargin,
		TotalUnrealizedPnl: total,
		Equity:             Equity(acc, total),
	}, nil
}
