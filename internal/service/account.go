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

var ErrInsufficientMargin = errors.New("可用余额不足，无法冻结保证金")

// 账户不存在。合作方必须先调创建账户接口，其它接口不会替他们悄悄建——否则uid手误写错的
// 充值会成功地充给一个没人认领的账户
var ErrAccountNotFound = repo.ErrAccountNotFound

var (
	// ErrInsufficientBalance 合作方扣减账户余额(POST /account/balance负数)时available不够
	ErrInsufficientBalance = repo.ErrInsufficientBalance
	// ErrIdempotencyConflict 同一个requestId已经用于一笔参数不同的请求
	ErrIdempotencyConflict = repo.ErrIdempotencyConflict
)

type AccountService struct {
	accounts  *repo.AccountRepo
	positions *PositionService
	tx        *repo.TxRepo
}

func NewAccountService(accounts *repo.AccountRepo, positions *PositionService, tx *repo.TxRepo) *AccountService {
	return &AccountService{accounts: accounts, positions: positions, tx: tx}
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

// 合作方资金注入/扣减——amount正数=加钱，负数=扣钱(扣的时候available必须够)。requestId是
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
		AccountID: acc.ID, UID: uid, Kind: kind, Amount: amount.Abs(), TxType: model.TxDeposit,
		RequestID:   requestID,
		RequestHash: RequestFingerprint("balance", strconv.FormatUint(uid, 10), amount.String()),
		Now:         time.Now().UnixMilli(),
	})
}

// 冻结成功后告诉调用方这笔钱分别从available/credit各拿了多少，调用方(下单接口)
// 要把这个拆分记到订单的frozen_margin/frozen_credit上，撤单/成交转正时才能精确退回来源
type FreezeResult struct {
	FromAvailable decimal.Decimal
	FromCredit    decimal.Decimal
}

// 挂单开仓冻结保证金，四级路径依次尝试：
//  1. available够 → 全部从available冻结
//  2. available不够，available+credit够 → 缺口从credit冻结
//  3. 前两级都不够，available+credit+全部持仓未实现盈亏够 → 币安式"持仓浮盈也能当买力
//     开新仓"，强制冻结、允许available变负；不动credit——浮盈不确定，不该跟已经到账的
//     保险赔付混在一起算作已用掉
//  4. 都不够 → 拒绝
func (s *AccountService) FreezeMargin(ctx context.Context, uid uint64, amount decimal.Decimal) (FreezeResult, error) {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return FreezeResult{}, err
	}
	ok, err := s.accounts.FreezeFromAvailable(ctx, account.ID, amount)
	if err != nil {
		return FreezeResult{}, err
	}
	if ok {
		return FreezeResult{FromAvailable: amount}, nil
	}

	freshAvailable, err := s.accounts.FindFreshAvailable(ctx, account.ID)
	if err != nil {
		return FreezeResult{}, err
	}
	freshCredit, err := s.accounts.FindFreshCredit(ctx, account.ID)
	if err != nil {
		return FreezeResult{}, err
	}
	// freshAvailable/freshCredit这两个读数只用来决定"值不值得走这条路径尝试"，不用来
	// 反推实际冻结的available/credit拆分——两次读之间账户可能被并发改过，拆分必须直接
	// 用FreezeSpillToCredit返回的、这条UPDATE语句自己算出来的真实值，见该方法的注释
	if freshAvailable.Add(freshCredit).GreaterThanOrEqual(amount) {
		fromAvailable, fromCredit, ok, err := s.accounts.FreezeSpillToCredit(ctx, account.ID, amount)
		if err != nil {
			return FreezeResult{}, err
		}
		if ok {
			return FreezeResult{FromAvailable: fromAvailable, FromCredit: fromCredit}, nil
		}
	}

	totalUnrealized, err := s.positions.TotalUnrealizedPnl(ctx, uid)
	if err != nil {
		return FreezeResult{}, err
	}
	// 浮盈是跨symbol聚合持仓表+标记价格算出来的，没法像available/credit那样表达成一条
	// SQL条件、交给数据库原子核对——这里能做的是在真正调用FreezeForceIntoNegative之前，
	// 尽量贴近地重新读一次available/credit(不复用freshAvailable/freshCredit这两个更早
	// 读到的、可能已经过期的值)，缩小"判断当时的账户状态"和"真正冻结时的账户状态"之间的
	// 窗口——不能完全消除(totalUnrealized本身没法原子核对)，但比复用一个更旧的快照强
	freshAvailable, err = s.accounts.FindFreshAvailable(ctx, account.ID)
	if err != nil {
		return FreezeResult{}, err
	}
	freshCredit, err = s.accounts.FindFreshCredit(ctx, account.ID)
	if err != nil {
		return FreezeResult{}, err
	}
	if freshAvailable.Add(freshCredit).Add(totalUnrealized).GreaterThanOrEqual(amount) {
		ok, err := s.accounts.FreezeForceIntoNegative(ctx, account.ID, amount, freshAvailable, freshCredit)
		if err != nil {
			return FreezeResult{}, err
		}
		if ok {
			return FreezeResult{FromAvailable: amount}, nil
		}
		// available/credit在"读出来判断够不够"和"真正冻结"这两步之间被别的并发操作改过了
		// (比如同一个uid在另一个symbol+side上也在走FreezeMargin，OrderLockKey管不到跨
		// symbol的并发)，不能假装冻结成功——按"这次没能安全地冻结"处理，让调用方走正常的
		// 余额不足拒绝路径，不重试(重试的复杂度收益不成比例，极端并发下的这类边界情况
		// 交给用户重新发起请求即可)
	}
	return FreezeResult{}, ErrInsufficientMargin
}

// 撤单/未成交部分释放冻结的保证金，availableAmount/creditAmount分别是
// 这笔委托当初从available/credit冻结的比例，必须分开还
func (s *AccountService) UnfreezeMargin(ctx context.Context, uid uint64, availableAmount, creditAmount decimal.Decimal) error {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return err
	}
	ok, err := s.accounts.UnfreezeMargin(ctx, account.ID, availableAmount, creditAmount)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("冻结保证金不足")
	}
	return nil
}

// 开仓成交：冻结的保证金转移到仓位记账，availableAmount/creditAmount
// 是这笔成交对应释放的两部分——调用方紧接着要分别把这两部分还回available/credit
// (全仓下position_margin只是记账用的名义值，不需要真的搬钱)
func (s *AccountService) DecreaseFrozenMargin(ctx context.Context, uid uint64, availableAmount, creditAmount decimal.Decimal) error {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return err
	}
	ok, err := s.accounts.DecreaseFrozenMargin(ctx, account.ID, availableAmount, creditAmount)
	if err != nil {
		return err
	}
	if !ok {
		// 跟UnfreezeMargin同样的守卫失败处理：不能吞掉这个错误——调用方(settlement.go)
		// 紧接着会把"释放的这部分"退回available/credit，如果这里的frozen_margin/
		// frozen_credit扣减实际没生效却假装成功，会让这笔钱同时留在frozen里又被当成
		// 已释放退回来源，变成平白多出来的钱
		return errors.New("冻结保证金不足，无法转移到仓位")
	}
	return nil
}

// 保证金原样归还到available——只用于明确知道钱该回available的场景
// (比如开仓成交后归还冻结时属于available的那一份)，不要用来结算亏损
func (s *AccountService) SettleToAvailable(ctx context.Context, uid uint64, amount decimal.Decimal) error {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return err
	}
	return s.accounts.SettleToAvailable(ctx, account.ID, amount)
}

// 跟SettleToAvailable对称，归还冻结时属于credit的那一份
func (s *AccountService) SettleToCredit(ctx context.Context, uid uint64, amount decimal.Decimal) error {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return err
	}
	return s.accounts.SettleToCredit(ctx, account.ID, amount)
}

// 已实现盈亏/强平清算缓冲结算，可正可负：盈利只进available；亏损先扣available、
// 扣完了再扣credit——运营发放的信用额度尽量少被真实亏损吃掉，是控制赔付成本的取舍
func (s *AccountService) SettlePnl(ctx context.Context, uid uint64, amount decimal.Decimal) error {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return err
	}
	return s.accounts.SettlePnl(ctx, account.ID, amount)
}

// 扣手续费，跟SettlePnl的亏损分支同样的"先available后credit"顺序
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

// 最新可用余额
func (s *AccountService) FindFreshAvailable(ctx context.Context, uid uint64) (decimal.Decimal, error) {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return decimal.Zero, err
	}
	return s.accounts.FindFreshAvailable(ctx, account.ID)
}

// 最新信用额度余额——强平联合判断用，理由跟FindFreshAvailable一样：不能读缓存的account实体，
// 要绕开一级缓存读最新值
func (s *AccountService) FindFreshCredit(ctx context.Context, uid uint64) (decimal.Decimal, error) {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return decimal.Zero, err
	}
	return s.accounts.FindFreshCredit(ctx, account.ID)
}

// 合作方发放/追加信用额度(用户买保险后的赔付)，同一轮内可以多次调用、直接累加。requestId的
// 语义同AdjustBalance——发额度是累加操作，重试不带幂等键会让信用额度翻倍
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
	return s.accounts.ApplyFundOp(ctx, repo.FundOp{
		AccountID: account.ID, UID: uid, Kind: repo.FundOpGrantCredit, Amount: amount, TxType: model.TxCreditGrant,
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

// 结束本轮的资金收尾：credit清零(没用完的赔付额度不追讨)、is_insured重置、
// round+1。调用前必须已经没有持仓/挂单——这里只做资金状态收尾，强平仓位/撤销挂单由
// 更上层的编排负责(见EngineService.CloseRound)。round是调用方预期的"当前轮次"，只有
// account当前round还是这个值才会真的执行，返回false表示round已经被别的调用推进过了
// (engine分片部署下多个实例可能都观察到"这个uid可以结算了"，见docs/engine-sharding.md)，
// 这种情况不是错误，调用方应该当no-op处理，不能重复插入round-close的资金流水记录
func (s *AccountService) CloseRound(ctx context.Context, uid, round uint64) (bool, error) {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return false, err
	}
	// 清零前的credit值由CloseRoundIfRound在同一条UPDATE里原子捕获返回，不在这里单独
	// 预读——预读的话，跟并发的GrantCredit之间有竞态，审计流水金额可能跟实际清零的
	// 金额对不上，见account_repo.go里CloseRoundIfRound的注释
	ok, clearedCredit, err := s.accounts.CloseRoundIfRound(ctx, account.ID, round)
	if err != nil || !ok {
		return ok, err
	}
	if clearedCredit.Sign() > 0 {
		if err := s.tx.Insert(ctx, uid, "USDT", model.TxRoundClose, clearedCredit.Neg(), time.Now().UnixMilli()); err != nil {
			return true, err
		}
	}
	return true, nil
}

// 查询接口用：账户原始字段+现算的未实现盈亏/权益
type AccountView struct {
	UID                uint64          `json:"uid"`
	IsInsured          bool            `json:"isInsured"`
	Status             string          `json:"status"`
	Round              uint64          `json:"round"`
	Credit             decimal.Decimal `json:"credit"`
	Available          decimal.Decimal `json:"available"`
	FrozenMargin       decimal.Decimal `json:"frozenMargin"`
	FrozenCredit       decimal.Decimal `json:"frozenCredit"`
	TotalUnrealizedPnl decimal.Decimal `json:"totalUnrealizedPnl"`
	Equity             decimal.Decimal `json:"equity"`
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
	return &AccountView{
		UID:                uid,
		IsInsured:          acc.IsInsured,
		Status:             string(acc.Status),
		Round:              acc.Round,
		Credit:             acc.Credit,
		Available:          acc.Available,
		FrozenMargin:       acc.FrozenMargin,
		FrozenCredit:       acc.FrozenCredit,
		TotalUnrealizedPnl: total,
		Equity:             acc.Available.Add(acc.Credit).Add(total),
	}, nil
}
