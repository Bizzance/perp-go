package service

import (
	"context"
	"errors"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
	"perp-go/internal/repo"
)

var ErrInsufficientMargin = errors.New("可用余额不足，无法冻结保证金")

type AccountService struct {
	accounts  *repo.AccountRepo
	positions *PositionService
	tx        *repo.TxRepo
}

func NewAccountService(accounts *repo.AccountRepo, positions *PositionService, tx *repo.TxRepo) *AccountService {
	return &AccountService{accounts: accounts, positions: positions, tx: tx}
}

func (s *AccountService) GetOrCreate(ctx context.Context, uid uint64) (*model.Account, error) {
	return s.accounts.GetOrCreate(ctx, uid)
}

// 合作方资金注入/扣减——amount正数=加钱，负数=扣钱
// 由调用方(handler)在鉴权中间件补上之前先用明文uid参数占位，见plan文件
func (s *AccountService) AdjustBalance(ctx context.Context, uid uint64, amount decimal.Decimal) error {
	if amount.IsZero() {
		return errors.New("调整金额不能为0")
	}
	acc, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return err
	}
	if err := s.accounts.SettleToAvailable(ctx, acc.ID, amount); err != nil {
		return err
	}
	return s.tx.Insert(ctx, uid, "USDT", model.TxDeposit, amount, time.Now().UnixMilli())
}

// 挂单开仓冻结保证金：available够就直接冻结；
// 不够时看"available+全部持仓未实现盈亏"够不够——币安式"持仓浮盈也能当买力开新仓"，够就强制冻结、允许available变负。
func (s *AccountService) FreezeMargin(ctx context.Context, uid uint64, amount decimal.Decimal) error {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return err
	}
	ok, err := s.accounts.FreezeFromAvailable(ctx, account.ID, amount)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	fresh, err := s.accounts.FindFreshAvailable(ctx, account.ID)
	if err != nil {
		return err
	}
	totalUnrealized, err := s.positions.TotalUnrealizedPnl(ctx, uid)
	if err != nil {
		return err
	}
	if fresh.Add(totalUnrealized).GreaterThanOrEqual(amount) {
		return s.accounts.FreezeForceIntoNegative(ctx, account.ID, amount)
	}
	return ErrInsufficientMargin
}

// 解冻保证金
func (s *AccountService) UnfreezeMargin(ctx context.Context, uid uint64, amount decimal.Decimal) error {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return err
	}
	ok, err := s.accounts.UnfreezeMargin(ctx, account.ID, amount)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("冻结保证金不足")
	}
	return nil
}

// 减少冻结保证金
func (s *AccountService) DecreaseFrozenMargin(ctx context.Context, uid uint64, amount decimal.Decimal) error {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return err
	}
	_, err = s.accounts.DecreaseFrozenMargin(ctx, account.ID, amount)
	return err
}

// 结算
func (s *AccountService) SettleToAvailable(ctx context.Context, uid uint64, amount decimal.Decimal) error {
	account, err := s.accounts.GetOrCreate(ctx, uid)
	if err != nil {
		return err
	}
	return s.accounts.SettleToAvailable(ctx, account.ID, amount)
}

// 扣手续费
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

// AccountView 查询接口用：账户原始字段+现算的未实现盈亏/权益
type AccountView struct {
	UID                uint64          `json:"uid"`
	Available          decimal.Decimal `json:"available"`
	FrozenMargin       decimal.Decimal `json:"frozenMargin"`
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
		Available:          acc.Available,
		FrozenMargin:       acc.FrozenMargin,
		TotalUnrealizedPnl: total,
		Equity:             acc.Available.Add(total),
	}, nil
}
