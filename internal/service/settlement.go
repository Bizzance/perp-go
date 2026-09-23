package service

import (
	"context"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
	"perp-go/internal/repo"
)

type SettlementService struct {
	accounts  *AccountService
	positions *repo.PositionRepo
	coins     *repo.CoinRepo
	tx        *repo.TxRepo
}

func NewSettlementService(
	accounts *AccountService,
	positions *repo.PositionRepo,
	coins *repo.CoinRepo,
	tx *repo.TxRepo,
) *SettlementService {
	return &SettlementService{
		accounts:  accounts,
		positions: positions,
		coins:     coins,
		tx:        tx,
	}
}

type FillResult struct {
	RealizedPnl decimal.Decimal // OPEN分支恒为0
	Fee         decimal.Decimal
	CloseVolume decimal.Decimal // OPEN分支等于dealVolume
}

// 结算一笔成交（对order代表的这一方）。isMaker决定手续费按maker还是taker费率收；
// MVP不区分强平单的清算费率，统一按maker/taker两档收(见plan文件的简化说明)。
func (s *SettlementService) SettleFill(
	ctx context.Context,
	order *model.Order,
	dealVolume, dealPrice decimal.Decimal,
	isMaker bool,
	now int64,
) (FillResult, error) {
	coin, err := s.coins.FindBySymbol(ctx, order.Symbol)
	if err != nil {
		return FillResult{}, err
	}

	var result FillResult
	if order.Action == model.ActionOpen {
		// 开仓
		// 按订单总冻结保证金比例，分别算出这一笔成交对应的frozen_margin/frozen_credit
		// 是多少——原始冻结按下单时router.go算出的保守参考价估算(SHORT+OPEN是
		// max(委托价,标记价)，防止用远低于市价的吃单价把冻结的保证金骗得过低)，可能跟实际
		// 成交价有出入，差额留在frozen_margin/frozen_credit里，等这张单全部成交/撤单时
		// 统一释放剩余部分
		filledFromAvailable, filledFromCredit := order.ProportionalFrozen(dealVolume)
		filledMargin := filledFromAvailable.Add(filledFromCredit)
		// 这笔成交按真实成交价算，本该占用多少保证金——真实的风险敞口，决定强平价该在哪
		properMargin := dealVolume.Mul(dealPrice).Div(decimal.NewFromInt(int64(order.Leverage)))

		// balance/credit是不随冻结变化的总额(见model.Account.Balance)，这笔仓位的锁定
		// 应该一直留在frozen_margin/frozen_credit里(直到平仓才释放)，不是退回balance/
		// credit。这里要做的只是把frozen_margin/frozen_credit从"下单时的保守估算"
		// 调整到"按真实成交价算出的properMargin"，按原始冻结里available/credit的比例把
		// properMargin也拆成两部分，调整量= filledFromAvailable-properFromAvailable
		// (正=多冻了、释放差额；负=冻少了、补冻差额)，不能笼统调整成一边，否则等于让
		// credit经过这条渠道被洗成balance
		var properFromAvailable, properFromCredit decimal.Decimal
		if filledMargin.Sign() > 0 {
			properFromAvailable = properMargin.Mul(filledFromAvailable).Div(filledMargin)
			properFromCredit = properMargin.Sub(properFromAvailable)
		} else {
			properFromAvailable = properMargin
		}
		// properFromCredit要记到仓位的credit_margin上——平仓时才能按这个比例精确释放
		// frozen_credit而不是笼统释放frozen_margin(见ApplyCloseFill)
		if err := s.positions.ApplyOpenFill(
			ctx,
			order.UID,
			order.Symbol,
			order.Side,
			dealVolume,
			dealPrice,
			properMargin,
			properFromCredit,
			order.Leverage,
			now,
		); err != nil {
			return FillResult{}, err
		}
		if err := s.accounts.UnfreezeMargin(ctx, order.UID, filledFromAvailable.Sub(properFromAvailable), filledFromCredit.Sub(properFromCredit)); err != nil {
			return FillResult{}, err
		}
		result.CloseVolume = dealVolume
	} else {
		// 平仓
		realizedPnl, releasedMargin, releasedCreditMargin, closeVolume, err := s.positions.ApplyCloseFill(
			ctx,
			order.UID,
			order.Symbol,
			order.Side,
			dealVolume,
			dealPrice,
			now,
		)
		if err != nil {
			return FillResult{}, err
		}
		// releasedMargin/releasedCreditMargin是这笔仓位从开仓起一直锁在frozen_margin/
		// frozen_credit里的那部分钱(见OPEN分支的properFromAvailable/properFromCredit)，
		// 按比例释放的那一份原样解锁——不能笼统释放成一边(会把credit的锁定洗成balance的
		// 锁定)，也不能跟已实现盈亏混在一起走SettlePnl的"先balance后credit"亏损兜底顺序
		// (那条顺序是给真实亏损用的，解锁本金不该被那套逻辑误吞)
		releasedFromAvailable := releasedMargin.Sub(releasedCreditMargin)
		if err := s.accounts.UnfreezeMargin(ctx, order.UID, releasedFromAvailable, releasedCreditMargin); err != nil {
			return FillResult{}, err
		}
		// 已实现盈亏可正可负，走SettlePnl的"先balance后credit"顺序——盈利只进balance，
		// 亏损优先冲抵balance之外的自有资金，credit留到最后
		if err := s.accounts.SettlePnl(ctx, order.UID, realizedPnl); err != nil {
			return FillResult{}, err
		}
		if err := s.tx.Insert(ctx, order.UID, order.Symbol, model.TxRealizedPnl, realizedPnl, now); err != nil {
			return FillResult{}, err
		}
		result.RealizedPnl = realizedPnl
		result.CloseVolume = closeVolume
	}

	feeVolume := result.CloseVolume
	feeBase := feeVolume.Mul(dealPrice)
	feeRate := coin.TakerFee
	if isMaker {
		feeRate = coin.MakerFee
	}
	fee := feeBase.Mul(feeRate)
	if err := s.accounts.DeductFee(ctx, order.UID, fee); err != nil {
		return FillResult{}, err
	}
	if err := s.tx.Insert(ctx, order.UID, order.Symbol, model.TxFee, fee.Neg(), now); err != nil {
		return FillResult{}, err
	}
	result.Fee = fee
	return result, nil
}

func NowMillis() int64 {
	return time.Now().UnixMilli()
}
