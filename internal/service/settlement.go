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
		// 按订单总冻结保证金比例，分别算出这一笔成交对应释放多少来自available/credit的
		// 冻结保证金——原始冻结按下单时router.go算出的保守参考价估算(SHORT+OPEN是
		// max(委托价,标记价)，防止用远低于市价的吃单价把冻结的保证金骗得过低)，可能跟实际
		// 成交价有出入，差额留在frozen_margin/frozen_credit里，等这张单全部成交/撤单时
		// 统一释放剩余部分
		filledFromAvailable, filledFromCredit := order.ProportionalFrozen(dealVolume)
		filledMargin := filledFromAvailable.Add(filledFromCredit)
		// 这笔成交按真实成交价算，本该占用多少保证金——真实的风险敞口，决定强平价该在哪
		properMargin := dealVolume.Mul(dealPrice).Div(decimal.NewFromInt(int64(order.Leverage)))

		if err := s.accounts.DecreaseFrozenMargin(ctx, order.UID, filledFromAvailable, filledFromCredit); err != nil {
			return FillResult{}, err
		}
		// 全仓模式：positionMargin只是记账用的名义值，不需要真的搬钱，但available/credit
		// 不能把filledFromAvailable/filledFromCredit原样退回——那是下单时按保守参考价冻结
		// 的钱，跟这笔仓位真实该占用的properMargin可能不相等(尤其是SHORT+OPEN报了远低于
		// 市价的吃单价、真实按对手高价成交的情况，冻结时按保守估计多冻了一些)。这里按
		// 原始冻结里available/credit的比例，把properMargin也拆成两部分，分别退
		// filledFromAvailable-properFromAvailable / filledFromCredit-properFromCredit这两个
		// 差额——冻结多了就把多冻的部分退回来源，冻结不够就从来源里再扣差额，不能笼统退到
		// available，否则等于让credit经过这条渠道被洗成available
		var properFromAvailable, properFromCredit decimal.Decimal
		if filledMargin.Sign() > 0 {
			properFromAvailable = properMargin.Mul(filledFromAvailable).Div(filledMargin)
			properFromCredit = properMargin.Sub(properFromAvailable)
		} else {
			properFromAvailable = properMargin
		}
		// properFromCredit要记到仓位的credit_margin上——这部分properMargin虽然全仓下没有
		// 真的搬钱，但对应地永久从available/credit里扣掉了(见下面SettleToAvailable/
		// SettleToCredit)，仓位必须记住这笔钱来自credit多少，平仓时才能把这部分精确还给
		// credit而不是笼统还给available(否则等于让credit经过"开仓再平仓"这条渠道被洗成
		// available，见ApplyCloseFill)
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
		if err := s.accounts.SettleToAvailable(ctx, order.UID, filledFromAvailable.Sub(properFromAvailable)); err != nil {
			return FillResult{}, err
		}
		if err := s.accounts.SettleToCredit(ctx, order.UID, filledFromCredit.Sub(properFromCredit)); err != nil {
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
		// releasedMargin/releasedCreditMargin是开仓时永久从available/credit划走、记进
		// position_margin/credit_margin的那部分钱(见OPEN分支的properFromAvailable/
		// properFromCredit)，按比例释放的那一份必须原样退回各自来源——不能笼统退到
		// available(会把credit洗成available)，也不能跟已实现盈亏混在一起走SettlePnl的
		// "先available后credit"亏损兜底顺序(那条顺序是给真实亏损用的，退还本金不该被
		// 那套逻辑误吞)
		releasedFromAvailable := releasedMargin.Sub(releasedCreditMargin)
		if err := s.accounts.SettleToAvailable(ctx, order.UID, releasedFromAvailable); err != nil {
			return FillResult{}, err
		}
		if !releasedCreditMargin.IsZero() {
			if err := s.accounts.SettleToCredit(ctx, order.UID, releasedCreditMargin); err != nil {
				return FillResult{}, err
			}
		}
		// 已实现盈亏可正可负，走SettlePnl的"先available后credit"顺序——盈利只进available，
		// 亏损优先冲抵available之外的自有资金，credit留到最后
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
