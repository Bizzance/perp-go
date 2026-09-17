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
		// 按订单总冻结保证金比例，算出这一笔成交对应释放多少冻结保证金——原始冻结按下单时
		// router.go算出的保守参考价估算(SHORT+OPEN是max(委托价,标记价)，防止用远低于市价
		// 的吃单价把冻结的保证金骗得过低)，可能跟实际成交价有出入，差额留在frozenMargin里，
		// 等这张单全部成交/撤单时统一释放剩余部分
		filledMargin := order.FrozenMargin.Mul(dealVolume).Div(order.Amount)
		// 这笔成交按真实成交价算，本该占用多少保证金——真实的风险敞口，决定强平价该在哪，也是这笔仓位真正应该从available里净扣掉的钱
		properMargin := dealVolume.Mul(dealPrice).Div(decimal.NewFromInt(int64(order.Leverage)))

		if err := s.accounts.DecreaseFrozenMargin(ctx, order.UID, filledMargin); err != nil {
			return FillResult{}, err
		}
		if err := s.positions.ApplyOpenFill(
			ctx,
			order.UID,
			order.Symbol,
			order.Side,
			dealVolume,
			dealPrice,
			properMargin,
			order.Leverage,
			now,
		); err != nil {
			return FillResult{}, err
		}
		// 全仓模式：positionMargin只是记账用的名义值，不需要真的搬钱，但available不能把
		// filledMargin原样退回——那是下单时按保守参考价冻结的钱，跟这笔仓位真实该占用的
		// properMargin可能不相等(尤其是SHORT+OPEN报了远低于市价的吃单价、真实按对手高价
		// 成交的情况，冻结时按保守估计多冻了一些)。这里退filledMargin-properMargin这个差额：
		// 冻结多了(filledMargin>properMargin)就把多冻的部分退给用户，冻结不够(理论上更
		// 少见，比如挂单挂了很久、真实成交时标记价格已经比下单时更高)就从available里再
		// 扣差额——多退少补，让available最终净扣掉的正好是这笔仓位真实占用的properMargin，
		// 不会把该占用的保证金错误地留在available里、变相凭空多出一部分可用余额
		if err := s.accounts.SettleToAvailable(ctx, order.UID, filledMargin.Sub(properMargin)); err != nil {
			return FillResult{}, err
		}
		result.CloseVolume = dealVolume
	} else {
		// 平仓
		realizedPnl, _, closeVolume, err := s.positions.ApplyCloseFill(
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
		// releasedMargin(第二个返回值)全仓下不用——开仓成交时已经还回available了，这里
		// 再加一次是重复计算，见ApplyOpenFill分支的注释
		if err := s.accounts.SettleToAvailable(ctx, order.UID, realizedPnl); err != nil {
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
