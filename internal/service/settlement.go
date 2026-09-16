// 结算：一笔成交(不管是正常撮合还是强平单成交)落地到账户/持仓/流水——照抄Java版
// ContractTradeSettlementService.settleFill的分支结构(OPEN分支/CLOSE分支)。
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

func NewSettlementService(accounts *AccountService, positions *repo.PositionRepo, coins *repo.CoinRepo, tx *repo.TxRepo) *SettlementService {
	return &SettlementService{accounts: accounts, positions: positions, coins: coins, tx: tx}
}

type FillResult struct {
	RealizedPnl decimal.Decimal // OPEN分支恒为0
	Fee         decimal.Decimal
	CloseVolume decimal.Decimal // OPEN分支等于dealVolume
}

// SettleFill 结算一笔成交（对order代表的这一方）。isMaker决定手续费按maker还是taker费率收；
// MVP不区分强平单的清算费率，统一按maker/taker两档收(见plan文件的简化说明)。
func (s *SettlementService) SettleFill(ctx context.Context, order *model.Order, dealVolume, dealPrice decimal.Decimal, isMaker bool, now int64) (FillResult, error) {
	coin, err := s.coins.FindBySymbol(ctx, order.Symbol)
	if err != nil {
		return FillResult{}, err
	}

	var result FillResult
	if order.Action == model.ActionOpen {
		// 按订单总冻结保证金比例，算出这一笔成交实际应该转正的保证金——原始冻结按下单时
		// 价格估算，可能跟实际成交价有出入，差额留在frozenMargin里，等这张单全部成交/
		// 撤单时统一释放剩余部分
		filledMargin := order.FrozenMargin.Mul(dealVolume).Div(order.Amount)
		// 这笔成交按真实成交价算，本该占用多少保证金——真实的风险敞口，决定强平价该在哪
		properMargin := dealVolume.Mul(dealPrice).Div(decimal.NewFromInt(int64(order.Leverage)))

		if err := s.accounts.DecreaseFrozenMargin(ctx, order.UID, filledMargin); err != nil {
			return FillResult{}, err
		}
		if err := s.positions.ApplyOpenFill(ctx, order.UID, order.Symbol, order.Side, dealVolume, dealPrice, properMargin, order.Leverage, now); err != nil {
			return FillResult{}, err
		}
		// 全仓模式：positionMargin只是记账用的名义值，不需要真的搬钱，冻结的这笔钱原样还回available
		if err := s.accounts.SettleToAvailable(ctx, order.UID, filledMargin); err != nil {
			return FillResult{}, err
		}
		result.CloseVolume = dealVolume
	} else {
		realizedPnl, _, closeVolume, err := s.positions.ApplyCloseFill(ctx, order.UID, order.Symbol, order.Side, dealVolume, dealPrice, now)
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

func NowMillis() int64 { return time.Now().UnixMilli() }
