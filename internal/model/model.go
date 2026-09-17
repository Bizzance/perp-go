package model

import "github.com/shopspring/decimal"

type Side string

const (
	SideLong  Side = "long"
	SideShort Side = "short"
)

func (s Side) Opposite() Side {
	if s == SideLong {
		return SideShort
	}
	return SideLong
}

type OrderAction string

const (
	ActionOpen  OrderAction = "open"
	ActionClose OrderAction = "close"
)

type OrderType string

const (
	OrderTypeLimit  OrderType = "limit"
	OrderTypeMarket OrderType = "market"
)

type OrderStatus string

const (
	OrderStatusOpen            OrderStatus = "open"
	OrderStatusPartiallyFilled OrderStatus = "partially_filled"
	OrderStatusFilled          OrderStatus = "filled"
	OrderStatusCanceled        OrderStatus = "canceled"
	OrderStatusRejected        OrderStatus = "rejected"
)

// ActiveOrderStatuses 撮合引擎还需要继续处理的状态——扫描定时任务/撤单校验都用这个判断
// "这笔委托还活着吗"
var ActiveOrderStatuses = map[OrderStatus]bool{
	OrderStatusOpen:            true,
	OrderStatusPartiallyFilled: true,
}

type PositionStatus string

const (
	PositionStatusNormal      PositionStatus = "normal"
	PositionStatusLiquidating PositionStatus = "liquidating"
	PositionStatusClosed      PositionStatus = "closed"
)

// TransactionType 资金流水类型——纯审计用途的字符串常量，不参与任何计算
const (
	TxDeposit          = "deposit"           // 资金账户注入(正数)/扣减(负数)
	TxFee              = "fee"               // 交易手续费(负数)
	TxRealizedPnl      = "realized_pnl"      // 平仓已实现盈亏(可正可负)
	TxLiquidationClear = "liquidation_clear" // 强平结算后清算维持保证金缓冲进保险基金，用户侧记为负数
	TxFundingFee       = "funding_fee"       // 资金费率结算，多头/空头互相划转，可正可负
)

type Account struct {
	ID           uint64          `db:"id"`
	UID          uint64          `db:"uid"`
	IsInsured    bool            `db:"is_insured"`    // 是否投保
	Round        uint64          `db:"round"`         // 轮数
	Credit       decimal.Decimal `db:"credit"`        // 信用额度
	Available    decimal.Decimal `db:"available"`     // 可用
	FrozenMargin decimal.Decimal `db:"frozen_margin"` // 冻结的保证金
	Version      uint32          `db:"version"`
}

type Coin struct {
	Symbol               string          `db:"symbol"`
	BaseCoinScale        int32           `db:"base_coin_scale"`
	PriceScale           int32           `db:"price_scale"`
	Enable               bool            `db:"enable"`
	MakerFee             decimal.Decimal `db:"maker_fee"`
	TakerFee             decimal.Decimal `db:"taker_fee"`
	PriceTick            decimal.Decimal `db:"price_tick"`
	VolumeStep           decimal.Decimal `db:"volume_step"`
	MinVolume            decimal.Decimal `db:"min_volume"`
	MaxVolume            decimal.Decimal `db:"max_volume"`
	FundingIntervalHours int32           `db:"funding_interval_hours"`
	FundingRateCap       decimal.Decimal `db:"funding_rate_cap"`
	PriceProtectionRatio decimal.Decimal `db:"price_protection_ratio"`
}

// 保证金分档(风险限额)：维持保证金率/最大杠杆按仓位名义价值分档，仓位越大
// 风险越高、维持保证金率越高、允许的杠杆越低——取代原来"整个合约一个固定维持保证金率/
// 最大杠杆"的简化。MaintenanceAmount是速算扣除数，让跨档位时维持保证金的计算连续，不会在
// 档位边界出现跳变，公式=名义价值*MaintenanceMarginRate-MaintenanceAmount，见
// PositionService.TierFor
type RiskLimitTier struct {
	ID                    uint64          `db:"id"`
	Symbol                string          `db:"symbol"`
	Tier                  int32           `db:"tier"`
	MaxNotional           decimal.Decimal `db:"max_notional"` // 本档名义价值上限，0=不限(最后一档)
	MaintenanceMarginRate decimal.Decimal `db:"maintenance_margin_rate"`
	MaintenanceAmount     decimal.Decimal `db:"maintenance_amount"`
	MaxLeverage           uint32          `db:"max_leverage"`
}

type Order struct {
	OrderID      uint64          `db:"order_id"`
	UID          uint64          `db:"uid"`
	Symbol       string          `db:"symbol"`
	Side         Side            `db:"side"`   // long/short
	Action       OrderAction     `db:"action"` // open/close
	Type         OrderType       `db:"type"`   // limit/market
	Price        decimal.Decimal `db:"price"`
	Amount       decimal.Decimal `db:"amount"`        // 挂单数量
	TradedAmount decimal.Decimal `db:"traded_amount"` // 已成交的数量
	AvgDealPrice decimal.Decimal `db:"avg_deal_price"`
	FrozenMargin decimal.Decimal `db:"frozen_margin"`
	Leverage     uint32          `db:"leverage"`
	ReduceOnly   bool            `db:"reduce_only"`
	Liquidation  bool            `db:"liquidation"`
	Status       OrderStatus     `db:"status"` // open/filled/partially_filled/canceled/rejected
	CreateTime   int64           `db:"create_time"`
	UpdateTime   int64           `db:"update_time"`
}

func (o *Order) RemainingAmount() decimal.Decimal {
	return o.Amount.Sub(o.TradedAmount)
}

type Position struct {
	ID             uint64          `db:"id"`
	UID            uint64          `db:"uid"`
	Symbol         string          `db:"symbol"`
	Side           Side            `db:"side"`
	Volume         decimal.Decimal `db:"volume"`
	AvgEntryPrice  decimal.Decimal `db:"avg_entry_price"`
	PositionMargin decimal.Decimal `db:"position_margin"`
	Leverage       uint32          `db:"leverage"`
	Status         PositionStatus  `db:"status"`
	Version        uint32          `db:"version"`
	UpdateTime     int64           `db:"update_time"`
}

// 未实现盈亏公式：
// 多头 = (markPrice - avgEntryPrice) * volume；
// 空头 = (avgEntryPrice - markPrice) * volume
func (p *Position) UnrealizedPnl(markPrice decimal.Decimal) decimal.Decimal {
	if p.Side == SideLong {
		return markPrice.Sub(p.AvgEntryPrice).Mul(p.Volume)
	}
	return p.AvgEntryPrice.Sub(markPrice).Mul(p.Volume)
}

// 单仓强平价估算(逐仓式公式，仅供展示参考)——维持保证金要求按分档公式
// notional * mmr-maintenanceAmount推导：
// 多头：liqPrice = (avgEntryPrice*N - positionMargin - maintenanceAmount) / (N * (1 - mmr))
// 空头：liqPrice = (avgEntryPrice*N + positionMargin + maintenanceAmount) / (N * (1 + mmr))
// 全仓真实强平以LiquidationService里"账户权益 vs 全部仓位维持保证金要求之和"为准，
// 这个值只在没有其它持仓、也不考虑available缓冲时才精确，MVP先用这个简化公式做展示
func (p *Position) LiquidationPrice(mmr, maintenanceAmount decimal.Decimal) decimal.Decimal {
	if p.Volume.IsZero() {
		return decimal.Zero
	}
	entryTimesN := p.AvgEntryPrice.Mul(p.Volume)
	if p.Side == SideLong {
		denom := p.Volume.Mul(decimal.NewFromInt(1).Sub(mmr))
		if denom.Sign() <= 0 {
			return decimal.Zero
		}
		return decimal.Max(decimal.Zero, entryTimesN.Sub(p.PositionMargin).Sub(maintenanceAmount).Div(denom))
	}
	denom := p.Volume.Mul(decimal.NewFromInt(1).Add(mmr))
	if denom.Sign() <= 0 {
		return decimal.Zero
	}
	return decimal.Max(decimal.Zero, entryTimesN.Add(p.PositionMargin).Add(maintenanceAmount).Div(denom))
}

type Trade struct {
	TradeID      uint64          `db:"trade_id"`
	Symbol       string          `db:"symbol"`
	Price        decimal.Decimal `db:"price"`
	Volume       decimal.Decimal `db:"volume"`
	BuyOrderID   uint64          `db:"buy_order_id"`
	SellOrderID  uint64          `db:"sell_order_id"`
	BuyUID       uint64          `db:"buy_uid"`
	SellUID      uint64          `db:"sell_uid"`
	MakerOrderID uint64          `db:"maker_order_id"`
	CreateTime   int64           `db:"create_time"`
}

type InsuranceFund struct {
	ID      uint64          `db:"id"`
	Balance decimal.Decimal `db:"balance"`
	Version uint32          `db:"version"`
}

// 一个symbol一个资金费率结算周期的落库记录——审计+客户端历史费率查询用，见FundingService.SettleIfDue
type FundingRateRecord struct {
	ID          uint64          `db:"id"`
	Symbol      string          `db:"symbol"`
	FundingTime int64           `db:"funding_time"`
	Rate        decimal.Decimal `db:"rate"`
	MarkPrice   decimal.Decimal `db:"mark_price"`
	IndexPrice  decimal.Decimal `db:"index_price"`
	CreateTime  int64           `db:"create_time"`
}
