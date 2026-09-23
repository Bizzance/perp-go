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

// 撮合引擎还需要继续处理的状态——扫描定时任务/撤单校验都用这个判断"这笔委托还活着吗"
var ActiveOrderStatuses = map[OrderStatus]bool{
	OrderStatusOpen:            true,
	OrderStatusPartiallyFilled: true,
}

type TriggerDirection string

const (
	TriggerGTE TriggerDirection = "gte" // 标记价格涨到(或超过)触发价才触发
	TriggerLTE TriggerDirection = "lte" // 标记价格跌到(或低于)触发价才触发
)

type ConditionalOrderStatus string

const (
	ConditionalStatusPending   ConditionalOrderStatus = "pending"   // 等待触发
	ConditionalStatusTriggered ConditionalOrderStatus = "triggered" // 已触发，转成了真正的委托(见orders表同order_id那一行)
	ConditionalStatusCanceled  ConditionalOrderStatus = "canceled"  // 触发前被撤销
)

type PositionStatus string

const (
	PositionStatusNormal      PositionStatus = "normal"
	PositionStatusLiquidating PositionStatus = "liquidating"
	PositionStatusClosed      PositionStatus = "closed"
)

// TransactionType 资金流水类型——纯审计用途的字符串常量，不参与任何计算
const (
	TxDeposit     = "deposit"      // 资金账户注入(正数)/扣减(负数)
	TxFee         = "fee"          // 交易手续费(负数)
	TxRealizedPnl = "realized_pnl" // 平仓已实现盈亏(可正可负)
	TxFundingFee  = "funding_fee"  // 资金费率结算，多头/空头互相划转，可正可负
	TxCreditGrant = "credit_grant" // 合作方发放/追加信用额度，含投保触发强平时的自动赔付(正数)
	TxRoundClose  = "round_close"  // 结束本轮：balance/credit都清零(每轮都是独立的资金周期)，
	// 用户侧记为负数——不是亏损，balance这部分对应退给用户的钱(由合作方在系统外处理)，
	// credit这部分是回收没用完的赔付额度
)

// 资金流水一行(member_transactions表)，Amount正数=入账、负数=出账，Type见上面Tx*常量
type Transaction struct {
	ID         uint64          `db:"id" json:"id,string"`
	UID        uint64          `db:"uid" json:"uid"`
	Symbol     string          `db:"symbol" json:"symbol"` // 跟具体合约无关的流水(充值/发放额度等)为空字符串
	Amount     decimal.Decimal `db:"amount" json:"amount"`
	Type       string          `db:"type" json:"type"`
	CreateTime int64           `db:"create_time" json:"createTime"`
	// RequestID 合作方发起的充值/扣减/发放额度带的幂等键，对账时可以拿它跟自己的请求一一对应；
	// 系统内部产生的流水没有
	RequestID *string `db:"request_id" json:"requestId,omitempty"`
}

// 账户状态。frozen是"禁止新增风险，不禁止降低风险"：不能开仓、创建条件开仓单、
// 改杠杆，但仍然可以平仓、撤单、查询、结束本轮，运营的资金操作(充值/扣款/发额度)和系统自己的
// 强平、ADL、资金费、成交结算也不受影响，见docs/account-and-margin.md
type AccountStatus string

const (
	AccountStatusActive AccountStatus = "active"
	AccountStatusFrozen AccountStatus = "frozen"
)

type Account struct {
	ID           uint64          `db:"id" json:"-"`
	UID          uint64          `db:"uid" json:"uid"`
	IsInsured    bool            `db:"is_insured" json:"isInsured"`       // 是否投保
	Round        uint64          `db:"round" json:"round"`                // 轮数，结束本轮时+1
	Credit       decimal.Decimal `db:"credit" json:"credit"`              // 信用额度总额，只能当开仓保证金用，不能转出/提现
	Balance      decimal.Decimal `db:"balance" json:"balance"`            // 余额总额，不随下单/开仓冻结变化，只有充值/提现/已实现盈亏/手续费才会改它
	FrozenMargin decimal.Decimal `db:"frozen_margin" json:"frozenMargin"` // 来自balance的锁定额(挂单+持仓占用的合计)，可用余额=balance-frozenMargin
	FrozenCredit decimal.Decimal `db:"frozen_credit" json:"frozenCredit"` // 来自credit的锁定额，可用信用额度=credit-frozenCredit
	Version      uint32          `db:"version" json:"-"`
	Status       AccountStatus   `db:"status" json:"status"`
	StatusReason string          `db:"status_reason" json:"-"`
	StatusTime   int64           `db:"status_time" json:"-"`
}

type Coin struct {
	Symbol                string          `db:"symbol" json:"symbol"`
	BaseCoinScale         int32           `db:"base_coin_scale" json:"baseCoinScale"`
	PriceScale            int32           `db:"price_scale" json:"priceScale"`
	Enable                bool            `db:"enable" json:"enable"`
	MakerFee              decimal.Decimal `db:"maker_fee" json:"makerFee"`
	TakerFee              decimal.Decimal `db:"taker_fee" json:"takerFee"`
	PriceTick             decimal.Decimal `db:"price_tick" json:"priceTick"`
	VolumeStep            decimal.Decimal `db:"volume_step" json:"volumeStep"`
	MinVolume             decimal.Decimal `db:"min_volume" json:"minVolume"`
	MaxVolume             decimal.Decimal `db:"max_volume" json:"maxVolume"`
	FundingIntervalHours  int32           `db:"funding_interval_hours" json:"fundingIntervalHours"`
	FundingRateCap        decimal.Decimal `db:"funding_rate_cap" json:"fundingRateCap"`
	FundingImpactNotional decimal.Decimal `db:"funding_impact_notional" json:"fundingImpactNotional"` // 资金费率溢价用的冲击名义金额(USDT)，见FundingService.SampleOnce。0=不采样，跟别的"0=不限制"字段不同
	PriceProtectionRatio  decimal.Decimal `db:"price_protection_ratio" json:"priceProtectionRatio"`
}

// 保证金分档(风险限额)：维持保证金率/最大杠杆按仓位名义价值分档，仓位越大
// 风险越高、维持保证金率越高、允许的杠杆越低——取代原来"整个合约一个固定维持保证金率/
// 最大杠杆"的简化。MaintenanceAmount是速算扣除数，让跨档位时维持保证金的计算连续，不会在
// 档位边界出现跳变，公式=名义价值*MaintenanceMarginRate-MaintenanceAmount，见
// PositionService.TierFor
type RiskLimitTier struct {
	ID                    uint64          `db:"id" json:"-"`
	Symbol                string          `db:"symbol" json:"symbol"`
	Tier                  int32           `db:"tier" json:"tier"`
	MaxNotional           decimal.Decimal `db:"max_notional" json:"maxNotional"` // 本档名义价值上限，0=不限(最后一档)
	MaintenanceMarginRate decimal.Decimal `db:"maintenance_margin_rate" json:"maintenanceMarginRate"`
	MaintenanceAmount     decimal.Decimal `db:"maintenance_amount" json:"maintenanceAmount"`
	MaxLeverage           uint32          `db:"max_leverage" json:"maxLeverage"`
}

type Order struct {
	OrderID      uint64          `db:"order_id" json:"orderId,string"`
	UID          uint64          `db:"uid" json:"uid"`
	Symbol       string          `db:"symbol" json:"symbol"`
	Side         Side            `db:"side" json:"side"`     // long/short
	Action       OrderAction     `db:"action" json:"action"` // open/close
	Type         OrderType       `db:"type" json:"type"`     // limit/market
	Price        decimal.Decimal `db:"price" json:"price"`
	Amount       decimal.Decimal `db:"amount" json:"amount"`              // 挂单数量
	TradedAmount decimal.Decimal `db:"traded_amount" json:"tradedAmount"` // 已成交的数量
	AvgDealPrice decimal.Decimal `db:"avg_deal_price" json:"avgDealPrice"`
	FrozenMargin decimal.Decimal `db:"frozen_margin" json:"frozenMargin"` // 冻结保证金来自balance的部分
	FrozenCredit decimal.Decimal `db:"frozen_credit" json:"frozenCredit"` // 冻结保证金来自credit的部分
	Leverage     uint32          `db:"leverage" json:"leverage"`
	ReduceOnly   bool            `db:"reduce_only" json:"reduceOnly"`
	Liquidation  bool            `db:"liquidation" json:"liquidation"`
	Status       OrderStatus     `db:"status" json:"status"` // open/filled/partially_filled/canceled/rejected
	CreateTime   int64           `db:"create_time" json:"createTime"`
	UpdateTime   int64           `db:"update_time" json:"updateTime"`
	RequestID    *string         `db:"request_id" json:"requestId,omitempty"` // RequestID 合作方自己给这笔委托指定的幂等键(同一uid内唯一)，没传就是nil/NULL
	RequestHash  *string         `db:"request_hash" json:"-"`                 // RequestHash 请求参数摘要，同一个requestId再次提交时用来判断参数是否跟第一次一致，不对外暴露
}

func (o *Order) RemainingAmount() decimal.Decimal {
	return o.Amount.Sub(o.TradedAmount)
}

// 条件单(止盈止损/条件开仓)：触发前只是"记着一个条件"，不进撮合引擎的
// 订单簿，触发后按OrderID同一个id落地成一笔真正的Order记录，见docs/conditional-orders.md
type ConditionalOrder struct {
	OrderID          uint64                 `db:"order_id" json:"orderId,string"`
	UID              uint64                 `db:"uid" json:"uid"`
	Symbol           string                 `db:"symbol" json:"symbol"`
	Side             Side                   `db:"side" json:"side"`
	Action           OrderAction            `db:"action" json:"action"`
	TriggerPrice     decimal.Decimal        `db:"trigger_price" json:"triggerPrice"`
	TriggerDirection TriggerDirection       `db:"trigger_direction" json:"triggerDirection"`
	Type             OrderType              `db:"type" json:"type"`
	Price            decimal.Decimal        `db:"price" json:"price"` // 触发后委托的价格，market类型恒为0
	Amount           decimal.Decimal        `db:"amount" json:"amount"`
	Leverage         uint32                 `db:"leverage" json:"leverage"`
	ReduceOnly       bool                   `db:"reduce_only" json:"reduceOnly"`
	FrozenMargin     decimal.Decimal        `db:"frozen_margin" json:"frozenMargin"` // 创建时冻结的保证金来自balance的部分，只有开仓方向才有
	FrozenCredit     decimal.Decimal        `db:"frozen_credit" json:"frozenCredit"` // 创建时冻结的保证金来自credit的部分，只有开仓方向才有
	Status           ConditionalOrderStatus `db:"status" json:"status"`
	CreateTime       int64                  `db:"create_time" json:"createTime"`
	UpdateTime       int64                  `db:"update_time" json:"updateTime"`
	RequestID        *string                `db:"request_id" json:"requestId,omitempty"` // 含义同Order.RequestID，条件单自己的幂等键
	RequestHash      *string                `db:"request_hash" json:"-"`
}

// 判断当前标记价格是否已经满足这个条件单的触发条件
func (co *ConditionalOrder) Triggered(mark decimal.Decimal) bool {
	if co.TriggerDirection == TriggerGTE {
		return mark.GreaterThanOrEqual(co.TriggerPrice)
	}
	return mark.LessThanOrEqual(co.TriggerPrice)
}

// 按volume(可以是一笔成交量，也可以是撤单剩余量)占这笔委托总量的比例，
// 拆分出对应比例的frozen_margin/frozen_credit——成交转正(settlement.go)、撤单释放
// (engine.go的CancelOrder)两处都要用同一个公式，写两份容易在以后改动时只改一边、
// 悄悄让两条路径的释放比例算法分叉
func (o *Order) ProportionalFrozen(volume decimal.Decimal) (fromAvailable, fromCredit decimal.Decimal) {
	return o.FrozenMargin.Mul(volume).Div(o.Amount), o.FrozenCredit.Mul(volume).Div(o.Amount)
}

type Position struct {
	ID             uint64          `db:"id" json:"id"`
	UID            uint64          `db:"uid" json:"uid"`
	Symbol         string          `db:"symbol" json:"symbol"`
	Side           Side            `db:"side" json:"side"`
	Volume         decimal.Decimal `db:"volume" json:"volume"`
	AvgEntryPrice  decimal.Decimal `db:"avg_entry_price" json:"avgEntryPrice"`
	PositionMargin decimal.Decimal `db:"position_margin" json:"positionMargin"`
	CreditMargin   decimal.Decimal `db:"credit_margin" json:"creditMargin"` // position_margin里来自credit的部分
	Leverage       uint32          `db:"leverage" json:"leverage"`
	Status         PositionStatus  `db:"status" json:"status"`
	Version        uint32          `db:"version" json:"-"`
	UpdateTime     int64           `db:"update_time" json:"updateTime"`
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

// 单仓强平价估算(逐仓式公式，仅供展示参考)——维持保证金要求是"账户亏损达到lossRatio的
// balance+credit"(未投保100%、已投保80%，投保保的是整个账户，不是单笔仓位，见
// LiquidationService.checkAndLiquidate)，equityBase是这个账户当前的balance+credit。
// 用单仓近似推导(忽略账户里其它仓位的浮盈亏)：equity ≈ equityBase + unrealizedPnl，
// 触发条件unrealizedPnl <= -lossRatio*equityBase：
// 多头：liqPrice = avgEntryPrice - lossRatio*equityBase/volume
// 空头：liqPrice = avgEntryPrice + lossRatio*equityBase/volume
// 全仓真实强平以LiquidationService里"账户权益 vs lossRatio*(balance+credit)"为准，这个值
// 只在没有其它持仓时才精确，MVP先用这个简化公式做展示
func (p *Position) LiquidationPrice(lossRatio, equityBase decimal.Decimal) decimal.Decimal {
	if p.Volume.IsZero() {
		return decimal.Zero
	}
	buffer := lossRatio.Mul(equityBase).Div(p.Volume)
	if p.Side == SideLong {
		return decimal.Max(decimal.Zero, p.AvgEntryPrice.Sub(buffer))
	}
	return p.AvgEntryPrice.Add(buffer)
}

type Trade struct {
	TradeID      uint64          `db:"trade_id" json:"tradeId,string"`
	Symbol       string          `db:"symbol" json:"symbol"`
	Price        decimal.Decimal `db:"price" json:"price"`
	Volume       decimal.Decimal `db:"volume" json:"volume"`
	BuyOrderID   uint64          `db:"buy_order_id" json:"buyOrderId,string"`
	SellOrderID  uint64          `db:"sell_order_id" json:"sellOrderId,string"`
	BuyUID       uint64          `db:"buy_uid" json:"buyUid"`
	SellUID      uint64          `db:"sell_uid" json:"sellUid"`
	MakerOrderID uint64          `db:"maker_order_id" json:"makerOrderId,string"`
	CreateTime   int64           `db:"create_time" json:"createTime"`
}

// 公开成交视图(REST的/market/trades和WS的trade频道用)：不含买卖双方的uid和
// 委托id——公开行情不能泄露其它用户的身份。TakerSide是吃单方向(taker买入=buy)，行情展示
// 常用来给成交着色
type PublicTrade struct {
	TradeID    uint64          `json:"tradeId,string"`
	Symbol     string          `json:"symbol"`
	Price      decimal.Decimal `json:"price"`
	Volume     decimal.Decimal `json:"volume"`
	TakerSide  string          `json:"takerSide"` // buy/sell
	CreateTime int64           `json:"createTime"`
}

// 转成公开成交视图。挂单方(maker)是买单说明吃单方是卖出，反之亦然
func (t *Trade) Public() PublicTrade {
	takerSide := "buy"
	if t.MakerOrderID == t.BuyOrderID {
		takerSide = "sell"
	}
	return PublicTrade{TradeID: t.TradeID, Symbol: t.Symbol, Price: t.Price, Volume: t.Volume,
		TakerSide: takerSide, CreateTime: t.CreateTime}
}

// K线周期——固定这几档，不支持任意周期。每个周期各自独立维护一份K线数据
// (每笔成交同时更新全部周期各自对应的那一根)，不是从更小周期现场聚合，查询时直接读、
// 不用现算，见docs/kline.md
type KlineInterval string

const (
	Kline1m  KlineInterval = "1m"
	Kline5m  KlineInterval = "5m"
	Kline15m KlineInterval = "15m"
	Kline1h  KlineInterval = "1h"
	Kline4h  KlineInterval = "4h"
	Kline1d  KlineInterval = "1d"
)

// KlineIntervalMillis 每个周期对应的毫秒数，用来把成交时间对齐到所在K线的开盘时间
// (open_time = floor(tradeTime / 周期毫秒) * 周期毫秒)
var KlineIntervalMillis = map[KlineInterval]int64{
	Kline1m:  60_000,
	Kline5m:  5 * 60_000,
	Kline15m: 15 * 60_000,
	Kline1h:  60 * 60_000,
	Kline4h:  4 * 60 * 60_000,
	Kline1d:  24 * 60 * 60_000,
}

// AllKlineIntervals 每笔成交要更新的全部周期，固定顺序，遍历用
var AllKlineIntervals = []KlineInterval{Kline1m, Kline5m, Kline15m, Kline1h, Kline4h, Kline1d}

type Kline struct {
	Symbol     string          `db:"symbol" json:"symbol"`
	Interval   KlineInterval   `db:"interval" json:"interval"`
	OpenTime   int64           `db:"open_time" json:"openTime"`
	Open       decimal.Decimal `db:"open" json:"open"`
	High       decimal.Decimal `db:"high" json:"high"`
	Low        decimal.Decimal `db:"low" json:"low"`
	Close      decimal.Decimal `db:"close" json:"close"`
	Volume     decimal.Decimal `db:"volume" json:"volume"`
	TradeCount uint32          `db:"trade_count" json:"tradeCount"`
	UpdateTime int64           `db:"update_time" json:"updateTime"`
}

type InsuranceFund struct {
	ID      uint64          `db:"id" json:"-"`
	Balance decimal.Decimal `db:"balance" json:"balance"`
	Version uint32          `db:"version" json:"version"`
}

// 一个symbol一个资金费率结算周期的落库记录——审计+客户端历史费率查询用，见FundingService.SettleIfDue
type FundingRateRecord struct {
	ID          uint64          `db:"id" json:"id"`
	Symbol      string          `db:"symbol" json:"symbol"`
	FundingTime int64           `db:"funding_time" json:"fundingTime"`
	Rate        decimal.Decimal `db:"rate" json:"rate"`
	MarkPrice   decimal.Decimal `db:"mark_price" json:"markPrice"`
	IndexPrice  decimal.Decimal `db:"index_price" json:"indexPrice"`
	CreateTime  int64           `db:"create_time" json:"createTime"`
}
