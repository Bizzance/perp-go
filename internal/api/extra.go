package api

import (
	"context"
	"errors"
	"log"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"

	"perp-go/internal/events"
	"perp-go/internal/model"
	"perp-go/internal/service"
)

// 这个文件是合作方对接需要的"查询类/批量类"接口：合约信息、行情、单笔委托查询、批量撤单、
// 资金流水、强平记录。核心交易链路(下单/撤单/条件单/杠杆)在router.go

func (s *Server) health(c *gin.Context) {
	ok(c, gin.H{"status": "ok", "time": time.Now().UnixMilli()})
}

// ---------- 合约信息 ----------

func (s *Server) contractList(c *gin.Context) {
	coins, err := s.coins.FindAllEnabled(c.Request.Context())
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	ok(c, coins)
}

// 单个合约的完整交易规则：价格/数量精度与步长、单笔限额、手续费率、资金费率
// 参数，加上保证金分档(最大杠杆随仓位名义价值分档)——客户端做下单表单校验、展示杠杆上限
// 都靠这个，不需要写死
type contractDetail struct {
	model.Coin
	Tiers []model.RiskLimitTier `json:"tiers"`
}

func (s *Server) contractDetail(c *gin.Context) {
	symbol := c.Query("symbol")
	coin, err := s.coins.FindBySymbol(c.Request.Context(), symbol)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	if coin == nil || !coin.Enable {
		failC(c, 400, ErrSymbolNotFound, "contract does not exist or is disabled")
		return
	}
	tiers, err := s.positions.Tiers(c.Request.Context(), symbol)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	if tiers == nil {
		tiers = []model.RiskLimitTier{}
	}
	ok(c, contractDetail{Coin: *coin, Tiers: tiers})
}

// ---------- 公开行情 ----------

// 没有对应数据的字段是null(比如这个合约还从没成交过)，不返回0——0是一个合法的价格
// 取值，客户端没法区分"价格是0"和"没有价格"
type ticker struct {
	Symbol     string           `json:"symbol"`
	LastPrice  *decimal.Decimal `json:"lastPrice"`
	MarkPrice  *decimal.Decimal `json:"markPrice"`
	IndexPrice *decimal.Decimal `json:"indexPrice"`
	Open24h    *decimal.Decimal `json:"open24h"`
	High24h    *decimal.Decimal `json:"high24h"`
	Low24h     *decimal.Decimal `json:"low24h"`
	Volume24h  decimal.Decimal  `json:"volume24h"`
	Change24h  *decimal.Decimal `json:"change24h"` // (lastPrice-open24h)/open24h
}

// 行情摘要。24h统计口径：最近24根1小时K线聚合(含当前还没走完的这一根)，所以实际
// 覆盖的时间窗口在23~24小时之间，不是严格的滚动24小时——换来的是不用扫成交明细表，查询成本
// 恒定；这段时间内没有成交的小时没有K线，不影响聚合结果
func (s *Server) marketTicker(c *gin.Context) {
	ctx := c.Request.Context()
	symbol := c.Query("symbol")
	coin, err := s.coins.FindBySymbol(ctx, symbol)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	if coin == nil || !coin.Enable {
		failC(c, 400, ErrSymbolNotFound, "contract does not exist or is disabled")
		return
	}
	t := ticker{Symbol: symbol}
	if mark, has := s.markPrice.Get(ctx, symbol); has {
		t.MarkPrice = &mark
	}
	if idx, has := s.markPrice.GetIndexPrice(ctx, symbol); has {
		t.IndexPrice = &idx
	}
	latest, err := s.trades.FindBySymbol(ctx, symbol, 1, 0)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	// 最近一根1分钟K线：K线来自外部行情时用它的收盘价当最新价；来自我们自己的成交时只有还没有成交才需要它，有成交就不用多查一次
	var recent []model.Kline
	if s.klineExternal || len(latest) == 0 {
		if recent, err = s.klines.FindRecent(ctx, symbol, model.Kline1m, 1); err != nil {
			fail(c, 500, err.Error())
			return
		}
	}
	// 最新价的来源：K线来自外部行情(币安)时，用最近一根1分钟K线的收盘价，也就是币安的最新价——我们自己成交少的时候
	// 最后一笔成交价会停很久，跟币安差很多；K线来自我们自己的成交时用最新成交价。另一个没有数据时退回到有的那个，都没有就是null
	switch {
	case s.klineExternal && len(recent) > 0:
		p := recent[0].Close
		t.LastPrice = &p
	case len(latest) > 0:
		t.LastPrice = &latest[0].Price
	case len(recent) > 0:
		p := recent[0].Close
		t.LastPrice = &p
	}
	klines, err := s.klines.FindRecent(ctx, symbol, model.Kline1h, 24)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	if len(klines) > 0 { // FindRecent按开盘时间升序返回
		open := klines[0].Open
		high, low := klines[0].High, klines[0].Low
		for _, k := range klines {
			high = decimal.Max(high, k.High)
			low = decimal.Min(low, k.Low)
			t.Volume24h = t.Volume24h.Add(k.Volume)
		}
		t.Open24h, t.High24h, t.Low24h = &open, &high, &low
		if t.LastPrice != nil && open.Sign() > 0 {
			change := t.LastPrice.Sub(open).Div(open)
			t.Change24h = &change
		}
	}
	ok(c, t)
}

// 公开最新成交(不含买卖双方uid/委托id)，trade_id倒序，支持limit+before翻页
func (s *Server) marketTrades(c *gin.Context) {
	symbol := c.Query("symbol")
	coin, err := s.coins.FindBySymbol(c.Request.Context(), symbol)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	if coin == nil || !coin.Enable {
		failC(c, 400, ErrSymbolNotFound, "contract does not exist or is disabled")
		return
	}
	limit, before, ok2 := pageParams(c)
	if !ok2 {
		return
	}
	trades, err := s.trades.FindBySymbol(c.Request.Context(), symbol, limit, before)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	out := make([]model.PublicTrade, 0, len(trades))
	for i := range trades {
		out = append(out, trades[i].Public())
	}
	ok(c, out)
}

// ---------- 单笔委托查询 ----------

// 单笔查询的定位参数：orderId和requestId二选一，都传优先orderId
func parseOrderRef(c *gin.Context) (orderID uint64, requestID string, valid bool) {
	if v := c.Query("orderId"); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil || n == 0 {
			fail(c, 400, "orderId is invalid")
			return 0, "", false
		}
		return n, "", true
	}
	cid, msg := normalizeRequestID(c.Query("requestId"))
	if msg != "" {
		fail(c, 400, msg)
		return 0, "", false
	}
	if cid == "" {
		fail(c, 400, "either orderId or requestId is required")
		return 0, "", false
	}
	return 0, cid, true
}

func (s *Server) orderDetail(c *gin.Context) {
	uid, ok1 := s.parseAccountUID(c)
	if !ok1 {
		return
	}
	orderID, requestID, valid := parseOrderRef(c)
	if !valid {
		return
	}
	var o *model.Order
	var err error
	if orderID > 0 {
		o, err = s.orders.FindByOrderID(c.Request.Context(), orderID)
	} else {
		o, err = s.orders.FindByRequestID(c.Request.Context(), uid, requestID)
	}
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	if o == nil || o.UID != uid {
		failC(c, 400, ErrOrderNotFound, "order does not exist")
		return
	}
	ok(c, o)
}

func (s *Server) conditionalOrderDetail(c *gin.Context) {
	uid, ok1 := s.parseAccountUID(c)
	if !ok1 {
		return
	}
	orderID, requestID, valid := parseOrderRef(c)
	if !valid {
		return
	}
	var co *model.ConditionalOrder
	var err error
	if orderID > 0 {
		co, err = s.conditionalOrders.FindByOrderID(c.Request.Context(), orderID)
	} else {
		co, err = s.conditionalOrders.FindByRequestID(c.Request.Context(), uid, requestID)
	}
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	if co == nil || co.UID != uid {
		failC(c, 400, ErrOrderNotFound, "conditional order does not exist")
		return
	}
	ok(c, co)
}

// ---------- 批量撤单 ----------

type cancelAllRequest struct {
	UID    uint64 `json:"uid" binding:"required"`
	Symbol string `json:"symbol"` // 省略=这个uid名下全部合约
	// IncludeConditional true=同时撤销还没触发的条件单。默认false：条件单(止盈止损)通常是
	// 用户希望一直留着保护仓位的，"撤销全部委托"不应该悄悄把它们也撤了
	IncludeConditional bool `json:"includeConditional"`
}

type cancelAllResult struct {
	CancelRequested     int `json:"cancelRequested"`     // 已提交撤单请求的普通委托笔数(异步，撤单是否生效看委托状态)
	CancelRequestFailed int `json:"cancelRequestFailed"` // 提交撤单请求失败的笔数(可以重试本接口)
	ConditionalCanceled int `json:"conditionalCanceled"` // 已经撤销的条件单笔数(同步生效)
	ConditionalFailed   int `json:"conditionalFailed"`   // 条件单撤销失败的笔数(含被并发触发抢先的)
}

// 批量撤销这个uid(可选限定symbol)的全部挂单。普通委托的撤单要摸engine内存里
// 的订单簿，跟单笔撤单接口一样只能给每笔发一条Kafka撤单事件、异步执行，这里同步返回的
// 是"已提交多少笔撤单请求"，不代表已经撤成功；条件单只碰MySQL、同步生效。部分失败不回滚
// 已经提交的，调用方按返回的failed计数重试即可(重复提交撤单请求是安全的，engine侧对已经
// 终结的委托会直接跳过)
func (s *Server) cancelAllOrders(c *gin.Context) {
	var req cancelAllRequest
	if !bindJSON(c, &req) {
		return
	}
	if !s.requireAccount(c, req.UID) {
		return
	}
	ctx := c.Request.Context()
	if req.Symbol != "" {
		coin, err := s.coins.FindBySymbol(ctx, req.Symbol)
		if err != nil {
			fail(c, 500, err.Error())
			return
		}
		if coin == nil {
			failC(c, 400, ErrSymbolNotFound, "contract does not exist")
			return
		}
	}
	orders, err := s.orders.FindActiveByUID(ctx, req.UID, req.Symbol)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	var result cancelAllResult
	for _, o := range orders {
		if err := s.publishCancel(o.OrderID, o.UID, o.Symbol); err != nil {
			result.CancelRequestFailed++
			continue
		}
		result.CancelRequested++
	}
	if req.IncludeConditional {
		pending, err := s.conditionalOrders.FindActiveByUID(ctx, req.UID, req.Symbol)
		if err != nil {
			fail(c, 500, err.Error())
			return
		}
		for _, co := range pending {
			if err := s.cancelPendingConditional(ctx, co); err != nil {
				result.ConditionalFailed++
				continue
			}
			result.ConditionalCanceled++
		}
	}
	ok(c, result)
}

func (s *Server) publishCancel(orderID, uid uint64, symbol string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.producer.Publish(ctx, events.TopicOrderCancel, symbol, events.OrderCancelEvent{OrderID: orderID, UID: uid, Symbol: symbol})
}

// ---------- 账户状态(冻结/解冻) ----------

type setAccountStatusRequest struct {
	UID    uint64              `json:"uid" binding:"required"`
	Status model.AccountStatus `json:"status"`
	Reason string              `json:"reason"`
}

type setAccountStatusResult struct {
	UID     uint64              `json:"uid"`
	Status  model.AccountStatus `json:"status"`
	Changed bool                `json:"changed"` // 这次有没有真的改变状态，已经是目标状态就是false
	// 下面几项只有冻结时才有意义：清理这个账户存量的开仓委托/条件开仓单的结果，含义同cancel-all
	CancelRequested     int `json:"cancelRequested"`
	CancelRequestFailed int `json:"cancelRequestFailed"`
	ConditionalCanceled int `json:"conditionalCanceled"`
	ConditionalFailed   int `json:"conditionalFailed"`
}

const maxStatusReasonLen = 255

// 冻结/解冻账户(运营接口)。冻结是"禁止新增风险"：账户不能再开仓、创建条件开仓单、改杠杆，
// 平仓、撤单、查询、结束本轮、运营的资金操作以及系统的强平/ADL/资金费/结算都不受影响。
// 冻结成功后顺带把存量的开仓类挂单清掉：普通委托给每笔发一条Kafka撤单事件(异步)，条件开仓单
// 直接撤销(同步)，强平委托和平仓类挂单不动。清理每次冻结请求都会执行、不管这次有没有改变状态，
// 所以部分失败后带同样的参数重试本接口就能补完。清理之外还有引擎侧的兜底(冻结前已落库还在
// Kafka排队的开仓委托、条件单刚好触发落地的委托，引擎撮合前会再查一次账户状态，冻结了就直接撤掉)，
// 所以不会有"清理时漏掉的开仓单被撮合"的窗口
func (s *Server) setAccountStatus(c *gin.Context) {
	var req setAccountStatusRequest
	if !bindJSON(c, &req) {
		return
	}
	if req.Status != model.AccountStatusActive && req.Status != model.AccountStatusFrozen {
		fail(c, 400, "status is invalid, must be active or frozen")
		return
	}
	if len(req.Reason) > maxStatusReasonLen {
		fail(c, 400, "reason must be at most 255 bytes")
		return
	}
	ctx := c.Request.Context()
	operator := c.GetString(ctxAPIKeyID)

	_, changed, err := s.accounts.SetStatus(ctx, req.UID, req.Status, req.Reason, operator)
	if err != nil {
		if errors.Is(err, service.ErrAccountNotFound) {
			failC(c, 400, ErrAccountNotFound, "account does not exist, please create it first")
			return
		}
		fail(c, 500, err.Error())
		return
	}
	if changed {
		log.Printf("[INFO] 账户状态变更 uid=%d status=%s operator=%q reason=%q", req.UID, req.Status, operator, req.Reason)
	}

	result := setAccountStatusResult{UID: req.UID, Status: req.Status, Changed: changed}
	if req.Status == model.AccountStatusFrozen {
		if err := s.sweepOpenOrders(ctx, req.UID, &result); err != nil {
			// 状态已经改成功了，只是清理没做完：返回失败让调用方重试(重试时changed=false，清理会重做)
			fail(c, 500, "account is frozen, but failed to clean up existing open orders, please retry this endpoint: "+err.Error())
			return
		}
	}
	ok(c, result)
}

// 冻结后清理这个账户的存量开仓类挂单：普通委托里只撤开仓且不是强平单的，平仓委托保留，
// 条件单里只撤开仓类的，平仓类的止盈止损保留
func (s *Server) sweepOpenOrders(ctx context.Context, uid uint64, result *setAccountStatusResult) error {
	orders, err := s.orders.FindActiveByUID(ctx, uid, "")
	if err != nil {
		return err
	}
	for _, o := range orders {
		if o.Action != model.ActionOpen || o.Liquidation {
			continue
		}
		if err := s.publishCancel(o.OrderID, o.UID, o.Symbol); err != nil {
			result.CancelRequestFailed++
			continue
		}
		result.CancelRequested++
	}
	pending, err := s.conditionalOrders.FindActiveByUID(ctx, uid, "")
	if err != nil {
		return err
	}
	for _, co := range pending {
		if co.Action != model.ActionOpen {
			continue
		}
		if err := s.cancelPendingConditional(ctx, co); err != nil {
			result.ConditionalFailed++
			continue
		}
		result.ConditionalCanceled++
	}
	return nil
}

// ---------- 资金流水 / 强平记录 ----------

var validTxTypes = map[string]bool{
	model.TxDeposit: true, model.TxFee: true, model.TxRealizedPnl: true,
	model.TxFundingFee: true, model.TxCreditGrant: true, model.TxRoundClose: true,
}

// 资金流水(充值/扣减、手续费、已实现盈亏、资金费、信用额度发放、结束本轮回收等)，id倒序，type可选过滤。合作方对账用
func (s *Server) accountTransactions(c *gin.Context) {
	uid, ok1 := s.parseAccountUID(c)
	if !ok1 {
		return
	}
	txType := c.Query("type")
	if txType != "" && !validTxTypes[txType] {
		fail(c, 400, "type is invalid")
		return
	}
	limit, before, ok2 := pageParams(c)
	if !ok2 {
		return
	}
	txs, err := s.txs.FindByUID(c.Request.Context(), uid, txType, limit, before)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	if txs == nil {
		txs = []model.Transaction{}
	}
	ok(c, txs)
}

// 这个uid的强平委托记录(orders表里liquidation=1的行)，orderId倒序。
// 每笔强平委托的成交价、成交量、状态都在里面，翻页规则同order/history
func (s *Server) liquidationHistory(c *gin.Context) {
	uid, ok1 := s.parseAccountUID(c)
	if !ok1 {
		return
	}
	limit, before, ok2 := pageParams(c)
	if !ok2 {
		return
	}
	orders, err := s.orders.FindLiquidationsByUID(c.Request.Context(), uid, limit, before)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	ok(c, orders)
}
