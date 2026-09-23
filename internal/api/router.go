package api

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"

	"perp-go/internal/events"
	"perp-go/internal/mq"
	"perp-go/internal/repo"
	"perp-go/internal/service"
	"perp-go/internal/ws"

	"perp-go/internal/model"
)

// 往Kafka发事件的能力，生产里是*mq.Producer。抽成接口是为了测试里换成只记录事件的替身，
// 不用起Kafka就能验证"发了哪些事件"
type eventPublisher interface {
	Publish(ctx context.Context, topic, key string, value any) error
}

type Server struct {
	accounts          *service.AccountService
	positions         *service.PositionService
	coins             *repo.CoinRepo
	orders            *repo.OrderRepo
	conditionalOrders *repo.ConditionalOrderRepo
	trades            *repo.TradeRepo
	klines            *repo.KlineRepo
	markPrice         *service.MarkPriceService
	funding           *service.FundingService
	producer          eventPublisher
	hub               *ws.Hub
	lock              *service.LockService
	txs               *repo.TxRepo
	auth              *Auth

	// POST /kline/sync：K线来源是外部行情(PERP_KLINE_SOURCE=external)时才接收；写入后把变化了的K线推给WebSocket订阅者
	klineExternal bool
	klinePub      klinePublisher
}

// 把一根K线推给订阅了这个symbol和周期的WebSocket客户端，生产里是*service.PushService
type klinePublisher interface {
	PublishKline(ctx context.Context, symbol string, k model.Kline)
}

// 打开POST /kline/sync。external=false(K线由我们自己的成交生成)时这个接口一律拒绝，避免两个来源的数据混在一起
func (s *Server) WithKlineSync(external bool, pub klinePublisher) *Server {
	s.klineExternal, s.klinePub = external, pub
	return s
}

func NewServer(
	accounts *service.AccountService,
	positions *service.PositionService,
	coins *repo.CoinRepo,
	orders *repo.OrderRepo,
	conditionalOrders *repo.ConditionalOrderRepo,
	trades *repo.TradeRepo,
	klines *repo.KlineRepo,
	markPrice *service.MarkPriceService,
	funding *service.FundingService,
	producer *mq.Producer,
	hub *ws.Hub,
	lock *service.LockService,
	txs *repo.TxRepo,
	auth *Auth,
) *Server {
	return &Server{
		accounts:          accounts,
		positions:         positions,
		coins:             coins,
		orders:            orders,
		conditionalOrders: conditionalOrders,
		trades:            trades,
		klines:            klines,
		markPrice:         markPrice,
		funding:           funding,
		producer:          producer,
		hub:               hub,
		lock:              lock,
		txs:               txs,
		auth:              auth,
	}
}

func (s *Server) Router() *gin.Engine {
	r := gin.Default()
	// 全局签名校验+权限检查。每个路由必须用s.auth.Route声明权限范围：ops是运营类接口(加钱扣钱、
	// 发信用额度、设投保、喂指数价)，其余都是trade；没声明的路由默认拒绝。/health免鉴权，
	// 用普通的r.GET注册，由中间件内部放行
	r.Use(s.auth.Middleware())
	s.auth.Route(r, "POST", "/account/create", ScopeTrade, s.createAccount)                             // 创建账户
	s.auth.Route(r, "POST", "/account/balance", ScopeOps, s.adjustBalance)                              // 调整余额
	s.auth.Route(r, "GET", "/account/info", ScopeTrade, s.accountInfo)                                  // 账户信息
	s.auth.Route(r, "POST", "/account/credit", ScopeOps, s.grantCredit)                                 // 授信
	s.auth.Route(r, "POST", "/account/insured", ScopeOps, s.setInsured)                                 // 开启/取消保险
	s.auth.Route(r, "POST", "/account/status", ScopeOps, s.setAccountStatus)                            // 设置账户状态
	s.auth.Route(r, "POST", "/account/round/close", ScopeTrade, s.closeRound)                           // 结束某轮
	s.auth.Route(r, "POST", "/order/add", ScopeTrade, s.addOrder)                                       // 下单
	s.auth.Route(r, "POST", "/order/cancel/:orderId", ScopeTrade, s.cancelOrder)                        // 取消订单
	s.auth.Route(r, "GET", "/order/current", ScopeTrade, s.orderCurrent)                                // 活跃订单
	s.auth.Route(r, "GET", "/order/history", ScopeTrade, s.orderHistory)                                // 历史委托
	s.auth.Route(r, "POST", "/order/conditional/add", ScopeTrade, s.addConditionalOrder)                // 挂条件单
	s.auth.Route(r, "POST", "/order/conditional/cancel/:orderId", ScopeTrade, s.cancelConditionalOrder) // 取消条件单
	s.auth.Route(r, "GET", "/order/conditional/current", ScopeTrade, s.conditionalOrderCurrent)         // 活跃条件单
	s.auth.Route(r, "GET", "/order/conditional/history", ScopeTrade, s.conditionalOrderHistory)         // 条件单历史委托
	s.auth.Route(r, "GET", "/position/current", ScopeTrade, s.positionCurrent)                          // 仓位
	s.auth.Route(r, "POST", "/position/leverage", ScopeTrade, s.setLeverage)                            // 调整杠杆
	s.auth.Route(r, "GET", "/trade/history", ScopeTrade, s.tradeHistory)                                // 成交历史
	s.auth.Route(r, "GET", "/funding/rate", ScopeTrade, s.fundingRate)                                  // 资金费率
	s.auth.Route(r, "GET", "/funding/history", ScopeTrade, s.fundingHistory)                            // 资金费率历史
	s.auth.Route(r, "GET", "/kline", ScopeTrade, s.kline)                                               // k线
	s.auth.Route(r, "POST", "/kline/sync", ScopeOps, s.syncKlines)                                      // 同步k线
	s.auth.Route(r, "POST", "/index-price", ScopeOps, s.setIndexPrice)                                  // 喂指数价
	s.auth.Route(r, "GET", "/ws", ScopeTrade, s.ws)                                                     // websocket
	r.GET("/health", s.health)                                                                          // 健康检查
	s.auth.Route(r, "GET", "/contract/list", ScopeTrade, s.contractList)                                // 合约列表
	s.auth.Route(r, "GET", "/contract/detail", ScopeTrade, s.contractDetail)                            // 合约详情
	s.auth.Route(r, "GET", "/market/ticker", ScopeTrade, s.marketTicker)                                // 行情摘要
	s.auth.Route(r, "GET", "/market/trades", ScopeTrade, s.marketTrades)                                // 最新成交
	s.auth.Route(r, "GET", "/order/detail", ScopeTrade, s.orderDetail)                                  // 订单详情
	s.auth.Route(r, "POST", "/order/cancel-all", ScopeTrade, s.cancelAllOrders)                         // 取消所有订单
	s.auth.Route(r, "GET", "/order/conditional/detail", ScopeTrade, s.conditionalOrderDetail)           // 条件单详情
	s.auth.Route(r, "GET", "/account/transactions", ScopeTrade, s.accountTransactions)                  // 用户流水
	s.auth.Route(r, "GET", "/liquidation/history", ScopeTrade, s.liquidationHistory)                    // 强平历史
	return r
}

// 统一的成功响应。nil切片会被序列化成JSON的null，而"没有数据"对列表类接口应该是空数组[]，
// 合作方的解析代码遍历null会出错，所以这里统一把nil切片换成空切片
func ok(c *gin.Context, data any) {
	if v := reflect.ValueOf(data); v.IsValid() && v.Kind() == reflect.Slice && v.IsNil() {
		data = reflect.MakeSlice(v.Type(), 0, 0).Interface()
	}
	c.JSON(http.StatusOK, gin.H{"code": 200, "message": "success", "data": data})
}

// 解析一个"必须是正整数、可省略"的query参数，省略时用def——
// contract-api的kline接口(limit)、contract-engine的depth接口(levels)都要这个校验规则，
// 两边共用同一份实现，不要各写一份、以后改校验规则漏改一边
func parsePositiveIntQuery(c *gin.Context, name string, def int) (int, string) {
	v := c.Query(name)
	if v == "" {
		return def, ""
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, name + " is invalid"
	}
	return n, ""
}

// 只给GET接口用——这些接口的参数走query string，没有body。写接口(POST)统一用
// JSON body + ShouldBindJSON，uid跟着各自的request struct走，不再需要这个helper
func parseUID(c *gin.Context) (uint64, bool) {
	uid, err := decimal.NewFromString(c.Query("uid"))
	if err != nil || uid.Sign() <= 0 {
		fail(c, 400, "uid is invalid")
		return 0, false
	}
	return uint64(uid.IntPart()), true
}

// 历史类GET接口共用的分页参数(limit + before游标)，参数不合法时已经写好400响应，
// 调用方判断第三个返回值决定要不要继续
func pageParams(c *gin.Context) (limit int, before uint64, ok bool) {
	limit, msg := parseLimit(c)
	if msg != "" {
		fail(c, 400, msg)
		return 0, 0, false
	}
	before, msg = parseBefore(c)
	if msg != "" {
		fail(c, 400, msg)
		return 0, 0, false
	}
	return limit, before, true
}

// 全部POST接口统一的JSON body解析入口，失败了直接写400响应——调用方判断返回值
// 决定要不要continue往下走，不用每个handler自己重复"解析失败就返回400"这几行
func bindJSON(c *gin.Context, req any) bool {
	if err := c.ShouldBindJSON(req); err != nil {
		fail(c, 400, "invalid request parameters")
		return false
	}
	return true
}

type adjustBalanceRequest struct {
	UID       uint64          `json:"uid" binding:"required"`
	Amount    decimal.Decimal `json:"amount"`
	RequestID string          `json:"requestId"` // RequestID 必填的幂等键：充值/扣款重试不带幂等键会重复入账或重复扣款
}

// 资金类接口(充值/扣款/发额度)的返回值，duplicate=true表示这个requestId之前已经处理过、
// 这次什么都没做
type fundOpResult struct {
	RequestID string `json:"requestId"`
	Duplicate bool   `json:"duplicate,omitempty"`
}

// 资金类接口共用的失败翻译：余额不足、幂等键参数冲突是业务上的正常拒绝(400)，其它是500
func respondFundOpErr(c *gin.Context, err error) {
	switch {
	case errors.Is(err, service.ErrAccountNotFound):
		failC(c, 400, ErrAccountNotFound, err.Error())
	case errors.Is(err, service.ErrInsufficientBalance):
		failC(c, 400, ErrInsufficientBalance, err.Error())
	case errors.Is(err, service.ErrIdempotencyConflict):
		failC(c, 400, ErrIdempotencyConflict, err.Error())
	default:
		fail(c, 500, err.Error())
	}
}

// 校验账户存在：不存在写好account_not_found响应并返回false。合作方必须先创建账户，其它接口
// 不会替他们悄悄建——否则uid手误写错的充值会成功地充给一个没人认领的账户，只读接口也会往库里
// 塞垃圾账户
func (s *Server) requireAccount(c *gin.Context, uid uint64) bool {
	return s.loadAccount(c, uid) != nil
}

// 校验账户存在并把账户返回，需要看账户状态(冻结)的接口用；失败时已经写好响应，返回nil
func (s *Server) loadAccount(c *gin.Context, uid uint64) *model.Account {
	account, err := s.accounts.Find(c.Request.Context(), uid)
	if err != nil {
		fail(c, 500, err.Error())
		return nil
	}
	if account == nil {
		failC(c, 400, ErrAccountNotFound, "account does not exist")
		return nil
	}
	return account
}

// 账户被冻结时拒绝新增风险的操作(开仓、条件开仓、改杠杆)：写好account_frozen响应并返回true
func rejectIfFrozen(c *gin.Context, account *model.Account) bool {
	if account.Status != model.AccountStatusFrozen {
		return false
	}
	failC(c, 400, ErrAccountFrozen, "account is frozen")
	return true
}

// 解析GET接口的uid参数并校验账户存在
func (s *Server) parseAccountUID(c *gin.Context) (uint64, bool) {
	uid, ok := parseUID(c)
	if !ok {
		return 0, false
	}
	if !s.requireAccount(c, uid) {
		return 0, false
	}
	return uid, true
}

type createAccountRequest struct {
	UID uint64 `json:"uid" binding:"required"`
}

// 创建账户的返回值：账户视图加上created标记
type createAccountResult struct {
	*service.AccountView
	Created bool `json:"created"` // true=这次新建的，false=账户之前就存在
}

// 创建账户。uid是合作方自己体系里的用户ID，直接沿用，重复创建天然幂等(返回已有账户，
// created=false)，超时重试没有风险
func (s *Server) createAccount(c *gin.Context) {
	var req createAccountRequest
	if !bindJSON(c, &req) {
		return
	}
	view, created, err := s.accounts.Create(c.Request.Context(), req.UID)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	ok(c, createAccountResult{AccountView: view, Created: created})
}

func (s *Server) adjustBalance(c *gin.Context) {
	var req adjustBalanceRequest
	if !bindJSON(c, &req) {
		return
	}
	requestID, msg := requireRequestID(req.RequestID)
	if msg != "" {
		fail(c, 400, msg)
		return
	}
	if req.Amount.IsZero() {
		fail(c, 400, "amount cannot be 0")
		return
	}
	replayed, err := s.accounts.AdjustBalance(c.Request.Context(), req.UID, req.Amount, requestID)
	if err != nil {
		respondFundOpErr(c, err)
		return
	}
	ok(c, fundOpResult{RequestID: requestID, Duplicate: replayed})
}

func (s *Server) accountInfo(c *gin.Context) {
	uid, ok1 := s.parseAccountUID(c)
	if !ok1 {
		return
	}
	view, err := s.accounts.View(c.Request.Context(), uid)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	ok(c, view)
}

type grantCreditRequest struct {
	UID       uint64          `json:"uid" binding:"required"`
	Amount    decimal.Decimal `json:"amount"`
	RequestID string          `json:"requestId"` // RequestID 必填的幂等键：发额度是累加操作，重试不带幂等键会让信用额度翻倍
}

// 合作方发放/追加信用额度(用户买保险后的赔付)，同一轮内可以多次调用、直接累加
func (s *Server) grantCredit(c *gin.Context) {
	var req grantCreditRequest
	if !bindJSON(c, &req) {
		return
	}
	requestID, msg := requireRequestID(req.RequestID)
	if msg != "" {
		fail(c, 400, msg)
		return
	}
	if req.Amount.Sign() <= 0 {
		fail(c, 400, "amount must be greater than 0")
		return
	}
	replayed, err := s.accounts.GrantCredit(c.Request.Context(), req.UID, req.Amount, requestID)
	if err != nil {
		respondFundOpErr(c, err)
		return
	}
	ok(c, fundOpResult{RequestID: requestID, Duplicate: replayed})
}

type setInsuredRequest struct {
	UID     uint64 `json:"uid" binding:"required"`
	Insured bool   `json:"insured"`
}

// 合作方单独设置这个账户本轮是否投保，跟发放信用额度是两个独立接口，互不联动
func (s *Server) setInsured(c *gin.Context) {
	var req setInsuredRequest
	if !bindJSON(c, &req) {
		return
	}
	if err := s.accounts.SetInsured(c.Request.Context(), req.UID, req.Insured); err != nil {
		respondFundOpErr(c, err)
		return
	}
	ok(c, nil)
}

type closeRoundRequest struct {
	UID   uint64 `json:"uid" binding:"required"`
	Round uint64 `json:"round" binding:"required"` // Round 必填，要结束的那一轮。轮数从1开始(0不是合法值)，required能直接拦住0。它同时是这个接口的幂等键：结束第N轮只会生效一次
}

type closeRoundResult struct {
	Round  uint64 `json:"round"`
	Status string `json:"status"` // submitted=已提交 / already_closed=这一轮之前已经结束过了，什么都没做
}

// 合作方通知本轮结束：撤销全部挂单、按标记价强平全部仓位、清算credit这几步都要
// 摸contract-engine内存里的订单簿/撮合状态，contract-api这边做不了，只能发Kafka事件路由
// 过去异步执行(跟撤单接口是同样的道理)——这里只做同步返回"请求已提交"。
//
// 必须指定要结束哪一轮：没有这个参数的话，合作方超时重试会在账户已经进入下一轮之后再结束一次，
// 把新一轮刚挂的单撤掉、刚开的仓强平、刚发的信用额度清零。round比账户当前的小说明这一轮已经
// 结束过，直接返回already_closed；比当前的大是不合法的请求
func (s *Server) closeRound(c *gin.Context) {
	var req closeRoundRequest
	if !bindJSON(c, &req) {
		return
	}
	round := req.Round
	account, err := s.accounts.Find(c.Request.Context(), req.UID)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	if account == nil {
		failC(c, 400, ErrAccountNotFound, "account does not exist")
		return
	}
	if round > account.Round {
		failC(c, 400, ErrRoundMismatch, "round cannot be greater than the account's current round")
		return
	}
	if round < account.Round {
		ok(c, closeRoundResult{Round: round, Status: "already_closed"})
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 发送关闭事件到kafka
	evt := events.RoundCloseEvent{UID: req.UID, Round: round}
	if err := s.producer.Publish(ctx, events.TopicRoundClose, strconv.FormatUint(req.UID, 10), evt); err != nil {
		fail(c, 500, err.Error())
		return
	}
	ok(c, closeRoundResult{Round: round, Status: "submitted"})
}

// side/action任何不认识的值都必须拒绝，不能放过去——matching.DirectionOf
// 对side/action只特判了(LONG,OPEN)和(SHORT,CLOSE)算买方，其它一律当卖方处理，一个拼错的
// side/action字符串会被悄悄撮合成方向相反的交易，而不是报错。普通委托(addOrder)和条件单
// (addConditionalOrder)共用同一套校验规则，避免两边各写一份、以后改一边漏改另一边
func validateSideAction(side model.Side, action model.OrderAction) string {
	if side != model.SideLong && side != model.SideShort {
		return "side is invalid"
	}
	if action != model.ActionOpen && action != model.ActionClose {
		return "action is invalid"
	}
	return ""
}

// 空值默认limit，非法值拒绝——普通委托和条件单共用
func resolveOrderType(reqType model.OrderType) (model.OrderType, string) {
	orderType := reqType
	if orderType == "" {
		orderType = model.OrderTypeLimit
	}
	if orderType != model.OrderTypeLimit && orderType != model.OrderTypeMarket {
		return "", "type is invalid"
	}
	return orderType, ""
}

// maxSaneLeverage是不区分开平仓、不查分档配置的兜底上限——只用来挡掉明显
// 离谱/会导致uint32(leverage.IntPart())溢出截断成垃圾值的输入。真正按分档算出来的
// tier.MaxLeverage只在ActionOpen分支里校验，是刻意的：leverage这个字段只有开仓会用来算
// 需要冻结多少保证金，平仓不创建新仓位/新风险，不需要经过分档校验，而且分档校验依赖
// risk_limit_tiers有配置，一旦要求平仓也必须过这一关，配置缺失或删除时反而会把用户已有
// 仓位卡死平不掉——两害相权，让平仓单的leverage只挡这道跟合约配置无关的离谱值兜底。
// 必须是整数：leverage落库时是uint32(order.Leverage)，settlement.go计算仓位保证金时
// 也是按这个落库的整数杠杆重算(不是按下单时这个decimal算requiredMargin用的原始精度值)，
// 放行小数杠杆会导致冻结保证金和落库/后续结算的杠杆口径对不上，污染position_margin/
// ROE/预估强平价——普通委托和条件单共用同一套规则
func resolveLeverage(reqLeverage *decimal.Decimal) (decimal.Decimal, string) {
	leverage := decimal.NewFromInt(1)
	if reqLeverage != nil {
		leverage = *reqLeverage
	}
	const maxSaneLeverage = 1000
	if leverage.Sign() <= 0 || leverage.GreaterThan(decimal.NewFromInt(maxSaneLeverage)) || !leverage.IsInteger() {
		return decimal.Zero, "leverage is invalid"
	}
	return leverage, ""
}

// 算这个uid在symbol+side方向上"已经占用/即将占用"的名义价值：已成交
// 仓位 + 排队中的普通开仓委托 + 排队中的条件开仓委托(触发后会变成普通开仓委托，同样会真实
// 占用仓位)。分档杠杆校验必须把这三者都算进去，不然可以用"一部分普通单、一部分条件单"
// 拆开下，绕开单独统计任何一种委托类型的分档校验——这是对之前那次"排队单不计入分档"漏洞
// 修复的延伸，条件单是后加的委托类型，同样的口径必须覆盖到
func (s *Server) existingOpenNotional(ctx context.Context, uid uint64, symbol string, side model.Side, hasMark bool, mark decimal.Decimal) (decimal.Decimal, error) {
	existingNotional := decimal.Zero
	existing, err := s.positions.Find(ctx, uid, symbol, side)
	if err != nil {
		return decimal.Zero, err
	}
	if existing != nil {
		// 用标记价估这个已有仓位当前值多少钱，没有标记价才退回持仓均价——不能用这笔新委托
		// 自己填的价格估：限价单的价格是用户随便填的，可以故意报一个远低于市价的价格，
		// 把existingNotional算得远小于真实值，从而蹭到一个本不该适用的低档高杠杆
		valuePrice := existing.AvgEntryPrice
		if hasMark {
			valuePrice = mark
		}
		existingNotional = existing.Volume.Mul(valuePrice)
	}
	activeOrders, err := s.orders.FindActiveByUID(ctx, uid, symbol)
	if err != nil {
		return decimal.Zero, err
	}
	for _, o := range activeOrders {
		if o.Side == side && o.Action == model.ActionOpen {
			// 这里必须用挂单自己的o.Price，不能像上面现有仓位那样退回标记价：这些是已经
			// 通过校验、真实挂在簿子上的委托，o.Price不是"用户随便填的、可能被拿来做局的
			// 报价"，而是它成交时会用到的真实价格(排队单成交价=挂单自己的限价，不是标记价)
			existingNotional = existingNotional.Add(o.RemainingAmount().Mul(o.Price))
		}
	}
	activeConditional, err := s.conditionalOrders.FindActiveByUID(ctx, uid, symbol)
	if err != nil {
		return decimal.Zero, err
	}
	for _, co := range activeConditional {
		if co.Side == side && co.Action == model.ActionOpen {
			// 条件单触发后按什么价格提交：LIMIT用co.Price(委托人自己指定的执行价，不是随便
			// 填的)，MARKET没有"未来的标记价"可用，退回用触发价trigger_price——同样是委托人
			// 自己指定、真实会用来判断触发的价格，不是可以随意做局的值
			estimatePrice := co.Price
			if co.Type == model.OrderTypeMarket {
				estimatePrice = co.TriggerPrice
			}
			existingNotional = existingNotional.Add(co.Amount.Mul(estimatePrice))
		}
	}
	return existingNotional, nil
}

type addOrderRequest struct {
	UID          uint64            `json:"uid" binding:"required"`    // UID
	Symbol       string            `json:"symbol" binding:"required"` // 币种
	Side         model.Side        `json:"side" binding:"required"`   // long/short
	Action       model.OrderAction `json:"action" binding:"required"` // open/close
	Type         model.OrderType   `json:"type"`                      // limit/market，默认值是limit
	Leverage     *decimal.Decimal  `json:"leverage"`                  // 默认值是1
	Price        decimal.Decimal   `json:"price"`                     // 限价单才填，市价单不用填
	MarginAmount *decimal.Decimal  `json:"marginAmount"`              // 与Amount是二选一的关系
	Amount       *decimal.Decimal  `json:"amount"`                    // 与MarginAmount是二选一的关系
	ReduceOnly   bool              `json:"reduceOnly"`                // 默认值false
	RequestID    string            `json:"requestId"`                 // RequestID 合作方自己生成的幂等键：同一uid下重复提交同一个值不会产生第二笔委托
}

// 下单/创建条件单成功的返回值。orderId是字符串(雪花ID超过JS安全整数范围)
type placeOrderResult struct {
	OrderID   uint64 `json:"orderId,string"`
	RequestID string `json:"requestId,omitempty"`
	Duplicate bool   `json:"duplicate,omitempty"` // true=这个requestId之前已经提交过，返回的是原来那笔
}

func strPtr(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// 从落库的委托构造发给撮合引擎的下单事件——首次下单和幂等重试补发共用
func orderSubmitEvent(o *model.Order) events.OrderSubmitEvent {
	return events.OrderSubmitEvent{
		OrderID:    o.OrderID,
		UID:        o.UID,
		Symbol:     o.Symbol,
		Side:       string(o.Side),
		Action:     string(o.Action),
		Type:       string(o.Type),
		Price:      o.Price.String(),
		Amount:     o.Amount.String(),
		Leverage:   o.Leverage,
		ReduceOnly: o.ReduceOnly,
	}
}

func (s *Server) publishOrderSubmit(o *model.Order) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.producer.Publish(ctx, events.TopicOrderSubmit, o.Symbol, orderSubmitEvent(o))
}

// 同一个uid+requestId已经有一笔委托了：返回原来那笔的orderId。
// 如果那笔还是刚落库、没被引擎处理过的open状态(TradedAmount=0)，重新发一次下单事件——上一次
// 请求可能正好卡在"委托已落库、发往Kafka失败"这一步，合作方超时后带同一个requestId重试，
// 这里就能把它补发出去。补发是安全的：EngineService.SubmitOrder对已终结/已经在订单簿上的
// 委托会直接跳过，不会二次撮合
func (s *Server) respondDuplicateOrder(c *gin.Context, existing *model.Order, requestHash string) {
	if idempotencyConflict(existing.RequestHash, requestHash) {
		failC(c, 400, ErrIdempotencyConflict, "requestId has already been used for a request with different parameters")
		return
	}
	if existing.Status == model.OrderStatusOpen && existing.TradedAmount.IsZero() {
		if err := s.publishOrderSubmit(existing); err != nil {
			failC(c, 500, ErrDispatchFailed, "order was saved but failed to dispatch to the matching engine: "+err.Error())
			return
		}
	}
	ok(c, placeOrderResult{OrderID: existing.OrderID, RequestID: derefStr(existing.RequestID), Duplicate: true})
}

// 同一个uid+requestId已经有一笔条件单了：参数一致就返回原来那笔的orderId，不一致是误用。
// 条件单没有"落库了但没发出去"的中间状态(创建时只写MySQL，触发由引擎扫描)，所以不需要像
// 普通委托那样补发
func (s *Server) respondDuplicateConditional(c *gin.Context, existing *model.ConditionalOrder, requestHash string) {
	if idempotencyConflict(existing.RequestHash, requestHash) {
		failC(c, 400, ErrIdempotencyConflict, "requestId has already been used for a request with different parameters")
		return
	}
	ok(c, placeOrderResult{OrderID: existing.OrderID, RequestID: derefStr(existing.RequestID), Duplicate: true})
}

// 落库失败/撞幂等键唯一索引时，把这次请求刚刚冻结的保证金退回去
func (s *Server) rollbackFreeze(ctx context.Context, uid uint64, fr service.FreezeResult) {
	if fr.FromAvailable.Sign() <= 0 && fr.FromCredit.Sign() <= 0 {
		return
	}
	if err := s.accounts.UnfreezeMargin(ctx, uid, fr.FromAvailable, fr.FromCredit); err != nil {
		log.Printf("[ERROR] failed to roll back frozen margin, uid=%d, fromBalance=%s, fromCredit=%s: %v", uid, fr.FromAvailable, fr.FromCredit, err)
	}
}

func (s *Server) addOrder(c *gin.Context) {
	var req addOrderRequest
	if !bindJSON(c, &req) {
		return
	}
	account := s.loadAccount(c, req.UID)
	if account == nil {
		return
	}
	uid := req.UID
	symbol := req.Symbol
	side := req.Side
	action := req.Action
	if msg := validateSideAction(side, action); msg != "" {
		fail(c, 400, msg)
		return
	}
	orderType, msg := resolveOrderType(req.Type)
	if msg != "" {
		fail(c, 400, msg)
		return
	}
	leverage, msg := resolveLeverage(req.Leverage)
	if msg != "" {
		fail(c, 400, msg)
		return
	}
	requestID, msg := normalizeRequestID(req.RequestID)
	if msg != "" {
		fail(c, 400, msg)
		return
	}
	requestHash := service.RequestFingerprint(
		"order",
		strconv.FormatUint(uid, 10), symbol, string(side),
		string(action),
		string(orderType), req.Price.String(),
		decPtrStr(req.Amount),
		decPtrStr(req.MarginAmount),
		leverage.String(), strconv.FormatBool(req.ReduceOnly))
	if requestID != "" {
		existing, err := s.orders.FindByRequestID(c.Request.Context(), uid, requestID)
		if err != nil {
			fail(c, 500, err.Error())
			return
		}
		if existing != nil {
			s.respondDuplicateOrder(c, existing, requestHash)
			return
		}
	}
	// 冻结检查放在幂等重放之后：冻结前已经成功的下单，带同一个requestId重试仍然返回原结果
	if action == model.ActionOpen && rejectIfFrozen(c, account) {
		return
	}

	coin, err := s.coins.FindBySymbol(c.Request.Context(), symbol)
	if err != nil || coin == nil || !coin.Enable {
		failC(c, 400, ErrSymbolNotFound, "contract does not exist or is disabled")
		return
	}
	// 只有MARKET单定价、开仓分档判断(含价格保护带，只对开仓生效)这两处要用标记价格，
	// LIMIT+CLOSE(平仓最常见的形态)完全用不上，按需取一次就好；但凡要用就只取这一次、
	// 全函数复用同一个值，避免读两次中间恰好更新导致不同判断用了不一致的值
	var mark decimal.Decimal
	var hasMark bool
	if orderType == model.OrderTypeMarket || action == model.ActionOpen {
		mark, hasMark = s.markPrice.Get(c.Request.Context(), symbol)
	}
	// referencePrice/hasReference是"标记价格不存在就退回指数价格"这个兜底逻辑的唯一实现，
	// 价格保护带、下面SHORT+OPEN的保守计价(orderNotionalPrice)都要用同一份，不能各写各的：
	// 之前orderNotionalPrice那处只判断hasMark、没有这个指数价兜底，导致一个"从没成交过但有
	// 指数价"的全新symbol上，价格保护带能拦住离谱吃单价，保守计价却拦不住，两处本该一致的
	// 防护基准不一致
	referencePrice, hasReference := mark, hasMark
	if !hasReference {
		referencePrice, hasReference = s.markPrice.GetIndexPrice(c.Request.Context(), symbol)
	}

	var price decimal.Decimal
	if orderType == model.OrderTypeLimit {
		price = req.Price
		if price.Sign() <= 0 {
			fail(c, 400, "price is invalid for a limit order")
			return
		}
		if coin.PriceTick.Sign() > 0 && !price.Mod(coin.PriceTick).IsZero() {
			failC(c, 400, ErrPriceTickInvalid, fmt.Sprintf("price must be a multiple of the tick size %s", coin.PriceTick))
			return
		}
		// 价格保护带：只对开仓单生效，防止两类问题——①用户瞎填价格导致的胖手指交易 ②故意报
		// 一个远离市价的"吃单价"去钻空子(挂单实际会按盘口对手的真实价格成交，不是按这个填的价，
		// contract-api这边在撮合之前根本不知道真实成交价是多少，靠这道带宽把两者的差距限制在
		// 一个可控范围内)。平仓单不冻结保证金(见下面action==ActionOpen那个分支)，钻这个空子
		// 对平仓单没有意义，反而如果对平仓单也校验，行情剧烈波动、标记价格滞后时会把用户想
		// 平仓离场的委托也挡在外面——跟开仓杠杆校验刻意放过平仓单是同一个道理。参考价优先用
		// 标记价格，标记价格不存在(这个symbol从没成交过)就退回用指数价格；两个都没有(全新
		// symbol、也没人喂过指数价)就没有参考基准，放行不校验，这是唯一防不住的缺口
		if action == model.ActionOpen && hasReference && referencePrice.Sign() > 0 && coin.PriceProtectionRatio.Sign() > 0 {
			deviation := price.Sub(referencePrice).Abs().Div(referencePrice)
			if deviation.GreaterThan(coin.PriceProtectionRatio) {
				failC(c, 400, ErrPriceOutOfRange, "order price deviates too far from the reference price")
				return
			}
		}
	} else {
		if !hasMark {
			failC(c, 400, ErrNoMarkPrice, "this contract has no mark price yet")
			return
		}
		price = mark
	}

	var amount decimal.Decimal
	switch {
	case req.MarginAmount != nil && req.Amount != nil:
		fail(c, 400, "marginAmount and amount cannot be provided together")
		return
	case req.MarginAmount != nil:
		if req.MarginAmount.Sign() <= 0 {
			fail(c, 400, "marginAmount is invalid")
			return
		}
		notional := req.MarginAmount.Mul(leverage)
		amount = notional.Div(price).Truncate(coin.BaseCoinScale)
	case req.Amount != nil:
		amount = *req.Amount
	default:
		fail(c, 400, "either marginAmount or amount is required")
		return
	}
	if amount.Sign() <= 0 {
		fail(c, 400, "amount must be greater than 0")
		return
	}
	if coin.MinVolume.Sign() > 0 && amount.LessThan(coin.MinVolume) {
		failC(c, 400, ErrVolumeOutOfRange, fmt.Sprintf("amount cannot be less than the minimum order size %s for this contract", coin.MinVolume))
		return
	}
	if coin.MaxVolume.Sign() > 0 && amount.GreaterThan(coin.MaxVolume) {
		failC(c, 400, ErrVolumeOutOfRange, fmt.Sprintf("amount cannot exceed the per-order limit %s for this contract", coin.MaxVolume))
		return
	}
	if coin.VolumeStep.Sign() > 0 && !amount.Mod(coin.VolumeStep).IsZero() {
		failC(c, 400, ErrVolumeOutOfRange, fmt.Sprintf("amount must be a multiple of the volume step %s", coin.VolumeStep))
		return
	}

	// orderNotionalPrice是这笔委托自己名义价值/冻结保证金的估值基准，只对SHORT+OPEN生效：
	// SHORT+OPEN在撮合引擎里是卖方向，挂一个远低于市价的价格属于"吃单价"，会立刻按盘口
	// 对手的真实价格成交，不是按这个填的低价，所以要用max(price, 参考价)取更保守的
	// 那个，冻结保证金/分档校验都按这个来，不能只信submitted price。LONG+OPEN反过来：
	// 远低于市价的价格是完全合法的被动挂单(买跌)，只会按这个低价成交，如果同样套
	// max(price,参考价)会把这类正常订单的保证金/名义价值算得比真实值更大——这里用
	// referencePrice(标记价格优先、没有就退回指数价格)而不是裸的mark，理由跟上面价格
	// 保护带一致：两处本该是同一套"有没有可信参考价"的判断，用不同的判断口径会让"有指数价
	// 但从没成交过"的全新symbol上，价格保护带能拦住的离谱吃单价，这里却拦不住。两者都没有
	// (全新symbol、也没人喂过指数价)才是真正防不住的缺口
	orderNotionalPrice := price
	if side == model.SideShort && hasReference && referencePrice.GreaterThan(price) {
		orderNotionalPrice = referencePrice
	}
	// 冻结保证金用orderNotionalPrice而不是price本身：SHORT+OPEN报一个远低于市价的吃单价，
	// 真实会按对手的高价成交，如果冻结按这个低价算，会把这笔仓位真实该占用的保证金严重
	// 低估。这里先按保守估计冻结，等真正成交、知道真实成交价之后，settlement.go的
	// SettleFill会用真实成交价重算，多退少补，不会让这部分差额一直悬在frozen_margin里
	requiredMargin := amount.Mul(orderNotionalPrice).Div(leverage)
	var freezeResult service.FreezeResult
	if action == model.ActionOpen {
		// 分档判断的名义价值不能只看已成交仓位：这个uid在同一symbol+side上如果还挂着别的没成交的
		// 开仓单，每一笔单独提交时都看不到彼此，会各自按"当前还没有仓位/挂单垫底"通过校验，等
		// 行情走到这些价位一起成交，合并起来的真实仓位可能远超单笔校验时的档位——顺序提交多笔
		// 远离盘口的限价单就能稳定触发，所以这里除了已成交仓位，还要把这个方向上全部还在排队的
		// OPEN单也算进去。"读现有仓位/挂单→算档位→冻结保证金"这段临界区不是天然原子的，靠
		// s.lock.WithLock按uid+symbol+side序列化并发请求来保证原子性，详见
		// docs/risk-limit-tiers.md"并发下单的原子性"一节——不是进程内mutex，因为
		// contract-api是无状态服务、允许多实例水平扩展(docs/architecture.md)，进程内锁
		// 只能防住单实例内部的竞态
		lockErr := s.lock.WithLock(c.Request.Context(), service.OrderLockKey(uid, symbol, side), func() error {
			existingNotional, err := s.existingOpenNotional(c.Request.Context(), uid, symbol, side, hasMark, mark)
			if err != nil {
				return err
			}
			// 这笔新委托自己的名义价值用orderNotionalPrice(上面已经算好，SHORT+OPEN时是
			// max(price,markPrice))，跟冻结保证金用的是同一个基准，两处口径必须一致
			tier, err := s.positions.TierFor(c.Request.Context(), symbol, existingNotional.Add(amount.Mul(orderNotionalPrice)))
			if err != nil {
				return err
			}
			if tier == nil {
				return newHTTPError(400, ErrTierNotConfigured, "this contract has no risk limit tier configured, opening positions is not allowed")
			}
			if leverage.GreaterThan(decimal.NewFromInt(int64(tier.MaxLeverage))) {
				return newHTTPError(400, ErrLeverageExceedsTier, "leverage exceeds the maximum allowed for the tier at the current position notional")
			}
			result, err := s.accounts.FreezeMargin(c.Request.Context(), uid, requiredMargin)
			if err != nil {
				return err
			}
			freezeResult = result
			return nil
		})
		if lockErr != nil {
			// 带requestId的并发重复请求：赢家已经把余额冻结走了，其它请求会在冻结这一步就
			// 因为余额不足/锁忙失败，走不到后面的唯一索引。这种失败不是"这笔请求本身有问题"，
			// 而是"这个requestId已经有人在处理了"，重查一次，查到了就按重复请求返回原委托
			if requestID != "" {
				if existing, findErr := s.orders.FindByRequestID(c.Request.Context(), uid, requestID); findErr == nil && existing != nil {
					s.respondDuplicateOrder(c, existing, requestHash)
					return
				}
			}
			respondLockErr(c, lockErr)
			return
		}
	}

	orderID := service.NextID()
	now := service.NowMillis()
	o := &model.Order{
		OrderID:     orderID,
		UID:         uid,
		Symbol:      symbol,
		Side:        side,
		Action:      action,
		Type:        orderType,
		Price:       price,
		Amount:      amount,
		Leverage:    uint32(leverage.IntPart()),
		ReduceOnly:  req.ReduceOnly,
		Status:      model.OrderStatusOpen,
		CreateTime:  now,
		UpdateTime:  now,
		RequestID:   strPtr(requestID),
		RequestHash: hashIfKeyed(requestID, requestHash),
	}
	if action == model.ActionOpen {
		o.FrozenMargin = freezeResult.FromAvailable
		o.FrozenCredit = freezeResult.FromCredit
	}
	if err := s.orders.Insert(c.Request.Context(), o); err != nil {
		// 撞了(uid, request_id)唯一索引：两个带同一个requestId的请求并发通过了上面的
		// 预检，另一个先落库了。这笔请求刚冻结的保证金必须退回，再按重复请求处理
		if requestID != "" && repo.IsDuplicateKey(err) {
			s.rollbackFreeze(c.Request.Context(), uid, freezeResult)
			existing, findErr := s.orders.FindByRequestID(c.Request.Context(), uid, requestID)
			if findErr == nil && existing != nil {
				s.respondDuplicateOrder(c, existing, requestHash)
				return
			}
		}
		fail(c, 500, err.Error())
		return
	}

	if err := s.publishOrderSubmit(o); err != nil {
		failC(c, 500, ErrDispatchFailed, "order was saved but failed to dispatch to the matching engine: "+err.Error())
		return
	}
	ok(c, placeOrderResult{OrderID: orderID, RequestID: requestID})
}

type cancelOrderRequest struct {
	UID uint64 `json:"uid" binding:"required"`
}

func (s *Server) cancelOrder(c *gin.Context) {
	var req cancelOrderRequest
	if !bindJSON(c, &req) {
		return
	}
	uid := req.UID
	orderID, err := decimal.NewFromString(c.Param("orderId"))
	if err != nil {
		fail(c, 400, "orderId is invalid")
		return
	}
	o, err := s.orders.FindByOrderID(c.Request.Context(), uint64(orderID.IntPart()))
	if err != nil || o == nil || o.UID != uid {
		failC(c, 400, ErrOrderNotFound, "order does not exist")
		return
	}
	if !model.ActiveOrderStatuses[o.Status] {
		failC(c, 400, ErrOrderNotCancelable, "order is already filled or canceled")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 发送订单取消事件到kafka
	evt := events.OrderCancelEvent{OrderID: o.OrderID, UID: uid, Symbol: o.Symbol}
	if err := s.producer.Publish(ctx, events.TopicOrderCancel, o.Symbol, evt); err != nil {
		fail(c, 500, err.Error())
		return
	}
	ok(c, "cancel request submitted")
}

func (s *Server) orderCurrent(c *gin.Context) {
	uid, ok1 := s.parseAccountUID(c)
	if !ok1 {
		return
	}
	orders, err := s.orders.FindActiveByUID(c.Request.Context(), uid, c.Query("symbol"))
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	ok(c, orders)
}

func (s *Server) orderHistory(c *gin.Context) {
	uid, ok1 := s.parseAccountUID(c)
	if !ok1 {
		return
	}
	limit, before, ok2 := pageParams(c)
	if !ok2 {
		return
	}
	orders, err := s.orders.FindHistoryByUID(c.Request.Context(), uid, limit, before)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	ok(c, orders)
}

type addConditionalOrderRequest struct {
	UID              uint64                 `json:"uid" binding:"required"`
	Symbol           string                 `json:"symbol" binding:"required"`
	Side             model.Side             `json:"side" binding:"required"`
	Action           model.OrderAction      `json:"action" binding:"required"`
	TriggerPrice     decimal.Decimal        `json:"triggerPrice" binding:"required"`
	TriggerDirection model.TriggerDirection `json:"triggerDirection" binding:"required"`
	Type             model.OrderType        `json:"type"`
	Leverage         *decimal.Decimal       `json:"leverage"`
	Price            decimal.Decimal        `json:"price"`
	MarginAmount     *decimal.Decimal       `json:"marginAmount"`
	Amount           *decimal.Decimal       `json:"amount"`
	ReduceOnly       bool                   `json:"reduceOnly"`
	RequestID        string                 `json:"requestId"` // 幂等键，语义同addOrderRequest.RequestID
}

// 创建条件单(止盈止损/条件开仓)：只落库到conditional_orders表，不进
// 撮合引擎的订单簿、不发Kafka事件——触发前这笔"委托"只是一个记在数据库里的条件，真正提交
// 撮合是contract-engine那边的ConditionalOrderService定时扫描标记价格触发之后的事，见
// docs/conditional-orders.md。校验链尽量复用addOrder的逻辑，两个关键差异：
//  1. 不做价格保护带校验——委托执行价(LIMIT类型的price)本来就该跟触发价接近、可能远离
//     当前标记价，这是条件单存在的意义，拿当前标记价去校验没有意义
//  2. 分档校验/保证金冻结用的"名义价值估值基准"，MARKET类型没有"可预知的未来标记价"，
//     退回用triggerPrice本身估——委托人自己指定、真实会用来判断触发的价格，不是随便填的
func (s *Server) addConditionalOrder(c *gin.Context) {
	var req addConditionalOrderRequest
	if !bindJSON(c, &req) {
		return
	}
	account := s.loadAccount(c, req.UID)
	if account == nil {
		return
	}
	uid := req.UID
	symbol := req.Symbol
	side := req.Side
	action := req.Action
	if msg := validateSideAction(side, action); msg != "" {
		fail(c, 400, msg)
		return
	}
	if req.TriggerDirection != model.TriggerGTE && req.TriggerDirection != model.TriggerLTE {
		fail(c, 400, "triggerDirection is invalid")
		return
	}
	if req.TriggerPrice.Sign() <= 0 {
		fail(c, 400, "triggerPrice is invalid")
		return
	}
	orderType, msg := resolveOrderType(req.Type)
	if msg != "" {
		fail(c, 400, msg)
		return
	}
	leverage, msg := resolveLeverage(req.Leverage)
	if msg != "" {
		fail(c, 400, msg)
		return
	}
	requestID, msg := normalizeRequestID(req.RequestID)
	if msg != "" {
		fail(c, 400, msg)
		return
	}
	requestHash := service.RequestFingerprint(
		"conditional",
		strconv.FormatUint(uid, 10),
		symbol,
		string(side),
		string(action),
		string(orderType),
		req.TriggerPrice.String(),
		string(req.TriggerDirection),
		req.Price.String(),
		decPtrStr(req.Amount),
		decPtrStr(req.MarginAmount),
		leverage.String(),
		strconv.FormatBool(req.ReduceOnly))
	if requestID != "" {
		existing, err := s.conditionalOrders.FindByRequestID(c.Request.Context(), uid, requestID)
		if err != nil {
			fail(c, 500, err.Error())
			return
		}
		if existing != nil {
			s.respondDuplicateConditional(c, existing, requestHash)
			return
		}
	}
	if action == model.ActionOpen && rejectIfFrozen(c, account) {
		return
	}

	coin, err := s.coins.FindBySymbol(c.Request.Context(), symbol)
	if err != nil || coin == nil || !coin.Enable {
		failC(c, 400, ErrSymbolNotFound, "contract does not exist or is disabled")
		return
	}
	if coin.PriceTick.Sign() > 0 && !req.TriggerPrice.Mod(coin.PriceTick).IsZero() {
		failC(c, 400, ErrPriceTickInvalid, fmt.Sprintf("triggerPrice must be a multiple of the tick size %s", coin.PriceTick))
		return
	}
	// referencePrice/hasReference：标记价格优先、缺失退回指数价格，跟addOrder用的是
	// 同一套兜底逻辑——下面SHORT+OPEN的保守计价基准要用它
	mark, hasMark := s.markPrice.Get(c.Request.Context(), symbol)
	referencePrice, hasReference := mark, hasMark
	if !hasReference {
		referencePrice, hasReference = s.markPrice.GetIndexPrice(c.Request.Context(), symbol)
	}

	// price是触发后委托的执行价——LIMIT类型必填，MARKET类型触发时按当时的标记价成交，
	// 这里不需要也不应该预先填一个价格(未来触发时刻的标记价现在还不知道)
	var price decimal.Decimal
	if orderType == model.OrderTypeLimit {
		price = req.Price
		if price.Sign() <= 0 {
			fail(c, 400, "price is invalid for a limit order")
			return
		}
		if coin.PriceTick.Sign() > 0 && !price.Mod(coin.PriceTick).IsZero() {
			failC(c, 400, ErrPriceTickInvalid, fmt.Sprintf("price must be a multiple of the tick size %s", coin.PriceTick))
			return
		}
	}
	// estimatePrice是这笔条件单在"触发之前"唯一能拿到的、委托人自己指定的价格基准——
	// LIMIT用触发后要执行的price，MARKET没有这个值就退回triggerPrice，marginAmount
	// 换算数量统一用这一个基准(不能用下面bump过的orderNotionalPrice——用户要的是
	// "这些保证金、这个杠杆对应多少数量"，不该被保守估计放大)
	estimatePrice := price
	if orderType == model.OrderTypeMarket {
		estimatePrice = req.TriggerPrice
	}

	var amount decimal.Decimal
	switch {
	case req.MarginAmount != nil && req.Amount != nil:
		fail(c, 400, "marginAmount and amount cannot be provided together")
		return
	case req.MarginAmount != nil:
		if req.MarginAmount.Sign() <= 0 {
			fail(c, 400, "marginAmount is invalid")
			return
		}
		notional := req.MarginAmount.Mul(leverage)
		amount = notional.Div(estimatePrice).Truncate(coin.BaseCoinScale)
	case req.Amount != nil:
		amount = *req.Amount
	default:
		fail(c, 400, "either marginAmount or amount is required")
		return
	}
	if amount.Sign() <= 0 {
		fail(c, 400, "amount must be greater than 0")
		return
	}
	if coin.MinVolume.Sign() > 0 && amount.LessThan(coin.MinVolume) {
		failC(c, 400, ErrVolumeOutOfRange, fmt.Sprintf("amount cannot be less than the minimum order size %s for this contract", coin.MinVolume))
		return
	}
	if coin.MaxVolume.Sign() > 0 && amount.GreaterThan(coin.MaxVolume) {
		failC(c, 400, ErrVolumeOutOfRange, fmt.Sprintf("amount cannot exceed the per-order limit %s for this contract", coin.MaxVolume))
		return
	}
	if coin.VolumeStep.Sign() > 0 && !amount.Mod(coin.VolumeStep).IsZero() {
		failC(c, 400, ErrVolumeOutOfRange, fmt.Sprintf("amount must be a multiple of the volume step %s", coin.VolumeStep))
		return
	}

	// orderNotionalPrice是分档校验/冻结保证金的估值基准，只对SHORT+OPEN生效，理由跟
	// addOrder的orderNotionalPrice完全一样(那边有详细注释)：LIMIT类型的price是委托人
	// 自己填的执行价，可以故意填得远低于参考价——触发后这笔委托会以真正的LIMIT SELL
	// 身份提交撮合，立刻按盘口对手的真实(更高)价格成交，不是按这个填的低价成交，分档/
	// 冻结保证金按委托价算会严重低估真实风险。这是addOrder那次修复(commit 5cece54)在
	// 条件单这条新路径上必须同步补上的同一处，不能因为多了"触发"这一步中间状态就漏掉
	orderNotionalPrice := estimatePrice
	if side == model.SideShort && hasReference && referencePrice.GreaterThan(estimatePrice) {
		orderNotionalPrice = referencePrice
	}

	requiredMargin := amount.Mul(orderNotionalPrice).Div(leverage)
	var freezeResult service.FreezeResult
	if action == model.ActionOpen {
		// 跟addOrder同一处临界区、同一把锁(按uid+symbol+side)，理由见addOrder里的详细注释：
		// 条件单触发前的"占坑"校验一样要防并发绕开保证金分档限制
		lockErr := s.lock.WithLock(c.Request.Context(), service.OrderLockKey(uid, symbol, side), func() error {
			existingNotional, err := s.existingOpenNotional(c.Request.Context(), uid, symbol, side, hasMark, mark)
			if err != nil {
				return err
			}
			tier, err := s.positions.TierFor(c.Request.Context(), symbol, existingNotional.Add(amount.Mul(orderNotionalPrice)))
			if err != nil {
				return err
			}
			if tier == nil {
				return newHTTPError(400, ErrTierNotConfigured, "this contract has no risk limit tier configured, opening positions is not allowed")
			}
			if leverage.GreaterThan(decimal.NewFromInt(int64(tier.MaxLeverage))) {
				return newHTTPError(400, ErrLeverageExceedsTier, "leverage exceeds the maximum allowed for the tier at the current position notional")
			}
			result, err := s.accounts.FreezeMargin(c.Request.Context(), uid, requiredMargin)
			if err != nil {
				return err
			}
			freezeResult = result
			return nil
		})
		if lockErr != nil {
			// 理由同addOrder：并发重复请求在冻结保证金这一步失败，查到已有就按重复请求返回
			if requestID != "" {
				if existing, findErr := s.conditionalOrders.FindByRequestID(c.Request.Context(), uid, requestID); findErr == nil && existing != nil {
					s.respondDuplicateConditional(c, existing, requestHash)
					return
				}
			}
			respondLockErr(c, lockErr)
			return
		}
	}

	orderID := service.NextID()
	now := service.NowMillis()
	co := &model.ConditionalOrder{
		OrderID:          orderID,
		UID:              uid,
		Symbol:           symbol,
		Side:             side,
		Action:           action,
		TriggerPrice:     req.TriggerPrice,
		TriggerDirection: req.TriggerDirection,
		Type:             orderType,
		Price:            price,
		Amount:           amount,
		Leverage:         uint32(leverage.IntPart()),
		ReduceOnly:       req.ReduceOnly,
		Status:           model.ConditionalStatusPending,
		CreateTime:       now,
		UpdateTime:       now,
		RequestID:        strPtr(requestID),
		RequestHash:      hashIfKeyed(requestID, requestHash),
	}
	if action == model.ActionOpen {
		co.FrozenMargin = freezeResult.FromAvailable
		co.FrozenCredit = freezeResult.FromCredit
	}
	if err := s.conditionalOrders.Insert(c.Request.Context(), co); err != nil {
		if requestID != "" && repo.IsDuplicateKey(err) {
			s.rollbackFreeze(c.Request.Context(), uid, freezeResult)
			existing, findErr := s.conditionalOrders.FindByRequestID(c.Request.Context(), uid, requestID)
			if findErr == nil && existing != nil {
				s.respondDuplicateConditional(c, existing, requestHash)
				return
			}
		}
		fail(c, 500, err.Error())
		return
	}
	// 落库后再看一眼账户状态：冻结接口的清理可能刚好在这笔落库之前扫完，这笔条件开仓单就漏在
	// 清理之外了。引擎侧触发时也会兜底撤掉，但那要等到触发，这段时间保证金一直被占着、单子一直显示
	// 待触发，这里直接撤销、返回account_frozen更干净
	if action == model.ActionOpen {
		frozen, err := s.accounts.IsFrozen(c.Request.Context(), uid)
		if err != nil {
			fail(c, 500, err.Error())
			return
		}
		if frozen {
			if err := s.cancelPendingConditional(c.Request.Context(), *co); err != nil {
				log.Printf("[ERROR] failed to cancel the just-created conditional open order during a freeze race, orderId=%d: %v", co.OrderID, err)
			}
			failC(c, 400, ErrAccountFrozen, "account is frozen, cannot create a conditional open order")
			return
		}
	}
	ok(c, placeOrderResult{OrderID: orderID, RequestID: requestID})
}

type cancelConditionalOrderRequest struct {
	UID uint64 `json:"uid" binding:"required"`
}

// 撤销一笔还没触发的条件单——全程只碰MySQL，不需要像普通委托撤单
// 那样经Kafka路由给contract-engine：条件单触发前从来没进过撮合引擎的内存订单簿，没有
// 什么可摘的，直接原子标记取消+退回冻结保证金即可，比普通撤单更简单
func (s *Server) cancelConditionalOrder(c *gin.Context) {
	var req cancelConditionalOrderRequest
	if !bindJSON(c, &req) {
		return
	}
	orderID, err := decimal.NewFromString(c.Param("orderId"))
	if err != nil {
		fail(c, 400, "orderId is invalid")
		return
	}
	co, err := s.conditionalOrders.FindByOrderID(c.Request.Context(), uint64(orderID.IntPart()))
	if err != nil || co == nil || co.UID != req.UID {
		failC(c, 400, ErrOrderNotFound, "conditional order does not exist")
		return
	}
	if co.Status != model.ConditionalStatusPending {
		failC(c, 400, ErrOrderNotCancelable, "conditional order has already triggered or been canceled")
		return
	}
	if err := s.cancelPendingConditional(c.Request.Context(), *co); err != nil {
		if errors.Is(err, errConditionalNotPending) {
			// 撤单请求跟engine那边的触发扫描并发竞争，扫描先一步赢了——这笔条件单已经变成了真正的委托，不能再当"条件单撤销"处理
			failC(c, 400, ErrOrderNotCancelable, "conditional order has already triggered")
			return
		}
		fail(c, 500, err.Error())
		return
	}
	ok(c, "conditional order canceled")
}

var errConditionalNotPending = errors.New("conditional order is not in pending status")

// 原子标记撤销并退回冻结保证金——单笔撤销和批量撤销共用。返回errConditionalNotPending表示条件单已经不是pending(被并发的触发扫描抢先了)
func (s *Server) cancelPendingConditional(ctx context.Context, order model.ConditionalOrder) error {
	ok1, err := s.conditionalOrders.MarkCanceled(ctx, order.OrderID, service.NowMillis())
	if err != nil {
		return err
	}
	if !ok1 {
		return errConditionalNotPending
	}
	if order.Action == model.ActionOpen && (order.FrozenMargin.Sign() > 0 || order.FrozenCredit.Sign() > 0) {
		// 条件单从来没有部分成交这一说(触发之前压根没提交撮合)，撤销就是整笔退，不需要像普通委托撤单那样按剩余量比例计算
		return s.accounts.UnfreezeMargin(ctx, order.UID, order.FrozenMargin, order.FrozenCredit)
	}
	return nil
}

func (s *Server) conditionalOrderCurrent(c *gin.Context) {
	uid, ok1 := s.parseAccountUID(c)
	if !ok1 {
		return
	}
	orders, err := s.conditionalOrders.FindActiveByUID(c.Request.Context(), uid, c.Query("symbol"))
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	ok(c, orders)
}

func (s *Server) conditionalOrderHistory(c *gin.Context) {
	uid, ok1 := s.parseAccountUID(c)
	if !ok1 {
		return
	}
	limit, before, ok2 := pageParams(c)
	if !ok2 {
		return
	}
	orders, err := s.conditionalOrders.FindHistoryByUID(c.Request.Context(), uid, limit, before)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	ok(c, orders)
}

func (s *Server) positionCurrent(c *gin.Context) {
	uid, ok1 := s.parseAccountUID(c)
	if !ok1 {
		return
	}
	views, err := s.positions.Views(c.Request.Context(), uid)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	ok(c, views)
}

type setLeverageRequest struct {
	UID      uint64     `json:"uid" binding:"required"`
	Symbol   string     `json:"symbol" binding:"required"`
	Side     model.Side `json:"side" binding:"required"`
	Leverage uint32     `json:"leverage" binding:"required"` // 必须是>=1的整数(uint32本身保证)，上限按tier.MaxLeverage校验
}

// 修改一个已有仓位的杠杆——只对已经有仓位的uid+symbol+side生效，这个系统里
// 杠杆本来就是下单时的参数，没有"没有仓位时预先声明杠杆"这种场景，见docs/leverage.md。
// 按新杠杆重新算这个仓位应该占用多少保证金，多退少补：杠杆调低(需要的保证金变多)从
// balance/credit补锁定差额，钱不够直接拒绝；杠杆调高(需要的保证金变少)按仓位现有的
// balance/credit来源比例解锁差额，不能笼统解到balance一侧——那样等于让credit经过
// "锁定再降杠杆"这个渠道被洗成balance，跟开仓保证金拆分的既有规则(见
// account-and-margin.md)是同一个道理
func (s *Server) setLeverage(c *gin.Context) {
	var req setLeverageRequest
	if !bindJSON(c, &req) {
		return
	}
	account := s.loadAccount(c, req.UID)
	if account == nil {
		return
	}
	// 降杠杆要补冻结保证金，等于变相新增风险，冻结账户一律不让改
	if rejectIfFrozen(c, account) {
		return
	}
	if req.Side != model.SideLong && req.Side != model.SideShort {
		fail(c, 400, "side is invalid")
		return
	}
	leverage := decimal.NewFromInt(int64(req.Leverage))

	uid, symbol, side := req.UID, req.Symbol, req.Side
	ctx := c.Request.Context()

	// 跟addOrder开仓路径共用同一把按uid+symbol+side的锁(service.OrderLockKey)——修改杠杆
	// 和并发下单一样，都要读现有状态(这里是仓位名义价值)再决定后续动作，必须序列化
	lockErr := s.lock.WithLock(ctx, service.OrderLockKey(uid, symbol, side), func() error {
		p, err := s.positions.Find(ctx, uid, symbol, side)
		if err != nil {
			return err
		}
		if p == nil || p.Volume.Sign() <= 0 {
			return newHTTPError(400, ErrPositionNotFound, "no position found in this direction")
		}
		mark, hasMark := s.markPrice.Get(ctx, symbol)
		if !hasMark {
			return newHTTPError(400, ErrNoMarkPrice, "this contract has no mark price yet")
		}
		notional := p.Volume.Mul(mark)
		tier, err := s.positions.TierFor(ctx, symbol, notional)
		if err != nil {
			return err
		}
		if tier == nil {
			return newHTTPError(400, ErrTierNotConfigured, "this contract has no risk limit tier configured")
		}
		if leverage.GreaterThan(decimal.NewFromInt(int64(tier.MaxLeverage))) {
			return newHTTPError(400, ErrLeverageExceedsTier, "leverage exceeds the maximum allowed for the tier at the current position notional")
		}

		newMargin := notional.Div(leverage)
		delta := newMargin.Sub(p.PositionMargin)
		newCreditMargin := p.CreditMargin
		switch delta.Sign() {
		case 1:
			// 杠杆调低，需要的保证金变多。这个仓位的position_margin从开仓起就一直锁在
			// accounts.frozen_margin/frozen_credit里(见settlement.go的OPEN分支)，跟还在
			// 排队等成交的委托共用同一个锁定池，所以这里直接复用FreezeMargin的四级路径
			// (free balance→free credit→浮盈买力→拒绝)判断这笔差额该从哪里出、锁进
			// frozen_margin/frozen_credit——净效果是自由余额被永久扣掉delta，
			// 跟SubmitOrder下单冻结走的是同一个函数
			result, err := s.accounts.FreezeMargin(ctx, uid, delta)
			if err != nil {
				return err
			}
			newCreditMargin = p.CreditMargin.Add(result.FromCredit)
		case -1:
			// 杠杆调高，需要的保证金变少——按仓位现有的balance/credit来源比例，把差额
			// 对应的frozen_margin/frozen_credit解锁，是ApplyCloseFill释放持仓保证金时
			// 同样的解锁路径
			release := delta.Neg()
			var releaseCredit decimal.Decimal
			if p.PositionMargin.Sign() > 0 {
				releaseCredit = p.CreditMargin.Mul(release).Div(p.PositionMargin)
			}
			releaseAvailable := release.Sub(releaseCredit)
			if err := s.accounts.UnfreezeMargin(ctx, uid, releaseAvailable, releaseCredit); err != nil {
				return err
			}
			newCreditMargin = p.CreditMargin.Sub(releaseCredit)
		}

		ok, err := s.positions.UpdateLeverage(ctx, p.ID, newMargin, newCreditMargin, uint32(leverage.IntPart()), p.Volume, service.NowMillis())
		if err != nil {
			return err
		}
		if !ok {
			// 理论上不该发生(外层已经用同一把锁序列化了同一个uid+symbol+side的并发请求)，
			// 防御性处理：仓位在读取之后到写入之前发生了变化
			return newHTTPError(500, ErrInternal, "position state changed, please retry")
		}
		return nil
	})
	if lockErr != nil {
		respondLockErr(c, lockErr)
		return
	}
	// 这个接口完全在contract-api内部同步完成，不经过Kafka/contract-engine，跟
	// adjustBalance/grantCredit这些contract-api自己的资金类接口一样没有WS推送——
	// WS私有频道的推送只从contract-engine那边发出(见docs/websocket.md)，客户端这里
	// 拿到的HTTP响应本身就是最新状态，不需要额外通知
	ok(c, "leverage changed")
}

func (s *Server) tradeHistory(c *gin.Context) {
	uid, ok1 := s.parseAccountUID(c)
	if !ok1 {
		return
	}
	limit, before, ok2 := pageParams(c)
	if !ok2 {
		return
	}
	trades, err := s.trades.FindByUID(c.Request.Context(), uid, limit, before)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	ok(c, trades)
}

func (s *Server) fundingRate(c *gin.Context) {
	symbol := c.Query("symbol")
	coin, err := s.coins.FindBySymbol(c.Request.Context(), symbol)
	if err != nil || coin == nil {
		failC(c, 400, ErrSymbolNotFound, "contract does not exist")
		return
	}
	now := service.NowMillis()
	ok(c, gin.H{
		"symbol":          symbol,
		"estimatedRate":   s.funding.EstimateRate(c.Request.Context(), *coin),
		"nextFundingTime": s.funding.NextFundingTime(*coin, now),
	})
}

// validKlineIntervals 允许的K线周期，跟model.AllKlineIntervals保持一致——请求任意
// 字符串都会被拒绝，不会被当成没有意义的周期悄悄放行
var validKlineIntervals = map[model.KlineInterval]bool{
	model.Kline1m: true, model.Kline5m: true, model.Kline15m: true,
	model.Kline1h: true, model.Kline4h: true, model.Kline1d: true,
}

func (s *Server) kline(c *gin.Context) {
	symbol := c.Query("symbol")
	coin, err := s.coins.FindBySymbol(c.Request.Context(), symbol)
	if err != nil || coin == nil {
		failC(c, 400, ErrSymbolNotFound, "contract does not exist")
		return
	}
	interval := model.KlineInterval(c.Query("interval"))
	if !validKlineIntervals[interval] {
		fail(c, 400, "interval is invalid")
		return
	}
	limit, msg := parsePositiveIntQuery(c, "limit", 200)
	if msg != "" {
		fail(c, 400, msg)
		return
	}
	rows, err := s.klines.FindRecent(c.Request.Context(), symbol, interval, limit)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	ok(c, rows)
}

func (s *Server) fundingHistory(c *gin.Context) {
	symbol := c.Query("symbol")
	limit, before, ok2 := pageParams(c)
	if !ok2 {
		return
	}
	records, err := s.funding.History(c.Request.Context(), symbol, limit, int64(before))
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	ok(c, records)
}

type setIndexPriceRequest struct {
	Symbol string          `json:"symbol" binding:"required"`
	Price  decimal.Decimal `json:"price"`
}

// 外部行情源(MVP阶段先靠脚本/运营手动喂，以后换成接入币安行情的适配器)推送指数价格——资金费率结算依赖这个值，见FundingService
func (s *Server) setIndexPrice(c *gin.Context) {
	var req setIndexPriceRequest
	if !bindJSON(c, &req) {
		return
	}
	if req.Price.Sign() <= 0 {
		fail(c, 400, "price is invalid")
		return
	}
	res, err := s.markPrice.PushIndexPrice(c.Request.Context(), req.Symbol, req.Price)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	if !res.Accepted {
		// 不是喂价方的错，是保护在等这个新价位持续够久。真实的行情大幅变动会在几秒内被承认，
		// 喂价方继续按周期推就行，不用特殊处理
		failC(c, 400, ErrIndexPriceJump, fmt.Sprintf("index price change relative to the current value %s exceeds the server threshold, pending for %.1fs", res.Current, res.Waited.Seconds()))
		return
	}
	ok(c, nil)
}
