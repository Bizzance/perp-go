// Package api 是contract-api进程的Gin路由/handler层。MVP阶段鉴权用明文uid参数占位
// (不做HMAC/token校验)，接口设计上uid都是独立传参，方便后续直接换成鉴权中间件注入，
// 不用改业务代码——见plan文件"明确不做"一节。GET查询接口走query string，POST写接口统一
// 用JSON body(Content-Type: application/json)+ShouldBindJSON，不用表单编码。
package api

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"

	"perp-go/internal/events"
	"perp-go/internal/mq"
	"perp-go/internal/repo"
	"perp-go/internal/service"

	"perp-go/internal/model"
)

type Server struct {
	accounts  *service.AccountService
	positions *service.PositionService
	coins     *repo.CoinRepo
	orders    *repo.OrderRepo
	trades    *repo.TradeRepo
	markPrice *service.MarkPriceService
	funding   *service.FundingService
	producer  *mq.Producer
}

func NewServer(
	accounts *service.AccountService,
	positions *service.PositionService,
	coins *repo.CoinRepo,
	orders *repo.OrderRepo,
	trades *repo.TradeRepo,
	markPrice *service.MarkPriceService,
	funding *service.FundingService,
	producer *mq.Producer,
) *Server {
	return &Server{
		accounts:  accounts,
		positions: positions,
		coins:     coins,
		orders:    orders,
		trades:    trades,
		markPrice: markPrice,
		funding:   funding,
		producer:  producer,
	}
}

func (s *Server) Router() *gin.Engine {
	r := gin.Default()
	r.POST("/account/balance", s.adjustBalance)
	r.GET("/account/info", s.accountInfo)
	r.POST("/order/add", s.addOrder)
	r.POST("/order/cancel/:orderId", s.cancelOrder)
	r.GET("/order/current", s.orderCurrent)
	r.GET("/order/history", s.orderHistory)
	r.GET("/position/current", s.positionCurrent)
	r.GET("/trade/history", s.tradeHistory)
	r.GET("/funding/rate", s.fundingRate)
	r.GET("/funding/history", s.fundingHistory)
	r.POST("/index-price", s.setIndexPrice)
	return r
}

func fail(c *gin.Context, code int, msg string) {
	c.JSON(http.StatusOK, gin.H{"code": code, "message": msg})
}

func ok(c *gin.Context, data any) {
	c.JSON(http.StatusOK, gin.H{"code": 200, "message": "success", "data": data})
}

// parseUID 只给GET接口用——这些接口的参数走query string，没有body。写接口(POST)统一用
// JSON body + ShouldBindJSON，uid跟着各自的request struct走，不再需要这个helper
func parseUID(c *gin.Context) (uint64, bool) {
	uid, err := decimal.NewFromString(c.Query("uid"))
	if err != nil || uid.Sign() <= 0 {
		fail(c, 400, "uid参数不合法")
		return 0, false
	}
	return uint64(uid.IntPart()), true
}

// bindJSON 全部POST接口统一的JSON body解析入口，失败了直接写400响应——调用方判断返回值
// 决定要不要continue往下走，不用每个handler自己重复"解析失败就返回400"这几行
func bindJSON(c *gin.Context, req any) bool {
	if err := c.ShouldBindJSON(req); err != nil {
		fail(c, 400, "请求参数不合法")
		return false
	}
	return true
}

type adjustBalanceRequest struct {
	UID    uint64          `json:"uid" binding:"required"`
	Amount decimal.Decimal `json:"amount"`
}

func (s *Server) adjustBalance(c *gin.Context) {
	var req adjustBalanceRequest
	if !bindJSON(c, &req) {
		return
	}
	if err := s.accounts.AdjustBalance(c.Request.Context(), req.UID, req.Amount); err != nil {
		fail(c, 500, err.Error())
		return
	}
	ok(c, nil)
}

func (s *Server) accountInfo(c *gin.Context) {
	uid, ok1 := parseUID(c)
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

type addOrderRequest struct {
	UID    uint64            `json:"uid" binding:"required"`
	Symbol string            `json:"symbol" binding:"required"`
	Side   model.Side        `json:"side" binding:"required"`
	Action model.OrderAction `json:"action" binding:"required"`
	Type   model.OrderType   `json:"type"`
	// Leverage/MarginAmount/Amount用指针而不是值类型：JSON里"字段没传"和"字段传了显式的0"
	// 必须能区分开——没传时才套默认值/走另一个字段，显式传0要老老实实地报错，不能悄悄当成
	// "没传"处理掉，那样一个总是把零值字段也序列化出来的客户端库会把真实的输入错误悄悄丢掉
	Leverage     *decimal.Decimal `json:"leverage"`
	Price        decimal.Decimal  `json:"price"`
	MarginAmount *decimal.Decimal `json:"marginAmount"`
	Amount       *decimal.Decimal `json:"amount"`
	ReduceOnly   bool             `json:"reduceOnly"`
}

func (s *Server) addOrder(c *gin.Context) {
	var req addOrderRequest
	if !bindJSON(c, &req) {
		return
	}
	uid := req.UID
	symbol := req.Symbol
	side := req.Side
	action := req.Action
	// side/action任何不认识的值都必须拒绝，不能放过去——matching.DirectionOf对side/action
	// 只特判了(LONG,OPEN)和(SHORT,CLOSE)算买方，其它一律当卖方处理，一个拼错的side/action
	// 字符串会被悄悄撮合成方向相反的交易，而不是报错
	if side != model.SideLong && side != model.SideShort {
		fail(c, 400, "side参数不合法")
		return
	}
	if action != model.ActionOpen && action != model.ActionClose {
		fail(c, 400, "action参数不合法")
		return
	}
	orderType := req.Type
	if orderType == "" {
		orderType = model.OrderTypeLimit
	}
	if orderType != model.OrderTypeLimit && orderType != model.OrderTypeMarket {
		fail(c, 400, "type参数不合法")
		return
	}
	leverage := decimal.NewFromInt(1)
	if req.Leverage != nil {
		leverage = *req.Leverage
	}
	// maxSaneLeverage是不区分开平仓、不查分档配置的兜底上限——只用来挡掉明显离谱/会导致
	// 下面uint32(leverage.IntPart())溢出截断成垃圾值的输入。真正按分档算出来的tier.MaxLeverage
	// 只在下面ActionOpen分支里校验，是刻意的：leverage这个字段只有开仓会用来算需要冻结多少
	// 保证金(见settlement.go的ApplyOpenFill)，平仓不创建新仓位/新风险，不需要经过分档校验，
	// 而且分档校验依赖risk_limit_tiers有配置，一旦要求平仓也必须过这一关，配置缺失或删除时
	// 反而会把用户已有仓位卡死平不掉——两害相权，让平仓单的leverage只挡这道跟合约配置无关的
	// 离谱值兜底，是本次分档改动反复权衡后的结论，不是遗漏
	const maxSaneLeverage = 1000
	if leverage.Sign() <= 0 || leverage.GreaterThan(decimal.NewFromInt(maxSaneLeverage)) || !leverage.IsInteger() {
		// 必须是整数：leverage落库时是uint32(order.Leverage)，settlement.go计算仓位保证金
		// 时也是按这个落库的整数杠杆重算(不是按下单时这个decimal算requiredMargin用的原始
		// 精度值)。如果这里放行小数杠杆(比如7.5)，冻结保证金按7.5算是对的，但落库/后续
		// 结算全部按truncate过的7算，两边对不上，会污染这个仓位的position_margin/ROE/
		// 预估强平价——干脆不允许小数杠杆，跟真实交易所只提供整数倍杠杆选项一致
		fail(c, 400, "leverage参数不合法")
		return
	}

	coin, err := s.coins.FindBySymbol(c.Request.Context(), symbol)
	if err != nil || coin == nil || !coin.Enable {
		fail(c, 400, "合约不存在或已下架")
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

	var price decimal.Decimal
	if orderType == model.OrderTypeLimit {
		price = req.Price
		if price.Sign() <= 0 {
			fail(c, 400, "限价单price参数不合法")
			return
		}
		if coin.PriceTick.Sign() > 0 && !price.Mod(coin.PriceTick).IsZero() {
			fail(c, 400, "price不符合最小变动单位")
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
		referencePrice, hasReference := mark, hasMark
		if !hasReference {
			referencePrice, hasReference = s.markPrice.GetIndexPrice(c.Request.Context(), symbol)
		}
		if action == model.ActionOpen && hasReference && referencePrice.Sign() > 0 && coin.PriceProtectionRatio.Sign() > 0 {
			deviation := price.Sub(referencePrice).Abs().Div(referencePrice)
			if deviation.GreaterThan(coin.PriceProtectionRatio) {
				fail(c, 400, "委托价格偏离参考价过多")
				return
			}
		}
	} else {
		if !hasMark {
			fail(c, 400, "该合约暂无标记价格，市价单无法估算数量")
			return
		}
		price = mark
	}

	var amount decimal.Decimal
	switch {
	case req.MarginAmount != nil:
		if req.MarginAmount.Sign() <= 0 {
			fail(c, 400, "marginAmount参数不合法")
			return
		}
		notional := req.MarginAmount.Mul(leverage)
		amount = notional.Div(price).Truncate(coin.BaseCoinScale)
	case req.Amount != nil:
		amount = *req.Amount
	default:
		fail(c, 400, "必须传marginAmount或amount之一")
		return
	}
	if amount.Sign() <= 0 {
		fail(c, 400, "数量必须大于0")
		return
	}
	if coin.MinVolume.Sign() > 0 && amount.LessThan(coin.MinVolume) {
		fail(c, 400, "数量低于该合约最小下单量")
		return
	}
	if coin.MaxVolume.Sign() > 0 && amount.GreaterThan(coin.MaxVolume) {
		fail(c, 400, "数量超出该合约最大下单量")
		return
	}
	if coin.VolumeStep.Sign() > 0 && !amount.Mod(coin.VolumeStep).IsZero() {
		fail(c, 400, "数量不符合最小步长")
		return
	}

	requiredMargin := amount.Mul(price).Div(leverage)
	if action == model.ActionOpen {
		// 分档判断的名义价值不能只看已成交仓位：这个uid在同一symbol+side上如果还挂着别的没成交的
		// 开仓单，每一笔单独提交时都看不到彼此，会各自按"当前还没有仓位/挂单垫底"通过校验，等
		// 行情走到这些价位一起成交，合并起来的真实仓位可能远超单笔校验时的档位——顺序提交多笔
		// 远离盘口的限价单就能稳定触发，所以这里除了已成交仓位，还要把这个方向上全部还在排队的
		// OPEN单也算进去。这段"读现有仓位/挂单→算档位→冻结保证金"整体不是原子的，并发对同一
		// uid+symbol+side提交多笔请求，每一笔读到的都是对方还没提交时的旧状态，理论上仍能绕开——
		// 这套系统一直没有为这类极端并发加锁(FreezeMargin等其它地方同样如此)，属于已知、接受的
		// MVP简化，这里只堵顺序提交这条更容易触发、不需要精确时机就能稳定复现的路径
		existingNotional := decimal.Zero
		existing, err := s.positions.Find(c.Request.Context(), uid, symbol, side)
		if err != nil {
			fail(c, 500, err.Error())
			return
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
		activeOrders, err := s.orders.FindActiveByUID(c.Request.Context(), uid, symbol)
		if err != nil {
			fail(c, 500, err.Error())
			return
		}
		for _, o := range activeOrders {
			if o.Side == side && o.Action == model.ActionOpen {
				// 这里必须用挂单自己的o.Price，不能像上面现有仓位那样退回标记价：这些是已经
				// 通过校验、真实挂在簿子上的委托，o.Price不是"用户随便填的、可能被拿来做局的
				// 报价"，而是它成交时会用到的真实价格(排队单成交价=挂单自己的限价，不是标记价)。
				// 之前这里写成跟现有仓位一样固定退回标记价，恰好把这个函数本要防的"顺序挂多笔
				// 远离盘口限价单"场景反向搞成了漏洞：挂单价格越是远离标记价(正是最该被算重的
				// 情况)，标记价对它的估值就越失真、越偏小
				existingNotional = existingNotional.Add(o.RemainingAmount().Mul(o.Price))
			}
		}
		// 这笔新委托自己的名义价值同样不能只信submitted price，但只对SHORT+OPEN生效：
		// SHORT+OPEN在撮合引擎里是卖方向，挂一个远低于市价的价格属于"吃单价"，会立刻按盘口
		// 对手的真实价格成交，不是按这个填的低价，所以要用max(price, markPrice)取更保守的
		// 那个当分档判断的基准。LONG+OPEN反过来：远低于市价的价格是完全合法的被动挂单(买
		// 跌)，只会按这个低价成交，如果同样套max(price,markPrice)会把这类正常订单的名义
		// 价值算得比真实值更大，可能因此被判进不该适用的更严档位、平白无故拒单——这个防护
		// 依赖有标记价格可用，一个从没成交过的全新symbol(hasMark=false)防不住这一招，是
		// contract-api这边看不到contract-engine盘口真实成交价这个更大架构问题的一角，跟
		// 下单冻结保证金用submitted price而不是真实成交价是同一个根因，MVP阶段先记录、
		// 不在这里单独打补丁
		orderNotionalPrice := price
		if side == model.SideShort && hasMark && mark.GreaterThan(price) {
			orderNotionalPrice = mark
		}
		tier, err := s.positions.TierFor(c.Request.Context(), symbol, existingNotional.Add(amount.Mul(orderNotionalPrice)))
		if err != nil {
			fail(c, 500, err.Error())
			return
		}
		if tier == nil {
			fail(c, 400, "该合约未配置保证金分档，暂不允许开仓")
			return
		}
		if leverage.GreaterThan(decimal.NewFromInt(int64(tier.MaxLeverage))) {
			fail(c, 400, "杠杆倍数超出当前仓位名义价值对应档位允许的范围")
			return
		}
		if err := s.accounts.FreezeMargin(c.Request.Context(), uid, requiredMargin); err != nil {
			fail(c, 500, err.Error())
			return
		}
	}

	orderID := service.NextID()
	now := service.NowMillis()
	o := &model.Order{
		OrderID: orderID, UID: uid, Symbol: symbol, Side: side, Action: action, Type: orderType,
		Price: price, Amount: amount, Leverage: uint32(leverage.IntPart()),
		ReduceOnly: req.ReduceOnly, Status: model.OrderStatusNew, CreateTime: now, UpdateTime: now,
	}
	if action == model.ActionOpen {
		o.FrozenMargin = requiredMargin
	}
	if err := s.orders.Insert(c.Request.Context(), o); err != nil {
		fail(c, 500, err.Error())
		return
	}

	evt := events.OrderSubmitEvent{
		OrderID: orderID, UID: uid, Symbol: symbol, Side: string(side), Action: string(action),
		Type: string(orderType), Price: price.String(), Amount: amount.String(),
		Leverage: o.Leverage, ReduceOnly: o.ReduceOnly,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.producer.Publish(ctx, events.TopicOrderSubmit, symbol, evt); err != nil {
		fail(c, 500, "委托已落库但发送到撮合引擎失败: "+err.Error())
		return
	}
	ok(c, orderID)
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
		fail(c, 400, "orderId不合法")
		return
	}
	o, err := s.orders.FindByOrderID(c.Request.Context(), uint64(orderID.IntPart()))
	if err != nil || o == nil || o.UID != uid {
		fail(c, 400, "委托单不存在")
		return
	}
	if !model.ActiveOrderStatuses[o.Status] {
		fail(c, 400, "委托单已完成或已取消")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	evt := events.OrderCancelEvent{OrderID: o.OrderID, UID: uid, Symbol: o.Symbol}
	if err := s.producer.Publish(ctx, events.TopicOrderCancel, o.Symbol, evt); err != nil {
		fail(c, 500, err.Error())
		return
	}
	ok(c, "撤单请求已提交")
}

func (s *Server) orderCurrent(c *gin.Context) {
	uid, ok1 := parseUID(c)
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
	uid, ok1 := parseUID(c)
	if !ok1 {
		return
	}
	orders, err := s.orders.FindHistoryByUID(c.Request.Context(), uid, 100)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	ok(c, orders)
}

// PositionView 查询接口展示用：持仓原始字段+现算的标记价/未实现盈亏/回报率/名义价值/预估强平价
type PositionView struct {
	model.Position
	MarkPrice        decimal.Decimal `json:"markPrice"`
	UnrealizedPnl    decimal.Decimal `json:"unrealizedPnl"`
	Roe              decimal.Decimal `json:"roe"`
	NotionalValue    decimal.Decimal `json:"notionalValue"`
	LiquidationPrice decimal.Decimal `json:"liquidationPrice"`
}

func (s *Server) positionCurrent(c *gin.Context) {
	uid, ok1 := parseUID(c)
	if !ok1 {
		return
	}
	positions, err := s.positions.FindByUID(c.Request.Context(), uid)
	if err != nil {
		fail(c, 500, err.Error())
		return
	}
	views := make([]PositionView, 0, len(positions))
	for _, p := range positions {
		v := PositionView{Position: p}
		mark, hasMark := s.markPrice.Get(c.Request.Context(), p.Symbol)
		if hasMark {
			v.MarkPrice = mark
			v.UnrealizedPnl = p.UnrealizedPnl(mark)
			v.NotionalValue = p.Volume.Mul(mark)
			if p.PositionMargin.Sign() > 0 {
				v.Roe = v.UnrealizedPnl.Div(p.PositionMargin)
			}
			if tier, err := s.positions.TierFor(c.Request.Context(), p.Symbol, v.NotionalValue); err == nil && tier != nil {
				v.LiquidationPrice = p.LiquidationPrice(tier.MaintenanceMarginRate, tier.MaintenanceAmount)
			}
		}
		views = append(views, v)
	}
	ok(c, views)
}

func (s *Server) tradeHistory(c *gin.Context) {
	uid, ok1 := parseUID(c)
	if !ok1 {
		return
	}
	trades, err := s.trades.FindByUID(c.Request.Context(), uid, 100)
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
		fail(c, 400, "合约不存在")
		return
	}
	now := service.NowMillis()
	ok(c, gin.H{
		"symbol":          symbol,
		"estimatedRate":   s.funding.EstimateRate(c.Request.Context(), *coin),
		"nextFundingTime": s.funding.NextFundingTime(*coin, now),
	})
}

func (s *Server) fundingHistory(c *gin.Context) {
	symbol := c.Query("symbol")
	records, err := s.funding.History(c.Request.Context(), symbol, 100)
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

// setIndexPrice 外部行情源(MVP阶段先靠脚本/运营手动喂，以后换成接入币安行情的适配器)推送
// 指数价格——资金费率结算依赖这个值，见FundingService
func (s *Server) setIndexPrice(c *gin.Context) {
	var req setIndexPriceRequest
	if !bindJSON(c, &req) {
		return
	}
	if req.Price.Sign() <= 0 {
		fail(c, 400, "price参数不合法")
		return
	}
	if err := s.markPrice.SetIndexPrice(c.Request.Context(), req.Symbol, req.Price); err != nil {
		fail(c, 500, err.Error())
		return
	}
	ok(c, nil)
}
