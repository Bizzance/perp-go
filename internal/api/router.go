// Package api 是contract-api进程的Gin路由/handler层。MVP阶段鉴权用明文uid参数占位
// (不做HMAC/token校验)，接口设计上uid都是独立传参，方便后续直接换成鉴权中间件注入，
// 不用改业务代码——见plan文件"明确不做"一节。
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
	producer  *mq.Producer
}

func NewServer(accounts *service.AccountService, positions *service.PositionService, coins *repo.CoinRepo,
	orders *repo.OrderRepo, trades *repo.TradeRepo, markPrice *service.MarkPriceService, producer *mq.Producer) *Server {
	return &Server{accounts: accounts, positions: positions, coins: coins, orders: orders, trades: trades, markPrice: markPrice, producer: producer}
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
	return r
}

func fail(c *gin.Context, code int, msg string) {
	c.JSON(http.StatusOK, gin.H{"code": code, "message": msg})
}

func ok(c *gin.Context, data any) {
	c.JSON(http.StatusOK, gin.H{"code": 0, "message": "SUCCESS", "data": data})
}

func parseUID(c *gin.Context) (uint64, bool) {
	uid, err := decimal.NewFromString(c.Query("uid"))
	if err != nil {
		if s := c.PostForm("uid"); s != "" {
			uid, err = decimal.NewFromString(s)
		}
	}
	if err != nil || uid.Sign() <= 0 {
		fail(c, 400, "uid参数不合法")
		return 0, false
	}
	return uint64(uid.IntPart()), true
}

func (s *Server) adjustBalance(c *gin.Context) {
	uid, ok1 := parseUID(c)
	if !ok1 {
		return
	}
	amount, err := decimal.NewFromString(c.PostForm("amount"))
	if err != nil {
		fail(c, 400, "amount参数不合法")
		return
	}
	if err := s.accounts.AdjustBalance(c.Request.Context(), uid, amount); err != nil {
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

func (s *Server) addOrder(c *gin.Context) {
	uid, ok1 := parseUID(c)
	if !ok1 {
		return
	}
	symbol := c.PostForm("symbol")
	side := model.Side(c.PostForm("side"))
	action := model.OrderAction(c.PostForm("action"))
	orderType := model.OrderType(c.DefaultPostForm("type", "LIMIT"))
	leverage, err := decimal.NewFromString(c.DefaultPostForm("leverage", "1"))
	if err != nil {
		fail(c, 400, "leverage参数不合法")
		return
	}

	coin, err := s.coins.FindBySymbol(c.Request.Context(), symbol)
	if err != nil || coin == nil || !coin.Enable {
		fail(c, 400, "合约不存在或已下架")
		return
	}

	var price decimal.Decimal
	if orderType == model.OrderTypeLimit {
		price, err = decimal.NewFromString(c.PostForm("price"))
		if err != nil || price.Sign() <= 0 {
			fail(c, 400, "限价单price参数不合法")
			return
		}
	} else {
		mark, hasMark := s.markPrice.Get(c.Request.Context(), symbol)
		if !hasMark {
			fail(c, 400, "该合约暂无标记价格，市价单无法估算数量")
			return
		}
		price = mark
	}

	var amount decimal.Decimal
	if marginStr := c.PostForm("marginAmount"); marginStr != "" {
		marginAmount, err := decimal.NewFromString(marginStr)
		if err != nil || marginAmount.Sign() <= 0 {
			fail(c, 400, "marginAmount参数不合法")
			return
		}
		notional := marginAmount.Mul(leverage)
		amount = notional.Div(price).Truncate(coin.BaseCoinScale)
	} else if amountStr := c.PostForm("amount"); amountStr != "" {
		amount, err = decimal.NewFromString(amountStr)
		if err != nil {
			fail(c, 400, "amount参数不合法")
			return
		}
	} else {
		fail(c, 400, "必须传marginAmount或amount之一")
		return
	}
	if amount.Sign() <= 0 {
		fail(c, 400, "数量必须大于0")
		return
	}

	requiredMargin := amount.Mul(price).Div(leverage)
	if action == model.ActionOpen {
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
		ReduceOnly: c.PostForm("reduceOnly") == "true", Status: model.OrderStatusNew, CreateTime: now, UpdateTime: now,
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

func (s *Server) cancelOrder(c *gin.Context) {
	uid, ok1 := parseUID(c)
	if !ok1 {
		return
	}
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
			if coin, err := s.coins.FindBySymbol(c.Request.Context(), p.Symbol); err == nil && coin != nil {
				v.LiquidationPrice = p.LiquidationPrice(coin.MaintenanceMarginRate)
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
