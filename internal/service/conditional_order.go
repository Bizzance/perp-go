package service

import (
	"context"
	"log"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
	"perp-go/internal/repo"
)

// ConditionalOrderService 条件单(止盈止损/条件开仓)触发扫描，运行在contract-engine里，
// 跟LiquidationService的风控扫描是同一种定时ticker模式。条件单创建/撤销发生在
// contract-api（router.go），因为那两步只需要读写MySQL、不需要摸撮合引擎的内存订单簿；
// 触发后要把条件单转成一笔真正的委托并提交撮合，这一步必须在contract-engine进程内完成
type ConditionalOrderService struct {
	conditionalOrders *repo.ConditionalOrderRepo
	orders            *repo.OrderRepo
	markPrice         *MarkPriceService
	engine            *EngineService
}

func NewConditionalOrderService(
	conditionalOrders *repo.ConditionalOrderRepo,
	orders *repo.OrderRepo,
	markPrice *MarkPriceService,
	engine *EngineService,
) *ConditionalOrderService {
	return &ConditionalOrderService{
		conditionalOrders: conditionalOrders,
		orders:            orders,
		markPrice:         markPrice,
		engine:            engine,
	}
}

// ScanOnce 扫一遍全部还没触发的条件单，标记价格满足触发条件的就触发。同一个symbol的
// 标记价格在一次扫描里只读一次、缓存复用，不对每个条件单单独查一次Redis
func (s *ConditionalOrderService) ScanOnce(ctx context.Context) {
	pending, err := s.conditionalOrders.FindAllPending(ctx)
	if err != nil {
		log.Printf("[ERROR] 条件单扫描: 查询待触发列表失败: %v", err)
		return
	}
	marks := make(map[string]decimal.Decimal)
	for _, co := range pending {
		mark, ok := marks[co.Symbol]
		if !ok {
			var hasMark bool
			mark, hasMark = s.markPrice.Get(ctx, co.Symbol)
			if !hasMark {
				continue // 这个symbol还没有标记价格，这一轮没法判断，等下一轮
			}
			marks[co.Symbol] = mark
		}
		if co.Triggered(mark) {
			s.trigger(ctx, co, mark)
		}
	}
}

// trigger 原子标记触发，把条件单落地成一笔真正的委托、提交撮合——完全复用
// LiquidationService.queueLiquidation同样的"落库+SubmitOrder"模式，只是这里触发后是
// 按条件单自己指定的type/price提交，不是强平那种保护价排队
func (s *ConditionalOrderService) trigger(ctx context.Context, co model.ConditionalOrder, mark decimal.Decimal) {
	now := NowMillis()
	ok, err := s.conditionalOrders.MarkTriggered(ctx, co.OrderID, now)
	if err != nil {
		log.Printf("[ERROR] 条件单触发标记失败, orderId=%d: %v", co.OrderID, err)
		return
	}
	if !ok {
		return // 撤单跟这次扫描并发竞争，撤单赢了，跳过
	}

	price := co.Price
	if co.Type == model.OrderTypeMarket {
		price = mark
	}
	o := &model.Order{
		OrderID:      co.OrderID,
		UID:          co.UID,
		Symbol:       co.Symbol,
		Side:         co.Side,
		Action:       co.Action,
		Type:         co.Type,
		Price:        price,
		Amount:       co.Amount,
		FrozenMargin: co.FrozenMargin,
		FrozenCredit: co.FrozenCredit,
		Leverage:     co.Leverage,
		ReduceOnly:   co.ReduceOnly,
		Status:       model.OrderStatusOpen,
		CreateTime:   now,
		UpdateTime:   now,
	}
	log.Printf("[INFO] 条件单触发, orderId=%d, uid=%d, symbol=%s, side=%s, action=%s, 触发价=%s, 标记价=%s",
		co.OrderID, co.UID, co.Symbol, co.Side, co.Action, co.TriggerPrice, mark)
	if err := s.orders.Insert(ctx, o); err != nil {
		log.Printf("[ERROR] 条件单触发后落库委托失败, orderId=%d: %v", co.OrderID, err)
		return
	}
	if err := s.engine.SubmitOrder(ctx, o, time.Now().UnixNano()); err != nil {
		log.Printf("[ERROR] 条件单触发后提交撮合失败, orderId=%d: %v", co.OrderID, err)
	}
}
