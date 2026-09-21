//go:build integration

package service_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/shopspring/decimal"

	"perp-go/internal/matching"
	"perp-go/internal/model"
	"perp-go/internal/repo"
	"perp-go/internal/service"
	"perp-go/internal/testutil"
)

const testSymbol = "BTCUSDT" // schema.sql的种子数据里有

// 一整套接到真实MySQL(每个测试独立的库)和Redis上的引擎服务，接线方式跟cmd/contract-engine一致
type engineEnv struct {
	db          *sqlx.DB
	accountRepo *repo.AccountRepo
	accounts    *service.AccountService
	orders      *repo.OrderRepo
	conditional *repo.ConditionalOrderRepo
	trades      *repo.TradeRepo
	positions   *repo.PositionRepo
	markPrice   *service.MarkPriceService
	book        *matching.Engine
	engine      *service.EngineService
	condSvc     *service.ConditionalOrderService
	liq         *service.LiquidationService
	funding     *service.FundingService
	coins       *repo.CoinRepo
	fund        *service.InsuranceFundService

	uidBase uint64
	nextID  atomic.Uint64

	bookIDs map[string][]uint64 // setBook摆在订单簿里的委托，下次setBook时先撤掉
}

func newEngineEnv(t *testing.T) *engineEnv {
	t.Helper()
	service.InitNodeID(1) // 成交结算会用雪花ID生成成交号，进程里必须初始化过
	conn := testutil.NewDB(t)
	rdb := testutil.NewCache(t)
	testutil.ResetPriceKeys(t, testSymbol, "ETHUSDT") // 开头和结尾各清一次，不受前后测试留下的指数价影响

	e := &engineEnv{db: conn, uidBase: testutil.UIDBase()}
	e.nextID.Store(e.uidBase * 1000)

	e.accountRepo = repo.NewAccountRepo(conn)
	coinRepo := repo.NewCoinRepo(conn)
	e.orders = repo.NewOrderRepo(conn)
	e.conditional = repo.NewConditionalOrderRepo(conn)
	positionRepo := repo.NewPositionRepo(conn)
	e.positions = positionRepo
	e.trades = repo.NewTradeRepo(conn)
	txRepo := repo.NewTxRepo(conn)
	fundRepo := repo.NewInsuranceFundRepo(conn)
	riskLimitRepo := repo.NewRiskLimitRepo(conn)
	klineRepo := repo.NewKlineRepo(conn)
	roundCloseProgressRepo := repo.NewRoundCloseProgressRepo(conn)

	e.markPrice = service.NewMarkPriceService(rdb)
	positionSvc := service.NewPositionService(positionRepo, riskLimitRepo, e.markPrice)
	e.accounts = service.NewAccountService(e.accountRepo, positionSvc, txRepo)
	settlementSvc := service.NewSettlementService(e.accounts, positionRepo, coinRepo, txRepo)
	fundSvc := service.NewInsuranceFundService(fundRepo)
	e.fund = fundSvc
	klineSvc := service.NewKlineService(klineRepo)
	pushSvc := service.NewPushService(rdb, e.accounts, positionSvc, e.orders)
	lockSvc := service.NewLockService(rdb)

	e.book = matching.NewEngine()
	e.engine = service.NewEngineService(e.book, e.orders, e.conditional, e.trades, e.accounts, positionSvc,
		settlementSvc, e.markPrice, fundSvc, klineSvc, pushSvc, roundCloseProgressRepo, lockSvc, coinRepo, nil)
	e.condSvc = service.NewConditionalOrderService(e.conditional, e.orders, e.markPrice, e.engine)
	// 强平单超时兜底设短一点(200ms)，测试里不用干等
	e.coins = coinRepo
	e.funding = service.NewFundingService(rdb, coinRepo, positionRepo, repo.NewFundingRepo(conn), e.accounts, txRepo, e.markPrice).
		WithBook(func(symbol string) bool { return e.engine.OwnsSymbol(symbol) }, // 闭包里读e.book/e.engine：restartEngine会换掉它们
			func(symbol string, notional decimal.Decimal) (decimal.Decimal, decimal.Decimal, bool) {
				return e.book.BookFor(symbol).ImpactPrices(notional)
			})
	e.liq = service.NewLiquidationService(e.engine, e.orders, positionRepo, positionSvc, e.markPrice, e.accounts,
		fundSvc, coinRepo, 200)
	return e
}

// 重启一个引擎：内存订单簿是全新的空的，数据库和Redis沿用，用来测恢复
func (e *engineEnv) restartEngine(t *testing.T) { e.restartEngineOwning(t, nil) }

// 重启一个只负责这些symbol的引擎(分片部署)，nil=负责全部
func (e *engineEnv) restartEngineOwning(t *testing.T, symbols []string) {
	t.Helper()
	conn, rdb := e.db, testutil.NewCache(t)
	positionRepo := repo.NewPositionRepo(conn)
	positionSvc := service.NewPositionService(positionRepo, repo.NewRiskLimitRepo(conn), e.markPrice)
	settlementSvc := service.NewSettlementService(e.accounts, positionRepo, repo.NewCoinRepo(conn), repo.NewTxRepo(conn))
	pushSvc := service.NewPushService(rdb, e.accounts, positionSvc, e.orders)
	e.book = matching.NewEngine()
	e.engine = service.NewEngineService(e.book, e.orders, e.conditional, e.trades, e.accounts, positionSvc,
		settlementSvc, e.markPrice, service.NewInsuranceFundService(repo.NewInsuranceFundRepo(conn)),
		service.NewKlineService(repo.NewKlineRepo(conn)), pushSvc, repo.NewRoundCloseProgressRepo(conn),
		service.NewLockService(rdb), repo.NewCoinRepo(conn), symbols)
}

func (e *engineEnv) id() uint64 { return e.nextID.Add(1) }

// 建一个账户并直接设置可用余额
func (e *engineEnv) newAccount(t *testing.T, offset uint64, available string) uint64 {
	t.Helper()
	uid := e.uidBase + offset
	if _, _, err := e.accountRepo.CreateIfAbsent(context.Background(), uid); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.Exec(`UPDATE accounts SET available = ? WHERE uid = ?`, available, uid); err != nil {
		t.Fatal(err)
	}
	return uid
}

func (e *engineEnv) setStatus(t *testing.T, uid uint64, status model.AccountStatus) {
	t.Helper()
	if _, _, err := e.accounts.SetStatus(context.Background(), uid, status, "测试", "it"); err != nil {
		t.Fatal(err)
	}
}

func (e *engineEnv) account(t *testing.T, uid uint64) *model.Account {
	t.Helper()
	a, err := e.accountRepo.FindByUID(context.Background(), uid)
	if err != nil || a == nil {
		t.Fatalf("查账户失败: %v %v", a, err)
	}
	return a
}

func (e *engineEnv) order(t *testing.T, orderID uint64) *model.Order {
	t.Helper()
	o, err := e.orders.FindByOrderID(context.Background(), orderID)
	if err != nil || o == nil {
		t.Fatalf("查委托失败: %v %v", o, err)
	}
	return o
}

type orderOpts struct {
	side        model.Side
	action      model.OrderAction
	price       string
	amount      string
	margin      string // 开仓时冻结的保证金，从available划到frozen_margin；平仓单填空
	liquidation bool
	symbol      string // 空=testSymbol
	market      bool   // true=市价单：price填contract-api下单时存进去的参考价(标记价)，跟真实下单落库的形态一致
}

// 模拟contract-api下单的落库结果：开仓先冻结保证金，再插入status=open的委托。不发事件、不撮合，
// 由测试自己决定什么时候交给引擎
func (e *engineEnv) insertOrder(t *testing.T, uid uint64, o orderOpts) *model.Order {
	t.Helper()
	ctx := context.Background()
	acc := e.account(t, uid)
	frozen := decimal.Zero
	if o.margin != "" {
		frozen = decimal.RequireFromString(o.margin)
		ok, err := e.accountRepo.FreezeFromAvailable(ctx, acc.ID, frozen)
		if err != nil || !ok {
			t.Fatalf("冻结保证金失败: ok=%v err=%v", ok, err)
		}
	}
	now := time.Now().UnixMilli()
	symbol := o.symbol
	if symbol == "" {
		symbol = testSymbol
	}
	orderType := model.OrderTypeLimit
	if o.market {
		orderType = model.OrderTypeMarket
	}
	order := &model.Order{
		OrderID:      e.id(),
		UID:          uid,
		Symbol:       symbol,
		Side:         o.side,
		Action:       o.action,
		Type:         orderType,
		Price:        decimal.RequireFromString(o.price),
		Amount:       decimal.RequireFromString(o.amount),
		TradedAmount: decimal.Zero,
		AvgDealPrice: decimal.Zero,
		FrozenMargin: frozen,
		FrozenCredit: decimal.Zero,
		Leverage:     10,
		Liquidation:  o.liquidation,
		Status:       model.OrderStatusOpen,
		CreateTime:   now,
		UpdateTime:   now,
	}
	if err := e.orders.Insert(ctx, order); err != nil {
		t.Fatal(err)
	}
	return order
}

func mustDec(t *testing.T, got decimal.Decimal, want string, what string) {
	t.Helper()
	if !got.Equal(decimal.RequireFromString(want)) {
		t.Fatalf("%s: got %s, want %s", what, got, want)
	}
}

func decimalOf(t *testing.T, s string) decimal.Decimal {
	t.Helper()
	d, err := decimal.NewFromString(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// 一笔待触发的条件开仓单(限价，价格=触发价，标记价涨到trigger及以上触发)，保证金由调用方先冻结好。
// 故意用限价单：市价单在没有对手盘时本来就会被撤销退款，跟"因为冻结被撤销"的结果一样，
// 测试就分辨不出冻结检查有没有生效；限价单没有冻结检查的话会挂进订单簿
func newConditionalOpen(e *engineEnv, uid uint64, trigger, amount, margin string) *model.ConditionalOrder {
	now := time.Now().UnixMilli()
	return &model.ConditionalOrder{
		OrderID:          e.id(),
		UID:              uid,
		Symbol:           testSymbol,
		Side:             model.SideLong,
		Action:           model.ActionOpen,
		TriggerPrice:     decimal.RequireFromString(trigger),
		TriggerDirection: model.TriggerGTE,
		Type:             model.OrderTypeLimit,
		Price:            decimal.RequireFromString(trigger),
		Amount:           decimal.RequireFromString(amount),
		Leverage:         10,
		FrozenMargin:     decimal.RequireFromString(margin),
		FrozenCredit:     decimal.Zero,
		Status:           model.ConditionalStatusPending,
		CreateTime:       now,
		UpdateTime:       now,
	}
}

func (e *engineEnv) position(t *testing.T, uid uint64, side model.Side) *model.Position {
	t.Helper()
	p, err := e.positions.Find(context.Background(), uid, testSymbol, side)
	if err != nil || p == nil {
		t.Fatalf("查仓位失败: %v %v", p, err)
	}
	return p
}

// 这个uid某类型流水的金额合计
func (e *engineEnv) ledgerSum(t *testing.T, uid uint64, txType string) decimal.Decimal {
	t.Helper()
	var sum decimal.Decimal
	if err := e.db.Get(&sum, `SELECT COALESCE(SUM(amount), 0) FROM member_transactions WHERE uid = ? AND type = ?`, uid, txType); err != nil {
		t.Fatal(err)
	}
	return sum
}

func mustParse(t *testing.T, s string) decimal.Decimal { return decimalOf(t, s) }

// 直接把标记价格写进Redis，绕开MarkPriceService的计算(指数价、基差、最新成交价)——大部分测试要的是
// "标记价就是这个值"，不关心它怎么算出来。要测计算本身用markprice_integration_test.go里的用例
func (e *engineEnv) setMark(t *testing.T, symbol, price string) {
	t.Helper()
	if err := testutil.NewCache(t).SetMarkPrice(context.Background(), symbol, price); err != nil {
		t.Fatal(err)
	}
}

// 删掉某个symbol跟标记价有关的全部Redis数据(标记价、最新成交价、指数价)，模拟"还从没成交过、没喂过价"。
// Redis是各测试共用的，这些键按symbol、不按测试隔离，需要"没有标记价格"的测试必须显式清一下
func (e *engineEnv) clearMark(t *testing.T, symbol string) {
	t.Helper()
	resetPriceKeys(t, symbol)
}

// 中途清一次(不注册新的Cleanup)：需要"从没成交过、没喂过价"状态的测试用
func resetPriceKeys(t *testing.T, symbol string) {
	t.Helper()
	keys := []string{"perpgo:mark:" + symbol, "perpgo:last:" + symbol, "perpgo:index:" + symbol, "perpgo:index_ts:" + symbol}
	if err := testutil.NewRedisClient(t).Del(context.Background(), keys...).Err(); err != nil {
		t.Fatal(err)
	}
}

// 直接给账户设信用额度和投保状态
func (e *engineEnv) setCredit(t *testing.T, uid uint64, credit string, insured bool) {
	t.Helper()
	if _, err := e.db.Exec(`UPDATE accounts SET credit = ?, is_insured = ? WHERE uid = ?`, credit, insured, uid); err != nil {
		t.Fatal(err)
	}
}

// 按symbol查仓位
func (e *engineEnv) positionOf(t *testing.T, uid uint64, symbol string, side model.Side) *model.Position {
	t.Helper()
	p, err := e.positions.Find(context.Background(), uid, symbol, side)
	if err != nil || p == nil {
		t.Fatalf("查仓位失败: %v %v", p, err)
	}
	return p
}
