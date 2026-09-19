//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/shopspring/decimal"

	"perp-go/internal/events"
	"perp-go/internal/model"
	"perp-go/internal/repo"
	"perp-go/internal/service"
	"perp-go/internal/testutil"
)

const testSymbol = "BTCUSDT"

type publishedEvent struct {
	Topic string
	Value any
}

// 只记录事件、不连Kafka的发布者，failing=true时所有发布都失败
type fakePublisher struct {
	mu      sync.Mutex
	events  []publishedEvent
	failing bool
}

func (f *fakePublisher) Publish(_ context.Context, topic, _ string, value any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failing {
		return errors.New("模拟Kafka不可用")
	}
	f.events = append(f.events, publishedEvent{Topic: topic, Value: value})
	return nil
}

func (f *fakePublisher) setFailing(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failing = v
}

func (f *fakePublisher) cancelOrderIDs() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var ids []uint64
	for _, e := range f.events {
		if e.Topic == events.TopicOrderCancel {
			ids = append(ids, e.Value.(events.OrderCancelEvent).OrderID)
		}
	}
	return ids
}

// 接到真实MySQL/Redis上的API服务(鉴权关闭)，只有Kafka换成了记录事件的替身
type apiEnv struct {
	db          *sqlx.DB
	srv         *Server
	pub         *fakePublisher
	accountRepo *repo.AccountRepo
	orders      *repo.OrderRepo
	conditional *repo.ConditionalOrderRepo
	handler     http.Handler
	uidBase     uint64
	seq         uint64
}

func newAPIEnv(t *testing.T) *apiEnv {
	t.Helper()
	service.InitNodeID(0)
	conn := testutil.NewDB(t)
	rdb := testutil.NewCache(t)

	e := &apiEnv{db: conn, pub: &fakePublisher{}, uidBase: testutil.UIDBase()}
	e.seq = e.uidBase * 1000
	e.accountRepo = repo.NewAccountRepo(conn)
	e.orders = repo.NewOrderRepo(conn)
	e.conditional = repo.NewConditionalOrderRepo(conn)
	positionRepo := repo.NewPositionRepo(conn)
	txRepo := repo.NewTxRepo(conn)
	markPrice := service.NewMarkPriceService(rdb)
	positions := service.NewPositionService(positionRepo, repo.NewRiskLimitRepo(conn), markPrice)
	accounts := service.NewAccountService(e.accountRepo, positions, txRepo)

	e.srv = &Server{
		accounts:          accounts,
		positions:         positions,
		coins:             repo.NewCoinRepo(conn),
		orders:            e.orders,
		conditionalOrders: e.conditional,
		trades:            repo.NewTradeRepo(conn),
		klines:            repo.NewKlineRepo(conn),
		markPrice:         markPrice,
		producer:          e.pub,
		lock:              service.NewLockService(rdb),
		txs:               txRepo,
		auth:              NewAuth(true, nil, nil),
	}
	e.handler = e.srv.Router()
	if err := markPrice.UpdateFromTrade(context.Background(), testSymbol, decimal.NewFromInt(65000)); err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *apiEnv) id() uint64 { e.seq++; return e.seq }

func (e *apiEnv) newAccount(t *testing.T, offset uint64, available string) uint64 {
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

func (e *apiEnv) account(t *testing.T, uid uint64) *model.Account {
	t.Helper()
	a, err := e.accountRepo.FindByUID(context.Background(), uid)
	if err != nil || a == nil {
		t.Fatalf("查账户失败: %v %v", a, err)
	}
	return a
}

// 发一个JSON请求，返回响应body解析出的map
func (e *apiEnv) post(t *testing.T, path string, body any) map[string]any {
	t.Helper()
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	e.handler.ServeHTTP(w, req)
	var m map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil {
		t.Fatalf("响应不是JSON: %v body=%s", err, w.Body.String())
	}
	return m
}

func data(t *testing.T, resp map[string]any) map[string]any {
	t.Helper()
	if resp["code"] != float64(200) {
		t.Fatalf("请求应该成功: %v", resp)
	}
	d, _ := resp["data"].(map[string]any)
	return d
}

func requireErr(t *testing.T, resp map[string]any, code float64, errCode string) {
	t.Helper()
	if resp["code"] != code || resp["errCode"] != errCode {
		t.Fatalf("应该返回 code=%v errCode=%s, got %v", code, errCode, resp)
	}
}

type orderSeed struct {
	action      model.OrderAction
	margin      string
	liquidation bool
}

// 直接落库一笔活跃委托(开仓的先把保证金冻结好)，模拟已经下过单
func (e *apiEnv) seedOrder(t *testing.T, uid uint64, o orderSeed) uint64 {
	t.Helper()
	ctx := context.Background()
	frozen := decimal.Zero
	if o.margin != "" {
		frozen = decimal.RequireFromString(o.margin)
		if ok, err := e.accountRepo.FreezeFromAvailable(ctx, e.account(t, uid).ID, frozen); err != nil || !ok {
			t.Fatalf("冻结保证金: ok=%v err=%v", ok, err)
		}
	}
	now := time.Now().UnixMilli()
	order := &model.Order{
		OrderID: e.id(), UID: uid, Symbol: testSymbol, Side: model.SideLong, Action: o.action,
		Type: model.OrderTypeLimit, Price: decimal.NewFromInt(64000), Amount: decimal.RequireFromString("0.1"),
		FrozenMargin: frozen, Leverage: 10, Liquidation: o.liquidation, Status: model.OrderStatusOpen,
		CreateTime: now, UpdateTime: now,
	}
	if err := e.orders.Insert(ctx, order); err != nil {
		t.Fatal(err)
	}
	return order.OrderID
}

func (e *apiEnv) seedConditional(t *testing.T, uid uint64, action model.OrderAction, margin string) uint64 {
	t.Helper()
	ctx := context.Background()
	frozen := decimal.Zero
	if margin != "" {
		frozen = decimal.RequireFromString(margin)
		if ok, err := e.accountRepo.FreezeFromAvailable(ctx, e.account(t, uid).ID, frozen); err != nil || !ok {
			t.Fatalf("冻结保证金: ok=%v err=%v", ok, err)
		}
	}
	now := time.Now().UnixMilli()
	co := &model.ConditionalOrder{
		OrderID: e.id(), UID: uid, Symbol: testSymbol, Side: model.SideLong, Action: action,
		TriggerPrice: decimal.NewFromInt(60000), TriggerDirection: model.TriggerLTE, Type: model.OrderTypeMarket,
		Amount: decimal.RequireFromString("0.1"), Leverage: 10, FrozenMargin: frozen,
		Status: model.ConditionalStatusPending, CreateTime: now, UpdateTime: now,
	}
	if err := e.conditional.Insert(ctx, co); err != nil {
		t.Fatal(err)
	}
	return co.OrderID
}

func (e *apiEnv) conditionalStatus(t *testing.T, id uint64) model.ConditionalOrderStatus {
	t.Helper()
	var s model.ConditionalOrderStatus
	if err := e.db.Get(&s, `SELECT status FROM conditional_orders WHERE order_id = ?`, id); err != nil {
		t.Fatal(err)
	}
	return s
}

func statusBody(uid uint64, status, reason string) map[string]any {
	return map[string]any{"uid": uid, "status": status, "reason": reason}
}

// 冻结：只清理开仓类的普通委托和条件单；平仓委托、强平委托、平仓类条件单、别的账户的单子不动
func TestSetAccountStatus_FreezeSweepsOnlyOpenOrders(t *testing.T) {
	e := newAPIEnv(t)
	uid := e.newAccount(t, 1, "10000")
	other := e.newAccount(t, 2, "10000")
	openOrder := e.seedOrder(t, uid, orderSeed{action: model.ActionOpen, margin: "640"})
	closeOrder := e.seedOrder(t, uid, orderSeed{action: model.ActionClose})
	liqOrder := e.seedOrder(t, uid, orderSeed{action: model.ActionOpen, margin: "100", liquidation: true})
	condOpen := e.seedConditional(t, uid, model.ActionOpen, "600")
	condClose := e.seedConditional(t, uid, model.ActionClose, "")
	otherOrder := e.seedOrder(t, other, orderSeed{action: model.ActionOpen, margin: "640"})
	_ = closeOrder

	d := data(t, e.post(t, "/account/status", statusBody(uid, "frozen", "风控")))

	if d["changed"] != true || d["status"] != "frozen" {
		t.Fatalf("响应不对: %v", d)
	}
	if d["cancelRequested"] != float64(1) || d["cancelRequestFailed"] != float64(0) ||
		d["conditionalCanceled"] != float64(1) || d["conditionalFailed"] != float64(0) {
		t.Fatalf("清理计数不对: %v", d)
	}
	ids := e.pub.cancelOrderIDs()
	if len(ids) != 1 || ids[0] != openOrder {
		t.Fatalf("只应该给开仓委托发一条撤单事件, got %v (开仓单=%d 平仓单=%d 强平单=%d 别人的单=%d)", ids, openOrder, closeOrder, liqOrder, otherOrder)
	}
	if got := e.conditionalStatus(t, condOpen); got != model.ConditionalStatusCanceled {
		t.Fatalf("条件开仓单应该被撤销, status=%s", got)
	}
	if got := e.conditionalStatus(t, condClose); got != model.ConditionalStatusPending {
		t.Fatalf("条件平仓单应该保留, status=%s", got)
	}
	acc := e.account(t, uid)
	if acc.Status != model.AccountStatusFrozen {
		t.Fatalf("账户应该是frozen, got %s", acc.Status)
	}
	// 条件开仓单的600同步退回；普通开仓单的640要等引擎处理撤单事件才退，强平单的100不动
	if !acc.FrozenMargin.Equal(decimal.NewFromInt(740)) {
		t.Fatalf("冻结保证金应该剩640(普通开仓单，等引擎撤)+100(强平单)=740, got %s", acc.FrozenMargin)
	}
	if !e.account(t, other).FrozenMargin.Equal(decimal.NewFromInt(640)) {
		t.Fatal("别的账户不应该受影响")
	}
}

// 发撤单事件失败时如实报告失败数，状态已经改成功；带同样的参数重试(changed=false)会把清理补完
func TestSetAccountStatus_PartialFailureCanBeRetried(t *testing.T) {
	e := newAPIEnv(t)
	uid := e.newAccount(t, 1, "10000")
	openOrder := e.seedOrder(t, uid, orderSeed{action: model.ActionOpen, margin: "640"})
	e.pub.setFailing(true)

	d := data(t, e.post(t, "/account/status", statusBody(uid, "frozen", "风控")))
	if d["changed"] != true || d["cancelRequested"] != float64(0) || d["cancelRequestFailed"] != float64(1) {
		t.Fatalf("Kafka不可用时应该报告1笔撤单请求失败: %v", d)
	}
	if e.account(t, uid).Status != model.AccountStatusFrozen {
		t.Fatal("状态应该已经改成功")
	}

	e.pub.setFailing(false)
	d = data(t, e.post(t, "/account/status", statusBody(uid, "frozen", "风控")))
	if d["changed"] != false || d["cancelRequested"] != float64(1) || d["cancelRequestFailed"] != float64(0) {
		t.Fatalf("重试应该changed=false但补完清理: %v", d)
	}
	if ids := e.pub.cancelOrderIDs(); len(ids) != 1 || ids[0] != openOrder {
		t.Fatalf("重试后应该发出那笔开仓单的撤单事件, got %v", ids)
	}
}

// 解冻不清理任何东西
func TestSetAccountStatus_UnfreezeDoesNotSweep(t *testing.T) {
	e := newAPIEnv(t)
	uid := e.newAccount(t, 1, "10000")
	e.seedOrder(t, uid, orderSeed{action: model.ActionOpen, margin: "640"})
	condOpen := e.seedConditional(t, uid, model.ActionOpen, "600")

	data(t, e.post(t, "/account/status", statusBody(uid, "frozen", "")))
	before := len(e.pub.cancelOrderIDs())
	d := data(t, e.post(t, "/account/status", statusBody(uid, "active", "复核通过")))

	if d["changed"] != true || d["status"] != "active" {
		t.Fatalf("解冻响应不对: %v", d)
	}
	if d["cancelRequested"] != float64(0) || d["conditionalCanceled"] != float64(0) {
		t.Fatalf("解冻不应该清理: %v", d)
	}
	if len(e.pub.cancelOrderIDs()) != before {
		t.Fatal("解冻不应该发撤单事件")
	}
	if e.account(t, uid).Status != model.AccountStatusActive {
		t.Fatal("应该已经解冻")
	}
	_ = condOpen
}

func TestSetAccountStatus_Validation(t *testing.T) {
	e := newAPIEnv(t)
	uid := e.newAccount(t, 1, "0")

	requireErr(t, e.post(t, "/account/status", statusBody(uid, "banned", "")), 400, "invalid_param")
	requireErr(t, e.post(t, "/account/status", statusBody(uid, "", "")), 400, "invalid_param")
	requireErr(t, e.post(t, "/account/status", statusBody(uid+999, "frozen", "")), 400, "account_not_found")
	long := make([]byte, 256)
	for i := range long {
		long[i] = 'a'
	}
	requireErr(t, e.post(t, "/account/status", statusBody(uid, "frozen", string(long))), 400, "invalid_param")
	if e.account(t, uid).Status != model.AccountStatusActive {
		t.Fatal("校验失败的请求不应该改状态")
	}
}

// 冻结账户：开仓、条件开仓、改杠杆被拒绝；平仓单不会被冻结检查拦住(这里让它死在别的校验上：
// 合约不存在)；创建账户和查询照常
func TestFrozenAccount_GatesOnNewRisk(t *testing.T) {
	e := newAPIEnv(t)
	uid := e.newAccount(t, 1, "10000")
	data(t, e.post(t, "/account/status", statusBody(uid, "frozen", "")))

	openBody := map[string]any{"uid": uid, "symbol": testSymbol, "side": "long", "action": "open", "type": "limit",
		"price": 64000, "amount": 0.1, "leverage": 10, "requestId": "o-frozen"}
	requireErr(t, e.post(t, "/order/add", openBody), 400, "account_frozen")

	requireErr(t, e.post(t, "/order/conditional/add", map[string]any{"uid": uid, "symbol": testSymbol, "side": "long",
		"action": "open", "triggerPrice": 60000, "triggerDirection": "lte", "type": "market", "amount": 0.1}), 400, "account_frozen")

	requireErr(t, e.post(t, "/position/leverage", map[string]any{"uid": uid, "symbol": testSymbol, "side": "long", "leverage": 5}), 400, "account_frozen")

	// 平仓单/条件平仓单过了冻结检查，才会走到后面的合约校验
	requireErr(t, e.post(t, "/order/add", map[string]any{"uid": uid, "symbol": "NOPE", "side": "long", "action": "close",
		"type": "limit", "price": 64000, "amount": 0.1}), 400, "symbol_not_found")
	requireErr(t, e.post(t, "/order/conditional/add", map[string]any{"uid": uid, "symbol": "NOPE", "side": "long",
		"action": "close", "triggerPrice": 70000, "triggerDirection": "gte", "type": "market", "amount": 0.1}), 400, "symbol_not_found")

	// 没有请求成功落库任何东西
	orders, err := e.orders.FindActiveByUID(context.Background(), uid, "")
	if err != nil || len(orders) != 0 {
		t.Fatalf("被拒绝的请求不应该落库委托: %v %v", orders, err)
	}
	if !e.account(t, uid).FrozenMargin.IsZero() {
		t.Fatal("被拒绝的请求不应该冻结保证金")
	}
}

// 端到端：账户正常时下单成功；冻结之后同一个requestId重试仍然返回第一次的结果(duplicate)，
// 换一个新的requestId才是account_frozen；解冻后新单恢复正常
func TestFrozenAccount_IdempotentReplayStillReturnsOriginal(t *testing.T) {
	e := newAPIEnv(t)
	uid := e.newAccount(t, 1, "10000")
	body := func(reqID string) map[string]any {
		return map[string]any{"uid": uid, "symbol": testSymbol, "side": "long", "action": "open", "type": "limit",
			"price": 64000, "amount": 0.1, "leverage": 10, "requestId": reqID}
	}

	first := data(t, e.post(t, "/order/add", body("r1")))
	orderID := first["orderId"]
	if orderID == nil || first["duplicate"] != nil {
		t.Fatalf("第一次下单应该成功且不是重复: %v", first)
	}

	data(t, e.post(t, "/account/status", statusBody(uid, "frozen", "")))

	replay := data(t, e.post(t, "/order/add", body("r1")))
	if replay["duplicate"] != true || replay["orderId"] != orderID {
		t.Fatalf("冻结后同一个requestId重试应该返回原结果: %v", replay)
	}
	requireErr(t, e.post(t, "/order/add", body("r2")), 400, "account_frozen")

	data(t, e.post(t, "/account/status", statusBody(uid, "active", "")))
	if second := data(t, e.post(t, "/order/add", body("r3"))); second["orderId"] == nil {
		t.Fatalf("解冻后应该能正常下单: %v", second)
	}
}

// 账户信息里带status
func TestAccountInfo_IncludesStatus(t *testing.T) {
	e := newAPIEnv(t)
	uid := e.newAccount(t, 1, "0")
	get := func() string {
		req := httptest.NewRequest(http.MethodGet, "/account/info?uid="+itoa(uid), nil)
		w := httptest.NewRecorder()
		e.handler.ServeHTTP(w, req)
		var m map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &m)
		return data(t, m)["status"].(string)
	}
	if got := get(); got != "active" {
		t.Fatalf("新账户应该是active, got %s", got)
	}
	data(t, e.post(t, "/account/status", statusBody(uid, "frozen", "")))
	if got := get(); got != "frozen" {
		t.Fatalf("冻结后应该是frozen, got %s", got)
	}
}

func itoa(u uint64) string { return decimal.NewFromInt(int64(u)).String() }
