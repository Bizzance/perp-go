package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

// 假的后端：内存里记账户和挂单，实现做市要用到的接口。只验证做市"发了哪些请求"，不验证撮合。
type fakeAPI struct {
	mu       sync.Mutex
	orders   map[string]fakeOrder // orderId -> 挂单
	nextID   int
	calls    []string // "METHOD path"
	requests []map[string]any
	accounts map[uint64]bool
	failOn   string // 路径包含这个字符串的请求返回业务错误
	mark     string // /market/ticker返回的标记价，空=null(合约还没有成交过)

	asyncCancel bool          // true=撤单请求只记下来不立刻生效(模拟经Kafka异步撤单)，flushCancels后才真正摘掉
	pending     []string      // 已经收到撤单请求、还没生效的委托
	failTicker  bool          // /market/ticker返回业务错误
	failUID     uint64        // 这个uid的下单请求返回业务错误
	delay       time.Duration // 每个请求先睡这么久，让做市循环停在一个进行中的周期里
}

type fakeOrder struct {
	uid          uint64
	side, price  string
	amount       string
	traded       string
	action, typ_ string
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{orders: map[string]fakeOrder{}, accounts: map[uint64]bool{}, nextID: 1000}
}

func (f *fakeAPI) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		delay := f.delay
		f.mu.Unlock()
		time.Sleep(delay)
		f.mu.Lock()
		defer f.mu.Unlock()
		raw, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		f.calls = append(f.calls, r.Method+" "+r.URL.Path)
		f.requests = append(f.requests, body)
		reply := func(data any) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 200, "data": data})
		}
		if f.failOn != "" && strings.Contains(r.URL.Path, f.failOn) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 400, "errCode": "invalid_param", "message": "模拟失败"})
			return
		}
		uid := func() uint64 { u, _ := strconv.ParseUint(r.URL.Query().Get("uid"), 10, 64); return u }
		switch {
		case r.URL.Path == "/account/create":
			f.accounts[uint64(body["uid"].(float64))] = true
			reply(map[string]any{})
		case r.URL.Path == "/account/info":
			reply(map[string]any{"balance": "0", "round": 1})
		case r.URL.Path == "/account/balance", r.URL.Path == "/index-price":
			reply(map[string]any{})
		case r.URL.Path == "/market/ticker":
			if f.failTicker {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"code": 500, "errCode": "internal_error", "message": "模拟失败"})
				return
			}
			var mark any
			if f.mark != "" {
				mark = f.mark
			}
			reply(map[string]any{"markPrice": mark})
		case r.URL.Path == "/contract/detail":
			reply(map[string]any{"baseCoinScale": 3, "minVolume": "0.001"})
		case r.URL.Path == "/order/current":
			var out []map[string]any
			for id, o := range f.orders {
				if o.uid == uid() {
					out = append(out, map[string]any{"orderId": id, "side": o.side, "action": o.action, "type": o.typ_,
						"price": o.price, "amount": o.amount, "tradedAmount": o.traded})
				}
			}
			reply(out)
		case r.URL.Path == "/order/add":
			if f.failUID != 0 && uint64(body["uid"].(float64)) == f.failUID {
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"code": 400, "errCode": "invalid_param", "message": "模拟这个账户下单失败"})
				return
			}
			f.nextID++
			id := strconv.Itoa(f.nextID)
			f.orders[id] = fakeOrder{uid: uint64(body["uid"].(float64)), side: body["side"].(string), price: body["price"].(string),
				amount: body["amount"].(string), traded: "0", action: body["action"].(string), typ_: body["type"].(string)}
			reply(map[string]any{"orderId": id})
		case strings.HasPrefix(r.URL.Path, "/order/cancel/"):
			id := strings.TrimPrefix(r.URL.Path, "/order/cancel/")
			if f.asyncCancel {
				f.pending = append(f.pending, id)
			} else {
				delete(f.orders, id)
			}
			reply("ok")
		case r.URL.Path == "/order/cancel-all":
			u := uint64(body["uid"].(float64))
			for id, o := range f.orders {
				if o.uid == u {
					delete(f.orders, id)
				}
			}
			reply(map[string]any{})
		default:
			http.NotFound(w, r)
		}
	})
}

func (f *fakeAPI) count(prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if strings.HasPrefix(c, prefix) {
			n++
		}
	}
	return n
}

func (f *fakeAPI) liveByUID(uid uint64) []fakeOrder {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []fakeOrder
	for _, o := range f.orders {
		if o.uid == uid {
			out = append(out, o)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].price < out[j].price })
	return out
}

// 假的币安：固定盘口。价格从80000.0起，买卖各一百多档，每档一个BTC
func fakeBinance(t *testing.T, shift *float64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/fapi/v1/depth":
			var bids, asks [][]string
			for i := 0; i < 300; i++ {
				bids = append(bids, []string{fmt.Sprintf("%.1f", 80000.0+*shift-float64(i)*0.1), "1"})
				asks = append(asks, []string{fmt.Sprintf("%.1f", 80000.1+*shift+float64(i)*0.1), "1"})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"bids": bids, "asks": asks})
		case "/fapi/v1/premiumIndex":
			_, _ = w.Write([]byte(`{"markPrice":"80001.5","indexPrice":"80002.5"}`))
		default:
			http.NotFound(w, r)
		}
	}))
}

func newTestMaker(t *testing.T, api *fakeAPI) *maker {
	t.Helper()
	m, _ := newTestMakerWithShift(t, api)
	return m
}

// 返回的*float64是假币安的价格平移量，测试里改它就等于币安的价格在动
func newTestMakerWithShift(t *testing.T, api *fakeAPI) (*maker, *float64) {
	t.Helper()
	us := httptest.NewServer(api.handler())
	t.Cleanup(us.Close)
	shift := new(float64)
	bs := fakeBinance(t, shift)
	t.Cleanup(bs.Close)
	cfg := defaultMakerConfig()
	cfg.symbols = []string{"BTCUSDT"}
	cfg.levels = 5
	cfg.printEvery = 0
	return newMaker(cfg, &apiClient{base: us.URL, http: &http.Client{Timeout: 5 * time.Second}}, newBinanceClient(bs.URL)), shift
}

func (f *fakeAPI) flushCancels() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range f.pending {
		delete(f.orders, id)
	}
	f.pending = nil
}

// 系统的买盘最高价是否碰到了卖盘最低价(含还没真正摘掉的旧单)
func systemBookCrossed(f *fakeAPI, m *maker) bool {
	bids, asks := f.liveByUID(m.uid(0)), f.liveByUID(m.uid(1))
	if len(bids) == 0 || len(asks) == 0 {
		return false
	}
	maxBid, _ := decimal.NewFromString(bids[len(bids)-1].price)
	minAsk, _ := decimal.NewFromString(asks[0].price)
	return maxBid.GreaterThanOrEqual(minAsk)
}

var validRequestID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// 第一个周期：四个系统账户建好，买卖各挂5档(每档是币安对应价格桶里的数量之和)，同步指数价，对敲一笔
func TestMaker_FirstCycleQuotesBothSidesAndPrints(t *testing.T) {
	api := newFakeAPI()
	m := newTestMaker(t, api)
	ctx := context.Background()

	if !m.ensureReady(ctx) {
		t.Fatalf("ensureReady失败: %s", m.snapshot().LastError)
	}
	if err := m.cycle(ctx, "BTCUSDT"); err != nil {
		t.Fatal(err)
	}

	if len(api.accounts) != 4 {
		t.Fatalf("应该建4个系统账户: %v", api.accounts)
	}
	bids, asks := api.liveByUID(m.uid(0)), api.liveByUID(m.uid(1))
	if len(bids) != 5 || len(asks) != 5 {
		t.Fatalf("买卖盘各应该挂5档: 买%d 卖%d", len(bids), len(asks))
	}
	// 币安买一80000.0：向下取整到5的整数倍还是80000；每个桶50个价位，每档一个BTC -> 数量50(第一个桶只有80000.0..79995.1)
	for _, o := range bids {
		if o.side != "long" || o.action != "open" || o.typ_ != "limit" {
			t.Fatalf("买盘做市应该是开多限价单: %+v", o)
		}
		if p, _ := decimal.NewFromString(o.price); !p.LessThanOrEqual(d("80000")) {
			t.Fatalf("买盘价格不能高于币安买一: %s", o.price)
		}
	}
	for _, o := range asks {
		if o.side != "short" {
			t.Fatalf("卖盘做市应该是开空: %+v", o)
		}
		if p, _ := decimal.NewFromString(o.price); !p.GreaterThanOrEqual(d("80005")) {
			t.Fatalf("卖盘价格不能低于币安卖一向上取整的桶: %s", o.price)
		}
	}
	if api.count("POST /index-price") != 1 {
		t.Fatal("应该把币安的指数价同步过来")
	}
	// 对敲：对敲卖(uid+3，开空)和对敲买(uid+2，开多)各一笔，价格在我们的买一卖一之间
	printSell, printBuy := api.liveByUID(m.uid(3)), api.liveByUID(m.uid(2))
	if len(printSell) != 1 || len(printBuy) != 1 || printSell[0].price != printBuy[0].price {
		t.Fatalf("应该各对敲一笔且同价: 卖%v 买%v", printSell, printBuy)
	}
	for _, r := range api.requests {
		if id, ok := r["requestId"].(string); ok && !validRequestID.MatchString(id) {
			t.Fatalf("requestId不合法(只允许字母数字和_-，长度1到64): %q", id)
		}
	}
}

// 行情没变、挂单都在：第二个周期不撤也不补挂(不为了没变化的行情天天撤了重挂)
func TestMaker_SecondCycleWithSameMarketDoesNothingToTheLadder(t *testing.T) {
	api := newFakeAPI()
	m := newTestMaker(t, api)
	ctx := context.Background()
	m.ensureReady(ctx)
	if err := m.cycle(ctx, "BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	addsBefore, cancelsBefore := api.count("POST /order/add"), api.count("POST /order/cancel/")

	if err := m.cycle(ctx, "BTCUSDT"); err != nil {
		t.Fatal(err)
	}

	// 第二个周期只有"清理对敲残留"的cancel-all，做市档位本身没有新的下单/撤单
	if got := api.count("POST /order/add") - addsBefore; got != 0 {
		t.Fatalf("行情没变不应该有新下单, got %d", got)
	}
	if got := api.count("POST /order/cancel/") - cancelsBefore; got != 0 {
		t.Fatalf("行情没变不应该撤做市挂单, got %d", got)
	}
}

// 有档位被吃掉大半：下个周期撤掉重挂补回
func TestMaker_RefillsLevelsThatWereMostlyEaten(t *testing.T) {
	api := newFakeAPI()
	m := newTestMaker(t, api)
	ctx := context.Background()
	m.ensureReady(ctx)
	if err := m.cycle(ctx, "BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	var eatenID, eatenPrice string
	api.mu.Lock()
	for id, o := range api.orders {
		if o.uid == m.uid(1) {
			eatenID, eatenPrice = id, o.price
			o.traded = o.amount // 全部被吃掉
			api.orders[id] = o
			break
		}
	}
	api.mu.Unlock()

	if err := m.cycle(ctx, "BTCUSDT"); err != nil {
		t.Fatal(err)
	}

	api.mu.Lock()
	_, stillThere := api.orders[eatenID]
	api.mu.Unlock()
	if stillThere {
		t.Fatal("被吃掉的挂单应该被撤掉")
	}
	refilled := false
	for _, o := range api.liveByUID(m.uid(1)) {
		if o.price == eatenPrice && o.traded == "0" {
			refilled = true
		}
	}
	if !refilled {
		t.Fatalf("价位%s应该重新挂满", eatenPrice)
	}
}

// 停止做市：系统账户的挂单全部撤掉，不留过期报价
func TestMaker_StopCancelsAllSystemOrders(t *testing.T) {
	api := newFakeAPI()
	m := newTestMaker(t, api)
	m.start()
	deadline := time.Now().Add(5 * time.Second)
	for len(api.liveByUID(m.uid(0))) == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if len(api.liveByUID(m.uid(0))) == 0 {
		t.Fatalf("做市应该挂出了单子: %s", m.snapshot().LastError)
	}

	m.stop()

	for i := 0; i < 4; i++ {
		if left := api.liveByUID(m.uid(i)); len(left) != 0 {
			t.Fatalf("停止后uid+%d还有挂单: %v", i, left)
		}
	}
	if m.snapshot().Enabled {
		t.Fatal("停止后状态应该是关闭")
	}
}

// 后端出错：记到状态里，做市循环不崩
func TestMaker_BackendErrorIsRecordedAndDoesNotPanic(t *testing.T) {
	api := newFakeAPI()
	api.failOn = "/order/add"
	m := newTestMaker(t, api)
	ctx := context.Background()
	m.ensureReady(ctx)

	err := m.cycle(ctx, "BTCUSDT")

	if err == nil || !strings.Contains(err.Error(), "invalid_param") {
		t.Fatalf("下单失败应该返回错误: %v", err)
	}
}

// 币安连不上：返回错误(带状态码信息)，不去动我们订单簿里已有的挂单
func TestMaker_BinanceDownKeepsExistingOrders(t *testing.T) {
	api := newFakeAPI()
	m := newTestMaker(t, api)
	ctx := context.Background()
	m.ensureReady(ctx)
	if err := m.cycle(ctx, "BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	before := len(api.liveByUID(m.uid(0)))
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "restricted location", http.StatusUnavailableForLegalReasons)
	}))
	defer down.Close()
	m.bn = newBinanceClient(down.URL)

	err := m.cycle(ctx, "BTCUSDT")

	if err == nil || !strings.Contains(err.Error(), "451") {
		t.Fatalf("应该返回带451状态码的错误: %v", err)
	}
	if after := len(api.liveByUID(m.uid(0))); after != before {
		t.Fatalf("币安连不上不应该动已有挂单: %d -> %d", before, after)
	}
}

func (f *fakeAPI) lastIndexPrice() decimal.Decimal {
	f.mu.Lock()
	defer f.mu.Unlock()
	var p decimal.Decimal
	for i, c := range f.calls {
		if c == "POST /index-price" {
			p, _ = decimal.NewFromString(f.requests[i]["price"].(string))
		}
	}
	return p
}

func offsetOf(m *maker, sym string) string {
	for _, s := range m.snapshot().Symbols {
		if s.Symbol == sym {
			return s.OffsetPct
		}
	}
	return ""
}

// 行情情景：目标偏移-5%，每个周期最多推进1个百分点。整个盘口和指数价都乘上偏移系数
func TestMaker_ScenarioOffsetShiftsQuotesAndIndexPrice(t *testing.T) {
	api := newFakeAPI()
	m := newTestMaker(t, api)
	ctx := context.Background()
	m.ensureReady(ctx)
	if err := m.setTarget(d("-5")); err != nil {
		t.Fatal(err)
	}

	if err := m.cycle(ctx, "BTCUSDT"); err != nil {
		t.Fatal(err)
	}

	if got := offsetOf(m, "BTCUSDT"); got != "-1" {
		t.Fatalf("第一个周期应该推进到-1%%, got %s", got)
	}
	// 币安买一80000.0 * 0.99 = 79200，买盘挂单不能高于它；卖一80000.1*0.99=79200.099，卖盘向上取整到79205
	for _, o := range api.liveByUID(m.uid(0)) {
		if p, _ := decimal.NewFromString(o.price); p.GreaterThan(d("79200")) {
			t.Fatalf("买盘价格应该已经乘上0.99: %s", o.price)
		}
	}
	for _, o := range api.liveByUID(m.uid(1)) {
		if p, _ := decimal.NewFromString(o.price); p.LessThan(d("79205")) || p.GreaterThan(d("79300")) {
			t.Fatalf("卖盘价格应该在79205附近往上: %s", o.price)
		}
	}
	// 指数价80002.5 * 0.99 = 79202.475
	if got := api.lastIndexPrice(); !got.Equal(d("79202.48")) {
		t.Fatalf("指数价也要乘上偏移系数: %s", got)
	}
}

// 偏移每个周期最多推进一步，到了目标就不动了：不能一步跳过去(价格保护带只有5%)
func TestMaker_ScenarioOffsetRampsOneStepPerCycle(t *testing.T) {
	api := newFakeAPI()
	m := newTestMaker(t, api)
	ctx := context.Background()
	m.ensureReady(ctx)
	_ = m.setTarget(d("-3"))

	var seen []string
	for i := 0; i < 5; i++ {
		if err := m.cycle(ctx, "BTCUSDT"); err != nil {
			t.Fatal(err)
		}
		seen = append(seen, offsetOf(m, "BTCUSDT"))
	}

	want := []string{"-1", "-2", "-3", "-3", "-3"}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("偏移应该逐步推进到目标后停住: got %v want %v", seen, want)
	}
}

// 启动时我们的标记价已经离币安很远(上一次的情景没走回来)：先按标记价反推当前偏移，再往目标(0)一步步走，
// 而不是直接按偏移0去挂单——那样价格离标记价超过5%，挂单全被拒
func TestMaker_InitialOffsetIsImpliedFromOurMarkPrice(t *testing.T) {
	api := newFakeAPI()
	api.mark = "76000" // 币安中间价约80000，我们的标记价低了5%
	m := newTestMaker(t, api)
	ctx := context.Background()
	m.ensureReady(ctx)

	if err := m.cycle(ctx, "BTCUSDT"); err != nil {
		t.Fatal(err)
	}

	if got := offsetOf(m, "BTCUSDT"); got != "-4" {
		t.Fatalf("应该从反推出的-5%%往目标0推进一步到-4%%, got %s", got)
	}
}

func TestMaker_SetTargetRejectsExtremeOffsets(t *testing.T) {
	m := newTestMaker(t, newFakeAPI())
	if err := m.setTarget(d("31")); err == nil {
		t.Fatal("+31%应该被拒绝")
	}
	if err := m.setTarget(d("-30")); err != nil {
		t.Fatalf("-30%%是允许的上限: %v", err)
	}
	if got := m.snapshot().TargetPct; got != "-30" {
		t.Fatalf("状态里应该有目标偏移: %s", got)
	}
}

// 撤单是异步的(经Kafka)，撤单请求返回不代表旧单已经摘掉。币安价格上行时，新的买单可能高于还没摘掉的旧卖单，
// 两个系统账户之间就会真实成交、把最新成交价拖回过期的价格。所以新挂的买单必须低于所有还挂着的卖单，
// 被挡下的档位留到下个周期
func TestMaker_NoSelfCrossWhenPriceRisesAndCancelsAreAsync(t *testing.T) {
	api := newFakeAPI()
	api.asyncCancel = true
	m, shift := newTestMakerWithShift(t, api)
	ctx := context.Background()
	m.ensureReady(ctx)
	if err := m.cycle(ctx, "BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	*shift = 50 // 币安上涨50美元，整条阶梯都要往上挪

	if err := m.cycle(ctx, "BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	if systemBookCrossed(api, m) {
		t.Fatalf("旧单还没摘掉时，新挂的买单不能高于旧卖单: 买%v 卖%v", api.liveByUID(m.uid(0)), api.liveByUID(m.uid(1)))
	}

	api.flushCancels() // 引擎处理完撤单事件
	if err := m.cycle(ctx, "BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	api.flushCancels()
	if systemBookCrossed(api, m) {
		t.Fatal("下个周期补挂之后也不能交叉")
	}
	if b, a := len(api.liveByUID(m.uid(0))), len(api.liveByUID(m.uid(1))); b != 5 || a != 5 {
		t.Fatalf("被挡下的档位应该在下个周期补挂满: 买%d 卖%d", b, a)
	}
}

// 对敲的买单失败：已经挂出去的卖单不能孤零零留在订单簿里(成了最优卖价被用户吃掉)，下个周期要清理
func TestMaker_FailedPrintLeavesNoOrphanOrderNextCycle(t *testing.T) {
	api := newFakeAPI()
	m := newTestMaker(t, api)
	ctx := context.Background()
	m.ensureReady(ctx)
	api.failUID = m.uid(2) // 对敲买账户下单一直失败
	if err := m.cycle(ctx, "BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	if len(api.liveByUID(m.uid(3))) != 1 {
		t.Fatalf("前提：对敲卖单已经挂出去了: %v", api.liveByUID(m.uid(3)))
	}

	api.failUID = 0
	before := api.count("POST /order/cancel-all")
	if err := m.cycle(ctx, "BTCUSDT"); err != nil {
		t.Fatal(err)
	}

	if api.count("POST /order/cancel-all")-before < 2 {
		t.Fatal("下个周期应该对两个对敲账户都发清理残留挂单的请求")
	}
}

// 双击做市开关：start/stop并发调用不能起出两个循环去读写同一批map(会崩进程)。配合go test -race
func TestMaker_ConcurrentStartStopIsSafe(t *testing.T) {
	api := newFakeAPI()
	m := newTestMaker(t, api)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				m.start()
			} else {
				m.stop()
			}
		}(i)
	}
	wg.Wait()
	m.stop()

	if m.snapshot().Enabled {
		t.Fatal("最后一次stop之后应该是关闭状态")
	}
	for i := 0; i < 4; i++ {
		if left := api.liveByUID(m.uid(i)); len(left) != 0 {
			t.Fatalf("停止后uid+%d还有挂单: %v", i, left)
		}
	}
}

// 取标记价反推偏移失败：这个周期不挂单、返回错误，下个周期重试；不能把偏移固定成0继续往下走
// (那样之后每笔下单都因为超出价格保护带被拒，标记价永远追不上)
func TestMaker_OffsetInitFailureIsRetriedInsteadOfPinnedToZero(t *testing.T) {
	api := newFakeAPI()
	api.failTicker = true
	m := newTestMaker(t, api)
	ctx := context.Background()
	m.ensureReady(ctx)

	if err := m.cycle(ctx, "BTCUSDT"); err == nil {
		t.Fatal("取不到标记价应该返回错误")
	}
	if len(api.liveByUID(m.uid(0))) != 0 {
		t.Fatal("偏移没确定之前不能挂单")
	}

	api.failTicker = false
	api.mark = "76000"
	if err := m.cycle(ctx, "BTCUSDT"); err != nil {
		t.Fatal(err)
	}
	if got := offsetOf(m, "BTCUSDT"); got != "-4" {
		t.Fatalf("重试成功后应该按标记价反推(-5%%)再推进一步到-4%%, got %s", got)
	}
}

// 标记价离币安很远(-50%)：反推的偏移不能截断到±30%，截断后的报价仍然进不了价格保护带
func TestMaker_ImpliedOffsetIsNotClampedToTheScenarioRange(t *testing.T) {
	api := newFakeAPI()
	api.mark = "40000"
	m := newTestMaker(t, api)
	ctx := context.Background()
	m.ensureReady(ctx)

	if err := m.cycle(ctx, "BTCUSDT"); err != nil {
		t.Fatal(err)
	}

	if got := offsetOf(m, "BTCUSDT"); got != "-49" {
		t.Fatalf("从反推出的约-50%%往0推进一步，应该是-49%%: %s", got)
	}
}

// 指数价同步失败只影响资金费率的参照，不能让整个周期的挂单都停掉
func TestMaker_IndexPriceFailureDoesNotStopQuoting(t *testing.T) {
	api := newFakeAPI()
	api.failOn = "/index-price"
	m := newTestMaker(t, api)
	ctx := context.Background()
	m.ensureReady(ctx)

	if err := m.cycle(ctx, "BTCUSDT"); err != nil {
		t.Fatalf("指数价失败不应该让周期失败: %v", err)
	}

	if len(api.liveByUID(m.uid(0))) == 0 || len(api.liveByUID(m.uid(1))) == 0 {
		t.Fatal("指数价失败时买卖盘仍然要挂出来")
	}
	if !strings.Contains(m.snapshot().LastError, "指数价") {
		t.Fatalf("错误应该记到状态里: %q", m.snapshot().LastError)
	}
}

// stop在锁外等循环退出：旧循环还没被取消时，并发的start如果能进来，就会起出第二个循环，两个循环无锁读写同一批map
// (Go会直接fatal，整个进程崩掉)。在stop摘掉cancel之后、取消旧循环之前的钩子里发起start，验证start被串行化到
// stop之后，同时运行的循环数永远不超过1
func TestMaker_StartDuringStopDoesNotRunTwoLoops(t *testing.T) {
	api := newFakeAPI()
	m := newTestMaker(t, api)
	m.start()
	deadline := time.Now().Add(10 * time.Second)
	for len(api.liveByUID(m.uid(0))) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	started := make(chan struct{})
	m.afterDetach = func() {
		go func() { m.start(); close(started) }()
		time.Sleep(200 * time.Millisecond) // 没有串行化的话，start会在这段时间里起出第二个循环
	}

	m.stop()
	m.afterDetach = nil
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("stop结束之后，被串行化的start应该能继续执行")
	}
	time.Sleep(100 * time.Millisecond)
	m.stop()

	if got := m.loopsMax.Load(); got != 1 {
		t.Fatalf("同时运行的做市循环数最多应该是1, got %d", got)
	}
}
