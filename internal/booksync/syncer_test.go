package booksync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/api"
)

const testSecret = "s3cret-s3cret-s3cret"

var requestIDRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// ---- 假的contract-api：内存里的账户和订单，校验签名和requestId格式 ----

type fakeOrder struct {
	id, uid, symbol, side, price, amount, traded string
	leverage                                     int
}

type fakeBackend struct {
	t   *testing.T
	srv *httptest.Server

	mu         sync.Mutex
	orders     map[string]*fakeOrder
	nextID     int
	avail      map[string]string // uid -> 可用余额
	topups     []string
	contracts  map[string][2]string // symbol -> {baseCoinScale, minVolume}
	adds       int
	cancels    int
	badSig     int
	failCreate bool
	indexes    map[string]string // symbol -> 最近一次推来的指数价
	indexPush  int
	failIndex  string // 非空时POST /index-price返回这个errCode
	// 撤单什么时候生效：""=立刻；"async"=撤单请求返回后，下一次查当前委托时才真正摘掉(模拟经Kafka的异步撤单)；
	// "never"=撤单请求成功返回但委托一直不摘
	cancelMode string
	pending    []string
	crossings  int // 挂单时会跟对面还挂着的系统委托交叉的次数：任何时候都必须是0
	// 某个合约的订单簿两侧都有单之后，又出现了一侧是空的次数(每处理完一个请求检查一次)。行情正常移动时必须是0
	emptyGaps int
	wasBoth   map[string]bool

	klineReqs []klineReq // POST /kline/sync收到的请求
	klineFail string     // 非空时/kline/sync返回这个errCode
}

type klineReq struct {
	symbol, interval string
	n                int
	first, last      map[string]any
}

func newFakeBackend(t *testing.T, symbols ...string) *fakeBackend {
	b := &fakeBackend{t: t, orders: map[string]*fakeOrder{}, nextID: 1000, avail: map[string]string{}, contracts: map[string][2]string{}, indexes: map[string]string{}}
	for _, s := range symbols {
		b.contracts[s] = [2]string{"3", "0.001"}
	}
	b.srv = httptest.NewServer(http.HandlerFunc(b.handle))
	t.Cleanup(b.srv.Close)
	return b
}

func (b *fakeBackend) reply(w http.ResponseWriter, data any) {
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 200, "message": "success", "data": data})
}

func (b *fakeBackend) fail(w http.ResponseWriter, errCode string) {
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 400, "errCode": errCode, "message": errCode})
}

// 每处理完一个请求，检查各合约的订单簿是不是从"两侧都有"变成了"有一侧是空的"
func (b *fakeBackend) checkSides() {
	sides := map[string][2]bool{}
	for _, o := range b.orders {
		s := sides[o.symbol]
		if o.side == "long" {
			s[0] = true
		} else {
			s[1] = true
		}
		sides[o.symbol] = s
	}
	if b.wasBoth == nil {
		b.wasBoth = map[string]bool{}
	}
	for sym := range b.contracts {
		both := sides[sym][0] && sides[sym][1]
		if b.wasBoth[sym] && !both {
			b.emptyGaps++
		}
		b.wasBoth[sym] = both
	}
}

func (b *fakeBackend) handle(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	b.mu.Lock()
	defer b.mu.Unlock()
	defer b.checkSides()
	want := api.SignRequest(testSecret, r.Header.Get("X-Timestamp"), r.Header.Get("X-Nonce"), r.Method, r.URL.EscapedPath(), r.URL.RawQuery, raw)
	if r.Header.Get("X-Api-Key") != "booksync" || r.Header.Get("X-Signature") != want {
		b.badSig++
		b.fail(w, "auth_invalid_signature")
		return
	}
	var body map[string]any
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &body)
	}
	uid := func() string {
		if v, ok := body["uid"]; ok {
			return fmt.Sprint(int64(v.(float64)))
		}
		return r.URL.Query().Get("uid")
	}
	switch {
	case r.URL.Path == "/kline/sync":
		if b.klineFail != "" {
			b.fail(w, b.klineFail)
			return
		}
		cs, _ := body["candles"].([]any)
		req := klineReq{symbol: fmt.Sprint(body["symbol"]), interval: fmt.Sprint(body["interval"]), n: len(cs)}
		if len(cs) > 0 {
			req.first, _ = cs[0].(map[string]any)
			req.last, _ = cs[len(cs)-1].(map[string]any)
		}
		b.klineReqs = append(b.klineReqs, req)
		b.reply(w, map[string]any{"written": len(cs)})
	case r.URL.Path == "/index-price":
		b.indexPush++
		if b.failIndex != "" {
			b.fail(w, b.failIndex)
			return
		}
		b.indexes[fmt.Sprint(body["symbol"])] = fmt.Sprint(body["price"])
		b.reply(w, nil)
	case r.URL.Path == "/account/create":
		if b.failCreate {
			b.fail(w, "internal_error")
			return
		}
		b.reply(w, nil)
	case r.URL.Path == "/account/info":
		a, ok := b.avail[uid()]
		if !ok {
			a = "0"
		}
		b.reply(w, map[string]any{"available": a})
	case r.URL.Path == "/account/balance":
		b.avail[uid()] = fmt.Sprint(body["amount"])
		b.topups = append(b.topups, uid())
		b.reply(w, nil)
	case r.URL.Path == "/contract/detail":
		c, ok := b.contracts[r.URL.Query().Get("symbol")]
		if !ok {
			b.fail(w, "symbol_not_found")
			return
		}
		scale, _ := strconv.Atoi(c[0])
		b.reply(w, map[string]any{"baseCoinScale": scale, "minVolume": c[1]})
	case r.URL.Path == "/order/current":
		for _, id := range b.pending {
			delete(b.orders, id)
		}
		b.pending = nil
		var out []map[string]any
		for _, o := range b.orders {
			if o.uid == uid() && o.symbol == r.URL.Query().Get("symbol") {
				out = append(out, map[string]any{"orderId": o.id, "side": o.side, "action": "open", "type": "limit",
					"price": o.price, "amount": o.amount, "tradedAmount": o.traded})
			}
		}
		b.reply(w, out)
	case r.URL.Path == "/order/add":
		if id, _ := body["requestId"].(string); !requestIDRe.MatchString(id) {
			b.t.Errorf("requestId格式不合法: %q", id)
		}
		newPrice, _ := decimal.NewFromString(fmt.Sprint(body["price"]))
		for _, o := range b.orders {
			if o.symbol != fmt.Sprint(body["symbol"]) || o.side == fmt.Sprint(body["side"]) {
				continue
			}
			p, _ := decimal.NewFromString(o.price)
			if (body["side"] == "long" && !p.GreaterThan(newPrice)) || (body["side"] == "short" && !p.LessThan(newPrice)) {
				b.crossings++
			}
		}
		b.nextID++
		id := strconv.Itoa(b.nextID)
		b.orders[id] = &fakeOrder{id: id, uid: uid(), symbol: fmt.Sprint(body["symbol"]), side: fmt.Sprint(body["side"]),
			price: fmt.Sprint(body["price"]), amount: fmt.Sprint(body["amount"]), traded: "0", leverage: int(body["leverage"].(float64))}
		b.adds++
		b.reply(w, map[string]any{"orderId": id})
	case strings.HasPrefix(r.URL.Path, "/order/cancel/"):
		id := strings.TrimPrefix(r.URL.Path, "/order/cancel/")
		o, ok := b.orders[id]
		if !ok || o.uid != uid() {
			b.fail(w, "order_not_cancelable")
			return
		}
		switch b.cancelMode {
		case "async":
			b.pending = append(b.pending, id)
		case "never":
		default:
			delete(b.orders, id)
		}
		b.cancels++
		b.reply(w, nil)
	default:
		b.t.Errorf("没有预期的请求: %s %s", r.Method, r.URL)
		b.fail(w, "invalid_param")
	}
}

// 让引擎把已经发出的撤单都处理完(异步撤单模式下，同步器最后发出的撤单在函数返回时还没生效)
func (b *fakeBackend) applyPending() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, id := range b.pending {
		delete(b.orders, id)
	}
	b.pending = nil
}

// 这个账户在这个合约上挂着的单子，"价格:数量"按价格排序
func (b *fakeBackend) book(uid uint64, symbol string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for _, o := range b.orders {
		if o.uid == strconv.FormatUint(uid, 10) && o.symbol == symbol {
			out = append(out, o.price+":"+o.amount)
		}
	}
	sort.Strings(out)
	return out
}

func (b *fakeBackend) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.orders)
}

func (b *fakeBackend) counters() (adds, cancels int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.adds, b.cancels
}

func (b *fakeBackend) seed(uid, symbol, side, price, amount string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextID++
	id := strconv.Itoa(b.nextID)
	b.orders[id] = &fakeOrder{id: id, uid: uid, symbol: symbol, side: side, price: price, amount: amount, traded: "0"}
}

// 模拟这个价位的单子被用户吃掉了一部分
func (b *fakeBackend) eat(uid, price, traded string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, o := range b.orders {
		if o.uid == uid && o.price == price {
			o.traded = traded
			return
		}
	}
	b.t.Fatalf("没找到要被吃的单子 uid=%s price=%s", uid, price)
}

// ---- 假的币安 ----

type fakeBinance struct {
	srv *httptest.Server
	mu  sync.Mutex
	// symbol -> {bids, asks}，每档是["价格","数量"]；status非0时所有请求返回这个状态码
	books     map[string][2][][2]string
	index     map[string]string // symbol -> 指数价，没有的symbol指数价接口返回400
	status    int               // 非0时所有请求返回这个状态码
	depthDown bool              // 只有深度接口返回500，指数价接口正常
	klineDown map[string]bool   // 这些周期的K线接口返回500
}

func newFakeBinance(t *testing.T) *fakeBinance {
	f := &fakeBinance{books: map[string][2][][2]string{}, index: map[string]string{}, klineDown: map[string]bool{}}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.status != 0 {
			w.WriteHeader(f.status)
			return
		}
		if r.URL.Path == "/fapi/v1/klines" {
			iv := r.URL.Query().Get("interval")
			if f.klineDown[iv] {
				w.WriteHeader(500)
				return
			}
			limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			// 生成limit根升序的K线：第i根 开=100+i 高=开+2 低=开-1 收=开+1 成交量1.5 成交笔数10+i，开盘时间从固定基准往后每根一个周期
			step := map[string]int64{"1m": 60_000, "5m": 300_000, "15m": 900_000, "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}[iv]
			rows := make([][]any, limit)
			for i := range rows {
				open := 100 + i
				rows[i] = []any{int64(1_700_000_000_000)/step*step + int64(i)*step, strconv.Itoa(open), strconv.Itoa(open + 2), strconv.Itoa(open - 1), strconv.Itoa(open + 1), "1.5", 0, "0", 10 + i}
			}
			_ = json.NewEncoder(w).Encode(rows)
			return
		}
		if r.URL.Path == "/fapi/v1/premiumIndex" {
			idx, ok := f.index[r.URL.Query().Get("symbol")]
			if !ok {
				w.WriteHeader(400)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"symbol": r.URL.Query().Get("symbol"), "indexPrice": idx})
			return
		}
		if f.depthDown {
			w.WriteHeader(500)
			return
		}
		bk, ok := f.books[r.URL.Query().Get("symbol")]
		if !ok {
			w.WriteHeader(400)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"bids": bk[0], "asks": bk[1]})
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeBinance) set(symbol string, bids, asks [][2]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.books[symbol] = [2][][2]string{bids, asks}
}

func (f *fakeBinance) setIndex(symbol, price string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.index[symbol] = price
}

func (f *fakeBinance) setDepthDown(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.depthDown = v
}

func (f *fakeBinance) setStatus(code int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status = code
}

// ---- 手动拨的时钟 ----

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type env struct {
	s     *Syncer
	be    *fakeBackend
	bn    *fakeBinance
	clock *fakeClock
}

const (
	bidUID = uint64(9000000)
	askUID = uint64(9000001)
)

// 阈值：每侧2档、币安数据10秒没更新算过期
func newEnv(t *testing.T, symbols ...string) *env {
	t.Helper()
	if len(symbols) == 0 {
		symbols = []string{"BTCUSDT"}
	}
	be := newFakeBackend(t, symbols...)
	bn := newFakeBinance(t)
	cfg := Config{Symbols: symbols, BaseUID: bidUID, Levels: 2, Interval: time.Second, Leverage: 5, Balance: "1000000", StaleAfter: 10 * time.Second}
	s, err := New(cfg, be.srv.URL, "booksync", testSecret, &Binance{BaseURL: bn.srv.URL, Client: bn.srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s.now = clock.now
	s.startedAt = clock.now()
	s.cancelWait, s.pollEvery = 200*time.Millisecond, 5*time.Millisecond
	// 所有测试共用的不变量：两个系统账户的挂单任何时候都不能交叉(交叉就会在两个系统账户之间真实成交，把最新成交价拖回过期的价格)
	t.Cleanup(func() {
		be.mu.Lock()
		defer be.mu.Unlock()
		if be.crossings != 0 {
			t.Errorf("挂单时跟对面还挂着的系统委托交叉了%d次", be.crossings)
		}
	})
	return &env{s: s, be: be, bn: bn, clock: clock}
}

func (e *env) step() { e.s.Step(context.Background()) }

// 两侧都有单之后，任何时刻都不能有一侧变空(用户的市价单一笔都吃不到)
func noEmptySide(t *testing.T, e *env) {
	t.Helper()
	e.be.mu.Lock()
	defer e.be.mu.Unlock()
	if e.be.emptyGaps != 0 {
		t.Fatalf("订单簿出现了%d次一侧为空", e.be.emptyGaps)
	}
}

func eqBook(t *testing.T, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("订单簿 = %v, want %v", got, want)
	}
}

var (
	bids0 = [][2]string{{"100.5", "1.2345"}, {"100.4", "2"}, {"100.3", "9"}}
	asks0 = [][2]string{{"100.6", "0.4"}, {"100.7", "3"}, {"100.8", "9"}}
)

// 第一轮：建两个系统账户并充值，币安盘口的前2档价格原样、数量按合约精度(3位)向下取整，
// 买盘挂在买盘账户(开多)、卖盘挂在卖盘账户(开空)，杠杆用配置的，请求签名都对得上
func TestSync_FirstCycleMirrorsBinanceExactly(t *testing.T) {
	e := newEnv(t)
	e.bn.set("BTCUSDT", bids0, asks0)
	e.step()

	eqBook(t, e.be.book(bidUID, "BTCUSDT"), "100.4:2", "100.5:1.234")
	eqBook(t, e.be.book(askUID, "BTCUSDT"), "100.6:0.4", "100.7:3")
	e.be.mu.Lock()
	defer e.be.mu.Unlock()
	if len(e.be.topups) != 2 || e.be.avail["9000000"] != "1000000" || e.be.avail["9000001"] != "1000000" {
		t.Fatalf("两个系统账户都要充值到配置的余额: topups=%v avail=%v", e.be.topups, e.be.avail)
	}
	for _, o := range e.be.orders {
		wantSide := map[string]string{"9000000": "long", "9000001": "short"}[o.uid]
		if o.side != wantSide || o.leverage != 5 {
			t.Fatalf("订单方向/杠杆不对: %+v", o)
		}
	}
	if e.be.badSig != 0 {
		t.Fatalf("有%d个请求签名对不上", e.be.badSig)
	}
}

// 余额已经够(不低于配置的一半)就不再充值
func TestSync_DoesNotTopUpWhenBalanceIsEnough(t *testing.T) {
	e := newEnv(t)
	e.be.avail["9000000"] = "600000" // 够一半
	e.be.avail["9000001"] = "499999" // 不到一半
	e.bn.set("BTCUSDT", bids0, asks0)
	e.step()
	e.be.mu.Lock()
	defer e.be.mu.Unlock()
	if len(e.be.topups) != 1 || e.be.topups[0] != "9000001" {
		t.Fatalf("只有余额不到一半的账户要充值: %v", e.be.topups)
	}
}

// 盘口没变：第二轮什么都不动，不能天天撤了重挂(会丢排队位置)
func TestSync_UnchangedBookDoesNothing(t *testing.T) {
	e := newEnv(t)
	e.bn.set("BTCUSDT", bids0, asks0)
	e.step()
	adds, cancels := e.be.counters()
	for i := 0; i < 3; i++ {
		e.clock.advance(time.Second)
		e.step()
	}
	if a, c := e.be.counters(); a != adds || c != cancels {
		t.Fatalf("盘口没变不该有任何撤挂: adds %d->%d cancels %d->%d", adds, a, cancels, c)
	}
}

// 盘口变了：掉出前2档的撤掉、新进来的补上；数量小幅波动不动；被用户吃掉大半的撤了重挂
func TestSync_FollowsBinanceWhenBookChanges(t *testing.T) {
	e := newEnv(t)
	e.bn.set("BTCUSDT", bids0, asks0)
	e.step()

	// 买盘：100.4档没了，100.3档补进前2档；卖盘：100.6档数量从0.4变成0.1(小幅波动)
	e.bn.set("BTCUSDT",
		[][2]string{{"100.5", "1.2345"}, {"100.3", "9"}},
		[][2]string{{"100.6", "0.1"}, {"100.7", "3"}})
	e.clock.advance(time.Second)
	e.step()
	eqBook(t, e.be.book(bidUID, "BTCUSDT"), "100.3:9", "100.5:1.234")
	eqBook(t, e.be.book(askUID, "BTCUSDT"), "100.6:0.4", "100.7:3") // 币安的数量变了，我们不动

	// 卖盘100.7档下单3，被吃掉2.9，剩0.1不到一半：撤了重挂，补回币安的当前数量
	e.be.eat("9000001", "100.7", "2.9")
	e.clock.advance(time.Second)
	e.step()
	eqBook(t, e.be.book(askUID, "BTCUSDT"), "100.6:0.4", "100.7:3")
	e.be.mu.Lock()
	defer e.be.mu.Unlock()
	for _, o := range e.be.orders {
		if o.price == "100.7" && o.traded != "0" {
			t.Fatal("被吃掉大半的那笔应该已经被撤掉、换成新单")
		}
	}
}

// 行情上行：新买单不能碰到还挂着的旧卖单(否则两个系统账户之间会真实成交，把最新成交价拖回过期的价格)，
// 但也不能干等到下一轮：行情单边移动时这一侧会有整整一轮是空的，用户的市价单一笔都吃不到。
// 所以旧卖单撤掉之后，在同一轮里就把被挡下的买单补上
func TestSync_BidsAreRefilledInTheSameCycleWhenMarketMovesUp(t *testing.T) {
	for _, mode := range []string{"", "async"} {
		t.Run("撤单生效方式="+mode, func(t *testing.T) {
			e := newEnv(t)
			e.be.cancelMode = mode
			e.s.cfg.Levels = 1
			e.bn.set("BTCUSDT", [][2]string{{"100", "1"}}, [][2]string{{"101", "1"}})
			e.step()
			eqBook(t, e.be.book(bidUID, "BTCUSDT"), "100:1")
			eqBook(t, e.be.book(askUID, "BTCUSDT"), "101:1")

			// 币安上行：买一101.5、卖一102。新买单101.5不低于还没撤掉的旧卖单101，要等它撤掉才能挂
			e.bn.set("BTCUSDT", [][2]string{{"101.5", "1"}}, [][2]string{{"102", "1"}})
			e.clock.advance(time.Second)
			e.step()
			e.be.applyPending()
			eqBook(t, e.be.book(bidUID, "BTCUSDT"), "101.5:1")
			eqBook(t, e.be.book(askUID, "BTCUSDT"), "102:1")
			noEmptySide(t, e)
		})
	}
}

// 行情下行是对称的：新卖单不能碰到还挂着的旧买单，撤掉之后同一轮补上
func TestSync_AsksAreRefilledInTheSameCycleWhenMarketMovesDown(t *testing.T) {
	for _, mode := range []string{"", "async"} {
		t.Run("撤单生效方式="+mode, func(t *testing.T) {
			e := newEnv(t)
			e.be.cancelMode = mode
			e.s.cfg.Levels = 1
			e.bn.set("BTCUSDT", [][2]string{{"100", "1"}}, [][2]string{{"101", "1"}})
			e.step()

			// 币安下行：买一98、卖一99。新卖单99不高于还没撤掉的旧买单100，要等它撤掉才能挂
			e.bn.set("BTCUSDT", [][2]string{{"98", "1"}}, [][2]string{{"99", "1"}})
			e.clock.advance(time.Second)
			e.step()
			e.be.applyPending()
			eqBook(t, e.be.book(bidUID, "BTCUSDT"), "98:1")
			eqBook(t, e.be.book(askUID, "BTCUSDT"), "99:1")
			noEmptySide(t, e)
		})
	}
}

// 撤单一直不生效(引擎慢、Kafka积压)：等到cancelWait就放弃，被挡下的档位不挂(宁可这一轮少一侧，也不能交叉)，
// 撤单生效之后的下一轮补上
func TestSync_HeldOrdersWaitForNextCycleWhenCancelNeverLands(t *testing.T) {
	e := newEnv(t)
	e.s.cfg.Levels = 1
	e.bn.set("BTCUSDT", [][2]string{{"100", "1"}}, [][2]string{{"101", "1"}})
	e.step()

	e.be.cancelMode = "never"
	e.bn.set("BTCUSDT", [][2]string{{"101.5", "1"}}, [][2]string{{"102", "1"}})
	e.clock.advance(time.Second)
	started := time.Now()
	e.step()
	if time.Since(started) < e.s.cancelWait {
		t.Fatalf("应该等满cancelWait(%s)才放弃", e.s.cancelWait)
	}
	if got := e.be.book(bidUID, "BTCUSDT"); strings.Contains(strings.Join(got, " "), "101.5") {
		t.Fatalf("旧卖单还没撤掉，新买单101.5不能挂: %v", got)
	}

	e.be.mu.Lock()
	e.be.cancelMode = ""
	e.be.mu.Unlock()
	e.clock.advance(time.Second)
	e.step()
	eqBook(t, e.be.book(bidUID, "BTCUSDT"), "101.5:1")
	eqBook(t, e.be.book(askUID, "BTCUSDT"), "102:1")
	noEmptySide(t, e)
}

// 币安拉不到：StaleAfter(10秒)以内不动已经挂着的单子(可能只是一次抖动)，超过就撤光、暂停报价，恢复后重新挂
func TestSync_StaleSourceCancelsQuotesAndResumes(t *testing.T) {
	e := newEnv(t)
	e.bn.set("BTCUSDT", bids0, asks0)
	e.step()
	if e.be.count() != 4 {
		t.Fatalf("应该挂了4笔, got %d", e.be.count())
	}

	e.bn.setStatus(500)
	e.clock.advance(5 * time.Second)
	e.step()
	if e.be.count() != 4 {
		t.Fatal("5秒还没过期，不能动挂单")
	}
	e.clock.advance(5 * time.Second) // 距离上次成功恰好10秒：还没超过
	e.step()
	if e.be.count() != 4 {
		t.Fatal("恰好10秒不算超过，不能动挂单")
	}
	e.clock.advance(time.Millisecond)
	e.step()
	if e.be.count() != 0 {
		t.Fatalf("超过10秒应该撤光, 还剩%d笔", e.be.count())
	}
	_, cancels := e.be.counters()
	e.clock.advance(time.Second)
	e.step()
	if _, c := e.be.counters(); c != cancels {
		t.Fatal("已经撤光了，不该重复撤")
	}

	e.bn.setStatus(0)
	e.clock.advance(time.Second)
	e.step()
	eqBook(t, e.be.book(bidUID, "BTCUSDT"), "100.4:2", "100.5:1.234")
	eqBook(t, e.be.book(askUID, "BTCUSDT"), "100.6:0.4", "100.7:3")
}

// 进程重启时币安一直拿不到：上一个进程留下的挂单也要撤掉(过期时间从进程启动算起)，而且不会挂新单
func TestSync_LeftoverQuotesAreCancelledWhenSourceNeverComesUp(t *testing.T) {
	cases := map[string]func(e *env){
		"币安被地区屏蔽(451)": func(e *env) { e.bn.setStatus(451) },
		"盘口是交叉的":       func(e *env) { e.bn.set("BTCUSDT", [][2]string{{"101", "1"}}, [][2]string{{"100", "1"}}) },
		"盘口有一侧是空的":     func(e *env) { e.bn.set("BTCUSDT", nil, [][2]string{{"100", "1"}}) },
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			e.be.seed("9000000", "BTCUSDT", "long", "100", "1")
			e.be.seed("9000001", "BTCUSDT", "short", "101", "1")
			breakIt(e)
			e.step()
			if e.be.count() != 2 {
				t.Fatal("启动后10秒以内不动上个进程留下的单子")
			}
			e.clock.advance(11 * time.Second)
			e.step()
			if e.be.count() != 0 {
				t.Fatalf("过期后应该撤光, 还剩%d笔", e.be.count())
			}
			if adds, _ := e.be.counters(); adds != 0 {
				t.Fatalf("拿不到可信的盘口时不该挂新单, adds=%d", adds)
			}
		})
	}
}

// 币安的盘口有数据，但按我们合约的数量精度/最小下单量取整后一侧挂不出来：不能只挂一侧
func TestSync_DoesNotQuoteOneSideOnly(t *testing.T) {
	e := newEnv(t)
	e.be.contracts["BTCUSDT"] = [2]string{"3", "1"} // 最小下单量1
	e.bn.set("BTCUSDT", [][2]string{{"100", "5"}}, [][2]string{{"101", "0.5"}})
	e.step()
	if e.be.count() != 0 {
		t.Fatalf("卖盘挂不出来时买盘也不能挂, 挂了%d笔", e.be.count())
	}
}

// 多个合约互相独立：一个合约的币安数据过期撤单，不影响另一个继续报价
func TestSync_SymbolsAreIndependent(t *testing.T) {
	e := newEnv(t, "BTCUSDT", "ETHUSDT")
	e.bn.set("BTCUSDT", bids0, asks0)
	e.bn.set("ETHUSDT", [][2]string{{"50.5", "3"}, {"50.4", "3"}}, [][2]string{{"50.6", "3"}, {"50.7", "3"}})
	e.step()
	if e.be.count() != 8 {
		t.Fatalf("两个合约各4笔, got %d", e.be.count())
	}

	// ETH从币安消失(假币安对没有的symbol返回400)，BTC照常
	e.bn.mu.Lock()
	delete(e.bn.books, "ETHUSDT")
	e.bn.mu.Unlock()
	e.clock.advance(11 * time.Second)
	e.step()
	eqBook(t, e.be.book(bidUID, "ETHUSDT"))
	eqBook(t, e.be.book(askUID, "ETHUSDT"))
	eqBook(t, e.be.book(bidUID, "BTCUSDT"), "100.4:2", "100.5:1.234")
}

// 准备系统账户失败(比如后端还没起来)：什么都不挂，下一轮重试，成功后正常同步
func TestSync_RetriesPreparationOnNextStep(t *testing.T) {
	e := newEnv(t)
	e.bn.set("BTCUSDT", bids0, asks0)
	e.be.failCreate = true
	e.step()
	if e.be.count() != 0 {
		t.Fatal("账户没准备好不能挂单")
	}
	e.be.mu.Lock()
	e.be.failCreate = false
	e.be.mu.Unlock()
	e.step()
	if e.be.count() != 4 {
		t.Fatalf("重试成功后应该挂好, got %d", e.be.count())
	}
}

func (e *env) indexOf(symbol string) (string, int) {
	e.be.mu.Lock()
	defer e.be.mu.Unlock()
	return e.be.indexes[symbol], e.be.indexPush
}

// 每一轮把币安的指数价原样推给contract-api(标记价靠它做锚)，币安的指数价变了就跟着变
func TestSync_PushesBinanceIndexPriceEveryCycle(t *testing.T) {
	e := newEnv(t)
	e.bn.set("BTCUSDT", bids0, asks0)
	e.bn.setIndex("BTCUSDT", "100.55")
	e.step()
	if p, n := e.indexOf("BTCUSDT"); p != "100.55" || n != 1 {
		t.Fatalf("指数价 = %q 推送%d次, want 100.55 1次", p, n)
	}
	e.bn.setIndex("BTCUSDT", "101.2")
	e.clock.advance(time.Second)
	e.step()
	if p, n := e.indexOf("BTCUSDT"); p != "101.2" || n != 2 {
		t.Fatalf("指数价 = %q 推送%d次, want 101.2 2次(每一轮都推，不然30秒没更新标记价就冻结)", p, n)
	}
}

// 指数价和盘口互相独立：指数价拿不到、或者被服务端拒绝，不能停掉挂单
func TestSync_IndexFailureDoesNotStopQuoting(t *testing.T) {
	t.Run("币安指数价接口拿不到", func(t *testing.T) {
		e := newEnv(t)
		e.bn.set("BTCUSDT", bids0, asks0) // 没设指数价：假币安对指数价接口返回400
		e.step()
		if e.be.count() != 4 {
			t.Fatalf("盘口应该照常同步, got %d笔", e.be.count())
		}
		if _, n := e.indexOf("BTCUSDT"); n != 0 {
			t.Fatal("币安指数价拿不到就不推")
		}
	})
	t.Run("服务端的跳变保护拦下了这次推送", func(t *testing.T) {
		e := newEnv(t)
		e.bn.set("BTCUSDT", bids0, asks0)
		e.bn.setIndex("BTCUSDT", "100.55")
		e.be.failIndex = "index_price_jump"
		e.step()
		if e.be.count() != 4 {
			t.Fatalf("盘口应该照常同步, got %d笔", e.be.count())
		}
		e.clock.advance(time.Second)
		e.step() // 下一轮继续推
		if p, _ := e.indexOf("BTCUSDT"); p != "" {
			t.Fatalf("被拒绝的推送不会记录价格, got %q", p)
		}
		e.be.mu.Lock()
		pushes := e.be.indexPush
		e.be.mu.Unlock()
		if pushes != 2 {
			t.Fatalf("被拒绝后每一轮都要继续推(满了确认时间服务端才承认), 推了%d次", pushes)
		}
	})
}

// 盘口拉不到(币安深度接口挂了)时，指数价接口还通就继续推：两条线互不牵连
func TestSync_IndexKeepsFlowingWhileBookIsDown(t *testing.T) {
	e := newEnv(t)
	e.bn.set("BTCUSDT", bids0, asks0)
	e.bn.setIndex("BTCUSDT", "100.55")
	e.step()

	e.bn.setDepthDown(true)
	e.bn.setIndex("BTCUSDT", "100.9")
	e.clock.advance(11 * time.Second)
	e.step()
	if e.be.count() != 0 {
		t.Fatalf("盘口过期应该撤光, 还剩%d笔", e.be.count())
	}
	if p, _ := e.indexOf("BTCUSDT"); p != "100.9" {
		t.Fatalf("盘口挂了指数价也要继续推, got %q", p)
	}
}

// ---- K线同步 ----

func (e *env) klineReqs() []klineReq {
	e.be.mu.Lock()
	defer e.be.mu.Unlock()
	return append([]klineReq(nil), e.be.klineReqs...)
}

func (e *env) clearKlineReqs() {
	e.be.mu.Lock()
	defer e.be.mu.Unlock()
	e.be.klineReqs = nil
}

func klineEnv(t *testing.T, backfill int) *env {
	t.Helper()
	e := newEnv(t)
	e.s.cfg.KlineEvery, e.s.cfg.KlineBackfill = 5*time.Second, backfill
	return e
}

// 每个周期一共推了多少根(一个周期可能分好几批)
func requestCounts(reqs []klineReq) map[string]int {
	m := map[string]int{}
	for _, r := range reqs {
		m[r.interval] += r.n
	}
	return m
}

// 第一轮：六个周期各补KlineBackfill根，K线的开盘时间、开高低收、成交量、成交笔数原样带过去
func TestSyncKlines_FirstRoundBackfillsEveryInterval(t *testing.T) {
	e := klineEnv(t, 5)
	e.s.SyncKlines(context.Background())
	reqs := e.klineReqs()
	if len(reqs) != 6 {
		t.Fatalf("应该六个周期各一个请求, got %d", len(reqs))
	}
	got := requestCounts(reqs)
	for _, iv := range []string{"1m", "5m", "15m", "1h", "4h", "1d"} {
		if got[iv] != 5 {
			t.Fatalf("%s应该补5根, got %v", iv, got)
		}
	}
	var one klineReq
	for _, r := range reqs {
		if r.interval == "1m" {
			one = r
		}
	}
	// 1m第0根: 开=100 高=102 低=99 收=101 成交量1.5 成交笔数10；最后一根(i=4): 开=104
	if one.symbol != "BTCUSDT" || one.first["open"] != "100" || one.first["high"] != "102" || one.first["low"] != "99" ||
		one.first["close"] != "101" || one.first["volume"] != "1.5" || one.first["tradeCount"] != float64(10) ||
		one.first["openTime"] != float64(1_700_000_000_000/60_000*60_000) || one.last["open"] != "104" || one.last["tradeCount"] != float64(14) {
		t.Fatalf("K线内容不对: %+v", one)
	}
}

// 补几百根历史时分批推：每批不超过klineChunk(200)根，合起来是完整的、按开盘时间从旧到新的一批。
// 一次请求的体积有上限(64KB)，一次推500根会被拒绝(真实系统里15m以上的周期就是这样被拒的)
func TestSyncKlines_BackfillIsSentInChunks(t *testing.T) {
	e := klineEnv(t, 450)
	e.s.SyncKlines(context.Background())
	perInterval := map[string][]klineReq{}
	for _, r := range e.klineReqs() {
		perInterval[r.interval] = append(perInterval[r.interval], r)
	}
	for _, iv := range []string{"1m", "5m", "15m", "1h", "4h", "1d"} {
		reqs := perInterval[iv]
		if len(reqs) != 3 || reqs[0].n != 200 || reqs[1].n != 200 || reqs[2].n != 50 {
			t.Fatalf("%s应该分成200+200+50三批, got %+v", iv, reqs)
		}
		if reqs[0].first["open"] != "100" || reqs[2].last["open"] != "549" {
			t.Fatalf("%s分批后应该是完整连续的450根: 第一批第一根open=%v 最后一批最后一根open=%v", iv, reqs[0].first["open"], reqs[2].last["open"])
		}
		for _, r := range reqs {
			if r.n > klineChunk {
				t.Fatalf("一批不能超过%d根", klineChunk)
			}
		}
	}
}

// 分批推的中途失败：这个周期不算已同步，下一轮还补完整的历史
func TestSyncKlines_ChunkFailureKeepsBackfill(t *testing.T) {
	e := klineEnv(t, 450)
	e.be.klineFail = "internal_error"
	e.s.SyncKlines(context.Background())
	e.be.mu.Lock()
	e.be.klineFail = ""
	e.be.mu.Unlock()
	e.clock.advance(2 * time.Second)
	e.s.SyncKlines(context.Background())
	if got := requestCounts(e.klineReqs()); got["1m"] != 450 || got["1d"] != 450 {
		t.Fatalf("上一次没成功，这一次要补完整的450根: %v", got)
	}
}

// 之后每次只同步最近几根(至少3根)，不再补历史
func TestSyncKlines_LaterRoundsOnlySendTheRecentOnes(t *testing.T) {
	e := klineEnv(t, 50)
	e.s.SyncKlines(context.Background())
	e.clearKlineReqs()
	e.clock.advance(5 * time.Second)
	e.s.SyncKlines(context.Background())
	got := requestCounts(e.klineReqs())
	for _, iv := range []string{"1m", "5m", "15m", "1h", "4h", "1d"} {
		if got[iv] != 3 {
			t.Fatalf("%s第二轮只该同步3根, got %v", iv, got)
		}
	}
}

// 中间断了一阵(币安或我们的api不可用)，恢复后按错过的根数补上，不用重启：
// 距离上次成功10分钟，1m错过10根+3=13，5m错过2根+3=5，15m和更大的周期最少3根
func TestSyncKlines_CatchesUpAfterAGap(t *testing.T) {
	e := klineEnv(t, 20)
	e.s.SyncKlines(context.Background())
	e.clearKlineReqs()
	e.clock.advance(10 * time.Minute)
	e.s.SyncKlines(context.Background())
	got := requestCounts(e.klineReqs())
	if got["1m"] != 13 || got["5m"] != 5 || got["15m"] != 3 || got["1h"] != 3 {
		t.Fatalf("补的根数不对: %v", got)
	}
	// 断了很久：不超过KlineBackfill
	e.clearKlineReqs()
	e.clock.advance(48 * time.Hour)
	e.s.SyncKlines(context.Background())
	if got := requestCounts(e.klineReqs()); got["1m"] != 20 {
		t.Fatalf("最多补KlineBackfill根: %v", got)
	}
}

// contract-api拒绝(K线来源不是外部行情)：不能当成已经同步过，修好之后要补的还是完整的历史
func TestSyncKlines_RejectedByAPIKeepsBackfill(t *testing.T) {
	e := klineEnv(t, 5)
	e.be.klineFail = "kline_source_not_external"
	e.s.SyncKlines(context.Background())
	if len(e.klineReqs()) != 0 {
		t.Fatal("被拒绝的请求不该被记录")
	}
	e.be.mu.Lock()
	e.be.klineFail = ""
	e.be.mu.Unlock()
	e.clock.advance(5 * time.Second)
	e.s.SyncKlines(context.Background())
	if got := requestCounts(e.klineReqs()); got["1m"] != 5 || got["1d"] != 5 {
		t.Fatalf("上一次没成功，这一次还要补完整的%d根: %v", 5, got)
	}
}

// 某一个周期从币安拉不到，不影响别的周期；它自己下一轮继续补历史
func TestSyncKlines_OneIntervalFailingDoesNotBlockTheOthers(t *testing.T) {
	e := klineEnv(t, 5)
	e.bn.mu.Lock()
	e.bn.klineDown["1h"] = true
	e.bn.mu.Unlock()
	e.s.SyncKlines(context.Background())
	got := requestCounts(e.klineReqs())
	if len(got) != 5 || got["1h"] != 0 || got["1m"] != 5 || got["1d"] != 5 {
		t.Fatalf("1h失败，其余五个照常: %v", got)
	}
	e.bn.mu.Lock()
	e.bn.klineDown["1h"] = false
	e.bn.mu.Unlock()
	e.clearKlineReqs()
	e.clock.advance(5 * time.Second)
	e.s.SyncKlines(context.Background())
	got = requestCounts(e.klineReqs())
	if got["1h"] != 5 || got["1m"] != 3 {
		t.Fatalf("1h恢复后补完整历史，别的只同步最近几根: %v", got)
	}
}

// 多个合约各自同步
func TestSyncKlines_EverySymbol(t *testing.T) {
	e := newEnv(t, "BTCUSDT", "ETHUSDT")
	e.s.cfg.KlineEvery, e.s.cfg.KlineBackfill = 5*time.Second, 2
	e.s.SyncKlines(context.Background())
	seen := map[string]int{}
	for _, r := range e.klineReqs() {
		seen[r.symbol]++
	}
	if seen["BTCUSDT"] != 6 || seen["ETHUSDT"] != 6 {
		t.Fatalf("两个合约各六个周期: %v", seen)
	}
}

// KlineEvery<=0不同步K线；开了的话Run会自己定时同步(第一轮立刻做)
func TestRun_KlineSyncIsOptionalAndRunsInBackground(t *testing.T) {
	for _, tc := range []struct {
		name  string
		every time.Duration
		want  bool
	}{{"关闭", 0, false}, {"开启", 50 * time.Millisecond, true}} {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.s.now = time.Now
			e.s.cfg.Interval = 10 * time.Millisecond
			e.s.cfg.KlineEvery, e.s.cfg.KlineBackfill = tc.every, 3
			e.bn.set("BTCUSDT", bids0, asks0)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { e.s.Run(ctx); close(done) }()
			time.Sleep(400 * time.Millisecond)
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("Run没有退出")
			}
			if got := len(e.klineReqs()) > 0; got != tc.want {
				t.Fatalf("同步了K线=%v, want %v", got, tc.want)
			}
		})
	}
}

// 退出时撤掉全部系统挂单，不留过期的报价
func TestRun_CancelsEverythingOnShutdown(t *testing.T) {
	e := newEnv(t)
	e.s.now = time.Now
	e.s.cfg.Interval = 10 * time.Millisecond
	e.bn.set("BTCUSDT", bids0, asks0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { e.s.Run(ctx); close(done) }()

	deadline := time.Now().Add(5 * time.Second)
	for e.be.count() != 4 {
		if time.Now().After(deadline) {
			t.Fatalf("5秒内没挂好, got %d", e.be.count())
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run没有在ctx取消后退出")
	}
	if e.be.count() != 0 {
		t.Fatalf("退出后应该撤光, 还剩%d笔", e.be.count())
	}
}

func TestNew_ValidatesConfig(t *testing.T) {
	ok := Config{Symbols: []string{"BTCUSDT"}, BaseUID: 9000000, Levels: 50, Interval: time.Second, Leverage: 5, Balance: "1000", StaleAfter: time.Second}
	bn := &Binance{BaseURL: "http://x", Client: http.DefaultClient}
	if _, err := New(ok, "http://x", "k", "s", bn); err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(c *Config){
		"没有合约":         func(c *Config) { c.Symbols = nil },
		"uid是0":        func(c *Config) { c.BaseUID = 0 },
		"档数是0":         func(c *Config) { c.Levels = 0 },
		"档数超过币安上限1000": func(c *Config) { c.Levels = 1001 },
		"间隔是0":         func(c *Config) { c.Interval = 0 },
		"过期时间是0":       func(c *Config) { c.StaleAfter = 0 },
		"杠杆是0":         func(c *Config) { c.Leverage = 0 },
		"余额不是数字":       func(c *Config) { c.Balance = "abc" },
		"余额是0":         func(c *Config) { c.Balance = "0" },
	}
	for name, mutate := range mutations {
		c := ok
		mutate(&c)
		if _, err := New(c, "http://x", "k", "s", bn); err == nil {
			t.Errorf("%s: 应该报错", name)
		}
	}
}
