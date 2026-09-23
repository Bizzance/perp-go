package booksync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"perp-go/internal/api"
	"perp-go/internal/binancefeed"
)

const testSecret = "s3cret-s3cret-s3cret"

var requestIDRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// ---- 假的contract-api：只接收指数价推送和K线同步(这个进程现在只做这两件事) ----

type fakeBackend struct {
	t   *testing.T
	srv *httptest.Server

	mu        sync.Mutex
	badSig    int
	indexes   map[string]string // symbol -> 最近一次推来的指数价
	indexPush int
	failIndex string // 非空时POST /index-price返回这个errCode

	klineReqs []klineReq // POST /kline/sync收到的请求
	klineFail string     // 非空时/kline/sync返回这个errCode
}

type klineReq struct {
	symbol, interval string
	n                int
	first, last      map[string]any
}

func newFakeBackend(t *testing.T) *fakeBackend {
	b := &fakeBackend{t: t, indexes: map[string]string{}}
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

func (b *fakeBackend) handle(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	b.mu.Lock()
	defer b.mu.Unlock()
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
	switch r.URL.Path {
	case "/kline/sync":
		if id, _ := body["requestId"].(string); id != "" && !requestIDRe.MatchString(id) {
			b.t.Errorf("requestId格式不合法: %q", id)
		}
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
	case "/index-price":
		b.indexPush++
		if b.failIndex != "" {
			b.fail(w, b.failIndex)
			return
		}
		b.indexes[fmt.Sprint(body["symbol"])] = fmt.Sprint(body["price"])
		b.reply(w, nil)
	default:
		b.t.Errorf("没有预期的请求: %s %s", r.Method, r.URL)
		b.fail(w, "invalid_param")
	}
}

// ---- 假的币安：指数价+K线(这个进程不再拉深度) ----

type fakeBinance struct {
	srv *httptest.Server
	mu  sync.Mutex
	// symbol -> 指数价，没有的symbol指数价接口返回400
	index     map[string]string
	status    int             // 非0时所有请求返回这个状态码
	klineDown map[string]bool // 这些周期的K线接口返回500
}

func newFakeBinance(t *testing.T) *fakeBinance {
	f := &fakeBinance{index: map[string]string{}, klineDown: map[string]bool{}}
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
		w.WriteHeader(400)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeBinance) setIndex(symbol, price string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.index[symbol] = price
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

func newEnv(t *testing.T, symbols ...string) *env {
	t.Helper()
	if len(symbols) == 0 {
		symbols = []string{"BTCUSDT"}
	}
	be := newFakeBackend(t)
	bn := newFakeBinance(t)
	cfg := Config{Symbols: symbols, Interval: time.Second}
	s, err := New(cfg, be.srv.URL, "booksync", testSecret, &binancefeed.Binance{BaseURL: bn.srv.URL, Client: bn.srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s.now = clock.now
	return &env{s: s, be: be, bn: bn, clock: clock}
}

func (e *env) step() { e.s.Step(context.Background()) }

func (e *env) indexOf(symbol string) (string, int) {
	e.be.mu.Lock()
	defer e.be.mu.Unlock()
	return e.be.indexes[symbol], e.be.indexPush
}

// 每一轮把币安的指数价原样推给contract-api(标记价靠它做锚)，币安的指数价变了就跟着变
func TestSync_PushesBinanceIndexPriceEveryCycle(t *testing.T) {
	e := newEnv(t)
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

// 指数价拿不到、或者被服务端拒绝：只记日志限流，下一轮继续推
func TestSync_IndexFailureIsLogged(t *testing.T) {
	t.Run("币安指数价接口拿不到", func(t *testing.T) {
		e := newEnv(t) // 没设指数价：假币安对指数价接口返回400
		e.step()
		if _, n := e.indexOf("BTCUSDT"); n != 0 {
			t.Fatal("币安指数价拿不到就不推")
		}
	})
	t.Run("服务端的跳变保护拦下了这次推送", func(t *testing.T) {
		e := newEnv(t)
		e.bn.setIndex("BTCUSDT", "100.55")
		e.be.failIndex = "index_price_jump"
		e.step()
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

// 多个合约互相独立：一个合约推指数价失败，不影响另一个
func TestSync_SymbolsAreIndependent(t *testing.T) {
	e := newEnv(t, "BTCUSDT", "ETHUSDT")
	e.bn.setIndex("BTCUSDT", "100.5")
	// 没设ETHUSDT的指数价：假币安对它返回400
	e.step()
	if p, _ := e.indexOf("BTCUSDT"); p != "100.5" {
		t.Fatalf("BTCUSDT指数价应该正常推送, got %q", p)
	}
	if p, _ := e.indexOf("ETHUSDT"); p != "" {
		t.Fatalf("ETHUSDT拿不到指数价不该推送, got %q", p)
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

// KlineEvery<=0不同步K线；开了的话Run会自己定时同步(第一轮立刻做)，指数价推送也在后台跑
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
			e.bn.setIndex("BTCUSDT", "100")
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
			if _, n := e.indexOf("BTCUSDT"); n == 0 {
				t.Fatal("指数价应该在后台持续推送")
			}
		})
	}
}

func TestNew_ValidatesConfig(t *testing.T) {
	ok := Config{Symbols: []string{"BTCUSDT"}, Interval: time.Second}
	bn := &binancefeed.Binance{BaseURL: "http://x", Client: http.DefaultClient}
	if _, err := New(ok, "http://x", "k", "s", bn); err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(c *Config){
		"没有合约":                func(c *Config) { c.Symbols = nil },
		"间隔是0":                func(c *Config) { c.Interval = 0 },
		"KlineBackfill超过币安上限": func(c *Config) { c.KlineBackfill = binancefeed.MaxKlineLimit + 1 },
		"KlineBackfill是负数":    func(c *Config) { c.KlineBackfill = -1 },
	}
	for name, mutate := range mutations {
		c := ok
		mutate(&c)
		if _, err := New(c, "http://x", "k", "s", bn); err == nil {
			t.Errorf("%s: 应该报错", name)
		}
	}
}
