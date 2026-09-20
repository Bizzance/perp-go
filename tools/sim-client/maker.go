package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shopspring/decimal"
)

// 系统做市：用币安的行情当价格源，系统账户把币安的盘口镜像成我们订单簿里的真实挂单，用户的单子跟它们
// 走真实的撮合和结算，成交的对手方就是系统。
//
// 四个系统账户(uid从baseUID起)：
//
//	+0 买盘做市(开多挂买单)   +1 卖盘做市(开空挂卖单)
//	+2 对敲买                 +3 对敲卖
//
// 对敲的作用：让最新成交价和K线跟着币安走。没有用户交易时最新成交价会停在很久以前，K线也是一条直线，
// 所以按币安的中间价定期让两个系统账户对敲一笔很小的成交。标记价不靠它：标记价由指数价、盘口基差、
// 最新成交价取中位数得出，做市每个周期都把币安的价格同步成指数价，标记价自己会跟上，见docs/mark-price.md。
// 对敲也拉不动标记价——这正是标记价要抗的攻击。
// 做市账户只开仓不平仓，仓位会随着用户交易累积(多空各一份、不轧差)，用余额兜着；累积多了用重置
// (结束本轮，按标记价平掉全部仓位)清掉。

type makerConfig struct {
	symbols    []string
	baseUID    uint64
	levels     int             // 每侧挂多少个价格档
	scale      decimal.Decimal // 币安数量 * scale = 我们挂的数量
	leverage   int
	interval   time.Duration
	depthLimit int
	printEvery time.Duration   // 对敲的最短间隔
	offsetStep decimal.Decimal // 行情情景的偏移每个周期最多推进多少个百分点
	balance    string          // 系统账户的余额，低于一半时自动补
}

func defaultMakerConfig() makerConfig {
	return makerConfig{
		symbols:    []string{"BTCUSDT", "ETHUSDT"},
		baseUID:    9000000,
		levels:     12,
		scale:      decimal.NewFromInt(1),
		leverage:   5, // 全部分档位里最低的最大杠杆是5，用5在哪个档位都不会被拒
		interval:   2 * time.Second,
		depthLimit: 500,
		printEvery: 5 * time.Second,
		offsetStep: decimal.NewFromInt(1),
		balance:    "1000000000",
	}
}

// 对敲要不要做：跟上次对敲价偏离超过这个比例，或者超过这么久没对敲过
const (
	printDriftRatio = 0.0001
	printMaxIdle    = 60 * time.Second
)

// 行情情景允许的最大偏移(百分点)
var maxOffsetPct = decimal.NewFromInt(30)

// ---- 调后端 ----

type envelope struct {
	Code    int             `json:"code"`
	ErrCode string          `json:"errCode"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

type apiClient struct {
	base   string
	signer signer
	http   *http.Client
}

func (a *apiClient) do(ctx context.Context, method, rawURL string, body any) (envelope, error) {
	var raw []byte
	if body != nil {
		raw, _ = json.Marshal(body)
	}
	u, err := url.Parse(a.base + rawURL)
	if err != nil {
		return envelope{}, err
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(raw))
	if err != nil {
		return envelope{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range a.signer.headers(method, u.EscapedPath(), u.RawQuery, raw) {
		req.Header[k] = v
	}
	resp, err := a.http.Do(req)
	if err != nil {
		return envelope{}, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	var env envelope
	if err := json.Unmarshal(b, &env); err != nil {
		return envelope{}, fmt.Errorf("响应不是JSON: %.200s", b)
	}
	return env, nil
}

// 成功返回data，业务失败(code != 200)当作error
func (a *apiClient) call(ctx context.Context, method, path string, body any) (json.RawMessage, error) {
	env, err := a.do(ctx, method, path, body)
	if err != nil {
		return nil, err
	}
	if env.Code != 200 {
		return nil, &apiError{code: env.ErrCode, msg: env.Message}
	}
	return env.Data, nil
}

type apiError struct{ code, msg string }

func (e *apiError) Error() string { return e.code + ": " + e.msg }

// ---- 状态 ----

type symbolStatus struct {
	Symbol      string `json:"symbol"`
	BinanceBid  string `json:"binanceBid"`
	BinanceAsk  string `json:"binanceAsk"`
	OurBestBid  string `json:"ourBestBid"`
	OurBestAsk  string `json:"ourBestAsk"`
	LiveBids    int    `json:"liveBids"`
	LiveAsks    int    `json:"liveAsks"`
	LastPrintAt int64  `json:"lastPrintAt"`
	LastPrint   string `json:"lastPrintPrice"`
	OffsetPct   string `json:"offsetPct"` // 我们的行情当前相对币安的偏移(百分点)
}

type makerStatus struct {
	Enabled   bool           `json:"enabled"`
	Symbols   []symbolStatus `json:"symbols"`
	Cycles    int            `json:"cycles"`
	TargetPct string         `json:"targetPct"` // 行情情景的目标偏移，实际偏移逐步推进过去
	LastError string         `json:"lastError"`
	ErrorAt   int64          `json:"errorAt"`
	UIDs      []uint64       `json:"uids"`
}

type contractInfo struct {
	qtyDP     int32
	minVolume decimal.Decimal
}

type maker struct {
	cfg makerConfig
	api *apiClient
	bn  *binanceClient

	lifecycle sync.Mutex // 串行化start/stop/reset：stop在锁外等循环退出，并发的start会起出第二个循环，两个循环读写同一批map
	// 只给测试用：stop摘掉cancel之后、真正取消旧循环之前调用，测试在这里发起start，验证不会同时跑两个循环
	afterDetach func()
	loopsActive atomic.Int32
	loopsMax    atomic.Int32 // 出现过的最大同时运行的循环数，正常永远是1
	mu          sync.Mutex
	cancel      context.CancelFunc
	done        chan struct{}
	status      makerStatus
	contracts   map[string]contractInfo
	steps       map[string]decimal.Decimal
	lastIndex   map[string]decimal.Decimal
	lastPrint   map[string]printRecord
	dirty       map[string]bool // 对敲之后要清理这一轮对敲账户上残留的挂单

	// 行情情景：在币安价格之上叠加一个偏移(百分点)，用来在页面上测试强平、盈亏。target是目标，
	// offCur是每个合约当前实际的偏移，每个周期最多往目标推进offsetStep
	target  decimal.Decimal
	offCur  map[string]decimal.Decimal
	offInit map[string]bool
}

type printRecord struct {
	at    time.Time
	price decimal.Decimal
}

func newMaker(cfg makerConfig, api *apiClient, bn *binanceClient) *maker {
	uids := make([]uint64, 4)
	for i := range uids {
		uids[i] = cfg.baseUID + uint64(i)
	}
	return &maker{cfg: cfg, api: api, bn: bn, status: makerStatus{UIDs: uids},
		contracts: map[string]contractInfo{}, steps: map[string]decimal.Decimal{}, lastIndex: map[string]decimal.Decimal{},
		lastPrint: map[string]printRecord{}, dirty: map[string]bool{},
		offCur: map[string]decimal.Decimal{}, offInit: map[string]bool{}}
}

func (m *maker) uid(i int) uint64 { return m.cfg.baseUID + uint64(i) }

func (m *maker) snapshot() makerStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.status
	s.Symbols = append([]symbolStatus(nil), s.Symbols...)
	return s
}

func (m *maker) setError(err error) {
	m.mu.Lock()
	m.status.LastError = err.Error()
	m.status.ErrorAt = time.Now().UnixMilli()
	m.mu.Unlock()
	log.Printf("[做市] %v", err)
}

// 设置行情情景的目标偏移(百分点，负数=下跌)。实际偏移每个周期推进offsetStep，不会一步跳过去
func (m *maker) setTarget(pct decimal.Decimal) error {
	if pct.Abs().GreaterThan(maxOffsetPct) {
		return fmt.Errorf("偏移不能超过±%s%%", maxOffsetPct)
	}
	m.mu.Lock()
	m.target = pct
	m.status.TargetPct = pct.String()
	m.mu.Unlock()
	return nil
}

func (m *maker) targetPct() decimal.Decimal {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.target
}

// 这个合约这个周期该用的偏移。第一次(每次启动后)先按我们现有的标记价跟币安中间价反推当前偏移：
// 我们的标记价可能已经停在离币安很远的地方(上次的情景没走回来、或者演示数据的旧价格)，直接按"偏移0"去
// 挂单，价格离标记价超过5%会全被拒。之后每个周期往目标推进一步。
// 查标记价失败就返回错误、这个周期不挂单，下个周期重试——不能当成"偏移0"继续往下走，那样偏移会被固定成0，
// 之后每笔下单都因为超出价格保护带被拒。反推出来的偏移也不能截断到±30%：标记价离币安很远时，截断后的
// 报价仍然进不了保护带，实际偏移比目标范围大没关系，往目标推进就是了
func (m *maker) offsetFor(ctx context.Context, sym string, mid decimal.Decimal) (decimal.Decimal, error) {
	if !m.offInit[sym] {
		raw, err := m.api.call(ctx, "GET", "/market/ticker?symbol="+sym, nil)
		if err != nil {
			return decimal.Zero, fmt.Errorf("取标记价反推当前偏移: %w", err)
		}
		var tk struct {
			MarkPrice *string `json:"markPrice"`
		}
		if err := json.Unmarshal(raw, &tk); err != nil {
			return decimal.Zero, fmt.Errorf("解析行情: %w", err)
		}
		cur := decimal.Zero // 合约还没有标记价(没成交过)：下单时后端拿指数价当参考，偏移0就是对的
		if tk.MarkPrice != nil {
			if mark, err := decimal.NewFromString(*tk.MarkPrice); err == nil && mark.Sign() > 0 && mid.Sign() > 0 {
				cur = mark.Div(mid).Sub(one).Mul(decimal.NewFromInt(100)).Round(2)
				if cur.Abs().LessThan(decimal.RequireFromString("0.05")) {
					cur = decimal.Zero
				}
			}
		}
		m.offCur[sym] = cur
		m.offInit[sym] = true
	}
	cur := rampOffset(m.offCur[sym], m.targetPct(), m.cfg.offsetStep)
	m.offCur[sym] = cur
	return cur, nil
}

// ---- 启停 ----

func (m *maker) start() {
	m.lifecycle.Lock()
	defer m.lifecycle.Unlock()
	m.startLocked()
}

func (m *maker) startLocked() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.done = make(chan struct{})
	m.status.Enabled = true
	m.status.LastError = ""
	m.offInit = map[string]bool{} // 重新启动后，按我们现有的标记价重新反推当前偏移，避免一步跳变
	go m.loop(ctx, m.done)
}

// 停掉做市循环，并把系统账户在订单簿里的挂单全部撤掉(不撤的话订单簿里会留着一堆过期的报价，
// 用户还能跟它们成交)
func (m *maker) stop() {
	m.lifecycle.Lock()
	defer m.lifecycle.Unlock()
	m.stopLocked()
}

func (m *maker) stopLocked() {
	m.mu.Lock()
	cancel, done := m.cancel, m.done
	m.cancel = nil
	m.status.Enabled = false
	m.mu.Unlock()
	if cancel == nil {
		return
	}
	if m.afterDetach != nil {
		m.afterDetach()
	}
	cancel()
	<-done
	ctx, c := context.WithTimeout(context.Background(), 15*time.Second)
	defer c()
	for i := 0; i < 4; i++ {
		if _, err := m.api.call(ctx, "POST", "/order/cancel-all", map[string]any{"uid": m.uid(i)}); err != nil && !isAccountMissing(err) {
			m.setError(fmt.Errorf("停止时撤单失败 uid=%d: %w", m.uid(i), err))
		}
	}
}

func isAccountMissing(err error) bool {
	e, ok := err.(*apiError)
	return ok && e.code == "account_not_found"
}

// 重置：停掉做市、结束本轮(按标记价平掉系统账户累积的全部仓位、回收额度)，之后如果原来在运行就重新启动
func (m *maker) reset() (err error) {
	m.lifecycle.Lock()
	defer m.lifecycle.Unlock()
	m.mu.Lock()
	was := m.cancel != nil
	m.mu.Unlock()
	m.stopLocked()
	// 不管中途成不成功，原来在运行的就要恢复运行：结束本轮失败了也不能让做市悄悄停掉
	defer func() {
		if was {
			if err == nil {
				time.Sleep(3 * time.Second) // 结束本轮是异步的，等平仓和撤单处理完再重新开始挂单
			}
			m.startLocked()
		}
	}()
	ctx, c := context.WithTimeout(context.Background(), 20*time.Second)
	defer c()
	for i := 0; i < 4; i++ {
		raw, err := m.api.call(ctx, "GET", fmt.Sprintf("/account/info?uid=%d", m.uid(i)), nil)
		if err != nil {
			if isAccountMissing(err) {
				continue
			}
			return err
		}
		var info struct {
			Round uint64 `json:"round"`
		}
		_ = json.Unmarshal(raw, &info)
		if _, err := m.api.call(ctx, "POST", "/account/round/close", map[string]any{"uid": m.uid(i), "round": info.Round}); err != nil {
			return err
		}
	}
	return nil
}

// ---- 主循环 ----

func (m *maker) loop(ctx context.Context, done chan struct{}) {
	defer close(done)
	n := m.loopsActive.Add(1)
	defer m.loopsActive.Add(-1)
	for {
		max := m.loopsMax.Load()
		if n <= max || m.loopsMax.CompareAndSwap(max, n) {
			break
		}
	}
	for !m.ensureReady(ctx) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
	t := time.NewTicker(m.cfg.interval)
	defer t.Stop()
	for {
		for _, sym := range m.cfg.symbols {
			if ctx.Err() != nil {
				return
			}
			if err := m.cycle(ctx, sym); err != nil && ctx.Err() == nil {
				m.setError(fmt.Errorf("%s: %w", sym, err))
			}
		}
		m.mu.Lock()
		m.status.Cycles++
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// 系统账户建好、余额充足、合约参数取到。任何一步失败返回false，稍后重试
func (m *maker) ensureReady(ctx context.Context) bool {
	for i := 0; i < 4; i++ {
		uid := m.uid(i)
		if _, err := m.api.call(ctx, "POST", "/account/create", map[string]any{"uid": uid}); err != nil {
			m.setError(fmt.Errorf("创建系统账户%d: %w", uid, err))
			return false
		}
		raw, err := m.api.call(ctx, "GET", fmt.Sprintf("/account/info?uid=%d", uid), nil)
		if err != nil {
			m.setError(err)
			return false
		}
		var info struct {
			Available string `json:"available"`
		}
		_ = json.Unmarshal(raw, &info)
		avail, _ := decimal.NewFromString(info.Available)
		bal, _ := decimal.NewFromString(m.cfg.balance)
		if avail.LessThan(bal.Div(decimal.NewFromInt(2))) {
			if _, err := m.api.call(ctx, "POST", "/account/balance", map[string]any{
				"uid": uid, "amount": m.cfg.balance, "requestId": fmt.Sprintf("mk-topup-%d-%d", uid, time.Now().UnixNano()),
			}); err != nil {
				m.setError(fmt.Errorf("给系统账户%d充值: %w", uid, err))
				return false
			}
		}
	}
	for _, sym := range m.cfg.symbols {
		if _, ok := m.contracts[sym]; ok {
			continue
		}
		raw, err := m.api.call(ctx, "GET", "/contract/detail?symbol="+sym, nil)
		if err != nil {
			m.setError(fmt.Errorf("取合约%s参数: %w", sym, err))
			return false
		}
		var c struct {
			BaseCoinScale int32  `json:"baseCoinScale"`
			MinVolume     string `json:"minVolume"`
		}
		_ = json.Unmarshal(raw, &c)
		minV, _ := decimal.NewFromString(c.MinVolume)
		m.contracts[sym] = contractInfo{qtyDP: c.BaseCoinScale, minVolume: minV}
	}
	return true
}

func (m *maker) cycle(ctx context.Context, sym string) error {
	book, err := m.bn.fetch(ctx, sym, m.cfg.depthLimit)
	if err != nil {
		return err
	}
	if book.mid().Sign() <= 0 {
		return fmt.Errorf("币安返回的盘口是空的")
	}
	ci := m.contracts[sym]

	// 状态里显示的是币安的原始价格，偏移之前先记下来
	bnBid, bnAsk := book.bestBid().String(), book.bestAsk().String()

	// 行情情景：整个盘口和指数价乘上偏移系数，币安的价格加上偏移就是我们的行情
	off, err := m.offsetFor(ctx, sym, book.mid())
	if err != nil {
		return err
	}
	factor := offsetFactor(off)
	book.bids, book.asks = shiftLevels(book.bids, factor), shiftLevels(book.asks, factor)
	book.index = book.index.Mul(factor).Round(2)
	ramping := !off.Equal(m.targetPct())

	// 指数价跟着行情走：资金费率的溢价率=(标记价-指数价)/指数价，指数价要有参照
	if book.index.Sign() > 0 && !book.index.Equal(m.lastIndex[sym]) {
		if _, err := m.api.call(ctx, "POST", "/index-price", map[string]any{"symbol": sym, "price": book.index.String()}); err != nil {
			// 指数价同步失败只影响资金费率的参照，不能因为它让整个周期的挂单都停掉
			m.setError(fmt.Errorf("%s同步指数价: %w", sym, err))
		} else {
			m.lastIndex[sym] = book.index
		}
	}

	step, ok := m.steps[sym]
	if !ok {
		step = niceStep(book.mid().Mul(decimal.RequireFromString("0.00005")))
		m.steps[sym] = step
	}
	bids := m.desired(aggregate(book.bids, step, true, m.cfg.levels), ci)
	asks := m.desired(aggregate(book.asks, step, false, m.cfg.levels), ci)

	var liveBids, liveAsks int
	if err := m.syncLadders(ctx, sym, bids, asks, &liveBids, &liveAsks); err != nil {
		return err
	}

	st := symbolStatus{Symbol: sym, BinanceBid: bnBid, BinanceAsk: bnAsk, LiveBids: liveBids, LiveAsks: liveAsks, OffsetPct: off.String()}
	if len(bids) > 0 {
		st.OurBestBid = bids[0].price.String()
	}
	if len(asks) > 0 {
		st.OurBestAsk = asks[0].price.String()
	}
	if len(bids) > 0 && len(asks) > 0 {
		if err := m.maybePrint(ctx, sym, bids[0].price, asks[0].price, ci, ramping); err != nil {
			m.setError(fmt.Errorf("%s对敲: %w", sym, err))
		}
	}
	if pr, ok := m.lastPrint[sym]; ok {
		st.LastPrintAt, st.LastPrint = pr.at.UnixMilli(), pr.price.String()
	}
	m.mu.Lock()
	replaced := false
	for i := range m.status.Symbols {
		if m.status.Symbols[i].Symbol == sym {
			m.status.Symbols[i], replaced = st, true
		}
	}
	if !replaced {
		m.status.Symbols = append(m.status.Symbols, st)
	}
	m.mu.Unlock()
	return nil
}

func (m *maker) desired(levels []level, ci contractInfo) []level {
	out := make([]level, 0, len(levels))
	for _, l := range levels {
		if q := scaleQty(l.qty, m.cfg.scale, ci.qtyDP, ci.minVolume); q.Sign() > 0 {
			out = append(out, level{price: l.price, qty: q})
		}
	}
	return out
}

// 让买盘、卖盘两个系统账户在订单簿里的挂单跟期望的档位一致：先查两边当前的挂单，算出差异，先把要撤的都撤掉，
// 再补挂缺的。撤单和挂单不能并行发：撤单事件和下单事件走两个独立的Kafka消费者，撤单请求返回不代表引擎已经把
// 旧单摘掉了。行情上行时新的买单可能高于还没摘掉的旧卖单，两个系统账户之间就会真实成交，把最新成交价(标记价)
// 拖回过期的价格。所以新挂的买单必须低于所有还挂着的卖单(含刚发出撤单请求、还没摘掉的)，新挂的卖单必须高于所有
// 还挂着的买单，被这条规则挡下的档位留到下个周期，那时旧单已经撤掉了
func (m *maker) syncLadders(ctx context.Context, sym string, bids, asks []level, liveBids, liveAsks *int) error {
	existingBids, err := m.liveOrders(ctx, sym, m.uid(0), "long")
	if err != nil {
		return err
	}
	existingAsks, err := m.liveOrders(ctx, sym, m.uid(1), "short")
	if err != nil {
		return err
	}
	cancelBids, placeBids := plan(bids, existingBids)
	cancelAsks, placeAsks := plan(asks, existingAsks)

	var cancels []func() error
	for _, id := range cancelBids {
		cancels = append(cancels, m.cancelFn(m.uid(0), id))
	}
	for _, id := range cancelAsks {
		cancels = append(cancels, m.cancelFn(m.uid(1), id))
	}
	if err := runLimited(cancels); err != nil {
		return err
	}

	minAsk, hasAsk := minLivePrice(existingAsks)
	maxBid, hasBid := maxLivePrice(existingBids)
	placeBids = placesBelow(placeBids, minAsk, hasAsk)
	placeAsks = placesAbove(placeAsks, maxBid, hasBid)
	var places []func() error
	for _, p := range placeBids {
		p := p
		places = append(places, func() error {
			_, err := m.api.call(ctx, "POST", "/order/add", m.orderBody(m.uid(0), sym, "long", p.price, p.qty))
			return err
		})
	}
	for _, p := range placeAsks {
		p := p
		places = append(places, func() error {
			_, err := m.api.call(ctx, "POST", "/order/add", m.orderBody(m.uid(1), sym, "short", p.price, p.qty))
			return err
		})
	}
	*liveBids = len(existingBids) - len(cancelBids) + len(placeBids)
	*liveAsks = len(existingAsks) - len(cancelAsks) + len(placeAsks)
	return runLimited(places)
}

func (m *maker) cancelFn(uid uint64, id string) func() error {
	return func() error {
		_, err := m.api.call(context.Background(), "POST", "/order/cancel/"+id, map[string]any{"uid": uid})
		// 已经在撤/已经成交完的单子再撤会报order_not_cancelable，不算错误
		if e, ok := err.(*apiError); ok && e.code == "order_not_cancelable" {
			return nil
		}
		return err
	}
}

// 并发执行(上限4个，别一口气打出几十个请求)，等全部完成，返回遇到的第一个错误
func runLimited(fns []func() error) error {
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	var mu sync.Mutex
	var firstErr error
	for _, fn := range fns {
		fn := fn
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := fn(); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return firstErr
}

func (m *maker) orderBody(uid uint64, sym, side string, price, qty decimal.Decimal) map[string]any {
	return map[string]any{
		"uid": uid, "symbol": sym, "side": side, "action": "open", "type": "limit",
		"price": price.String(), "amount": qty.String(), "leverage": m.cfg.leverage,
		// requestId只允许字母数字和_-，价格里的小数点要换掉
		"requestId": fmt.Sprintf("mk-%d-%s-%s-%d", uid, sym, strings.ReplaceAll(price.String(), ".", "_"), time.Now().UnixNano()),
	}
}

func (m *maker) liveOrders(ctx context.Context, sym string, uid uint64, side string) ([]liveOrder, error) {
	raw, err := m.api.call(ctx, "GET", fmt.Sprintf("/order/current?uid=%d&symbol=%s", uid, sym), nil)
	if err != nil {
		return nil, err
	}
	var orders []struct {
		OrderID      string `json:"orderId"`
		Side         string `json:"side"`
		Action       string `json:"action"`
		Type         string `json:"type"`
		Price        string `json:"price"`
		Amount       string `json:"amount"`
		TradedAmount string `json:"tradedAmount"`
	}
	if err := json.Unmarshal(raw, &orders); err != nil {
		return nil, err
	}
	var out []liveOrder
	for _, o := range orders {
		if o.Side != side || o.Action != "open" || o.Type != "limit" {
			continue
		}
		price, _ := decimal.NewFromString(o.Price)
		amount, _ := decimal.NewFromString(o.Amount)
		traded, _ := decimal.NewFromString(o.TradedAmount)
		out = append(out, liveOrder{id: o.OrderID, price: price, amount: amount, remaining: amount.Sub(traded)})
	}
	return out, nil
}

// 对敲：在我们自己的买一卖一之间(取中点)，让两个对敲账户成交一笔最小量，把最新成交价拉到
// 币安的价格上(K线和成交列表跟着动，标记价不受它影响)。卖单先挂、买单再来吃它——同一个合约的下单事件在Kafka里有序，引擎按这个顺序处理。
// 如果价差里恰好有用户的挂单，对敲的单子会先跟用户的单子成交(价格更好，是真实成交)，剩下没成交的对敲单
// 在下一轮被清理，不会留在订单簿里
func (m *maker) maybePrint(ctx context.Context, sym string, bestBid, bestAsk decimal.Decimal, ci contractInfo, force bool) error {
	if m.dirty[sym] {
		for _, i := range []int{2, 3} {
			if _, err := m.api.call(ctx, "POST", "/order/cancel-all", map[string]any{"uid": m.uid(i), "symbol": sym}); err != nil {
				return fmt.Errorf("清理对敲残留: %w", err)
			}
		}
		m.dirty[sym] = false
	}
	p := bestBid.Add(bestAsk).Div(decimal.NewFromInt(2))
	last, printed := m.lastPrint[sym]
	if printed && !force { // 行情情景正在推进时每个周期都对敲，标记价才跟得上一步步移动的报价
		if time.Since(last.at) < m.cfg.printEvery {
			return nil
		}
		drift, _ := p.Sub(last.price).Abs().Div(p).Float64()
		if drift < printDriftRatio && time.Since(last.at) < printMaxIdle {
			return nil
		}
	}
	qty := ci.minVolume
	if qty.Sign() <= 0 {
		qty = decimal.RequireFromString("0.001")
	}
	// 先标记"下个周期要清理对敲账户的残留挂单"，再下单：卖单挂出去之后买单如果失败，卖单就孤零零留在订单簿里，
	// 成了最优卖价被用户的买单吃掉；先标记的话不管哪一步失败，下个周期都会清理
	m.dirty[sym] = true
	if _, err := m.api.call(ctx, "POST", "/order/add", m.orderBody(m.uid(3), sym, "short", p, qty)); err != nil {
		return err
	}
	if _, err := m.api.call(ctx, "POST", "/order/add", m.orderBody(m.uid(2), sym, "long", p, qty)); err != nil {
		return err
	}
	m.lastPrint[sym] = printRecord{at: time.Now(), price: p}
	return nil
}
