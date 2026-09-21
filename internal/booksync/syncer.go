package booksync

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/api"
)

type Config struct {
	Symbols []string
	// 两个系统账户：买盘账户的uid是BaseUID(开多挂买单)，卖盘账户是BaseUID+1(开空挂卖单)
	BaseUID  uint64
	Levels   int           // 每侧同步币安盘口的前多少档
	Interval time.Duration // 每一轮的间隔
	Leverage int           // 系统账户挂单的杠杆。全部保证金分档里最低的最大杠杆是5，用5在哪个档位都不会被拒
	Balance  string        // 系统账户的余额(USDT)，低于一半时补到这个数
	// 币安数据超过这么久没拉成功，就撤掉这个合约的全部挂单、暂停报价：过期的报价留在订单簿里，
	// 谁比我们更早看到币安的价格，谁就能按旧价成交
	StaleAfter time.Duration
}

func (c Config) validate() error {
	switch {
	case len(c.Symbols) == 0:
		return errors.New("Symbols不能为空")
	case c.BaseUID == 0:
		return errors.New("BaseUID必须大于0")
	case c.Levels <= 0:
		return errors.New("Levels必须大于0")
	case c.Interval <= 0 || c.StaleAfter <= 0:
		return errors.New("Interval和StaleAfter必须大于0")
	case c.Leverage <= 0:
		return errors.New("Leverage必须大于0")
	}
	if b, err := decimal.NewFromString(c.Balance); err != nil || b.Sign() <= 0 {
		return fmt.Errorf("Balance不合法: %q", c.Balance)
	}
	return nil
}

type contractInfo struct {
	qtyDP     int32           // 数量的小数位数
	minVolume decimal.Decimal // 最小下单量
}

// 每个合约的运行状态。每个合约的一轮在自己的goroutine里跑，只碰自己的这一份，不需要锁
type symState struct {
	lastOK        time.Time // 上一次成功拉到币安盘口的时间
	stopped       bool      // 已经因为币安数据过期撤了全部挂单
	lastWarn      time.Time
	lastIndexWarn time.Time
}

type Syncer struct {
	cfg        Config
	api        *apiClient
	bn         *Binance
	depthLimit int
	now        func() time.Time // 测试里可以替换

	// 撤单是异步的(经Kafka)，补挂被挡下的档位前要等旧单真的撤掉：最多等cancelWait，每pollEvery查一次
	cancelWait time.Duration
	pollEvery  time.Duration

	startedAt time.Time
	ready     bool
	contracts map[string]contractInfo
	states    map[string]*symState
}

func New(cfg Config, apiURL, keyID, secret string, bn *Binance) (*Syncer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	limit, ok := depthLimitFor(cfg.Levels)
	if !ok {
		return nil, fmt.Errorf("Levels=%d超过币安深度接口的上限1000", cfg.Levels)
	}
	s := &Syncer{
		cfg: cfg, bn: bn, depthLimit: limit, now: time.Now, cancelWait: time.Second, pollEvery: 50 * time.Millisecond,
		api:       &apiClient{base: strings.TrimRight(apiURL, "/"), keyID: keyID, secret: secret, http: &http.Client{Timeout: 10 * time.Second}},
		contracts: map[string]contractInfo{}, states: map[string]*symState{},
	}
	for _, sym := range cfg.Symbols {
		s.states[sym] = &symState{}
	}
	s.startedAt = s.now()
	return s, nil
}

func (s *Syncer) bidUID() uint64 { return s.cfg.BaseUID }
func (s *Syncer) askUID() uint64 { return s.cfg.BaseUID + 1 }

// 定时跑，直到ctx结束。退出前撤掉全部系统挂单，不留过期的报价
func (s *Syncer) Run(ctx context.Context) {
	t := time.NewTicker(s.cfg.Interval)
	defer t.Stop()
	s.Step(ctx)
	for {
		select {
		case <-ctx.Done():
			cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			s.CancelAll(cctx)
			return
		case <-t.C:
			s.Step(ctx)
		}
	}
}

// 跑一轮：账户和合约参数没准备好就先准备，然后每个合约并行同步一次
func (s *Syncer) Step(ctx context.Context) {
	if !s.ready {
		if err := s.ensureReady(ctx); err != nil {
			log.Printf("[ERROR] 订单簿同步: 准备系统账户失败: %v", err)
			return
		}
		s.ready = true
	}
	var wg sync.WaitGroup
	for _, sym := range s.cfg.Symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			if err := s.cycle(ctx, sym); err != nil {
				s.warn(&s.states[sym].lastWarn, "[WARN] 订单簿同步失败, symbol=%s: %v", sym, err)
			}
		}(sym)
	}
	wg.Wait()
}

// 撤掉全部合约的全部系统挂单
func (s *Syncer) CancelAll(ctx context.Context) {
	for _, sym := range s.cfg.Symbols {
		if err := s.cancelAll(ctx, sym); err != nil {
			log.Printf("[ERROR] 撤系统挂单失败, symbol=%s: %v", sym, err)
		}
	}
}

func (s *Syncer) cycle(ctx context.Context, sym string) error {
	st := s.states[sym]
	// 每一轮顺带把币安的指数价推给contract-api。标记价拿指数价当锚，没有指数价生产环境就没有标记价，市价单和强平都不能用。
	// 跟盘口同步互不影响：指数价推失败不停挂单，盘口拉失败也照样推指数价
	s.pushIndex(ctx, sym, st)
	bids, asks, err := s.bn.Depth(ctx, sym, s.depthLimit)
	if err != nil {
		return s.sourceFailed(ctx, sym, st, err)
	}
	ci := s.contracts[sym]
	wantBids := desiredLevels(bids, s.cfg.Levels, ci.qtyDP, ci.minVolume)
	wantAsks := desiredLevels(asks, s.cfg.Levels, ci.qtyDP, ci.minVolume)
	if len(wantBids) == 0 || len(wantAsks) == 0 {
		// 币安的盘口有数据，但按我们合约的数量精度取整后一侧没有可挂的档位：不能只挂一侧
		return s.sourceFailed(ctx, sym, st, errors.New("按合约的数量精度取整后，盘口一侧没有可挂的档位"))
	}
	st.lastOK = s.now()
	if st.stopped {
		st.stopped = false
		log.Printf("[INFO] 币安数据恢复，重新同步订单簿, symbol=%s", sym)
	}
	return s.syncLadders(ctx, sym, wantBids, wantAsks)
}

// 把币安的指数价推给contract-api。失败只记日志(限流)：contract-api自己会在指数价30秒没更新时冻结标记价、
// 暂停强平，这里不用另外处理；服务端的跳变保护拦下的推送(index_price_jump)也是这样，下一轮继续推，满了确认时间就承认
func (s *Syncer) pushIndex(ctx context.Context, sym string, st *symState) {
	price, err := s.bn.IndexPrice(ctx, sym)
	if err == nil {
		_, err = s.api.call(ctx, "POST", "/index-price", map[string]any{"symbol": sym, "price": price.String()})
	}
	if err != nil {
		s.warn(&st.lastIndexWarn, "[WARN] 同步币安指数价失败, symbol=%s: %v", sym, err)
	}
}

// 拿不到可信的币安盘口：短时间内不动已经挂着的单子(可能只是一次抖动)，超过StaleAfter就全部撤掉、暂停报价
func (s *Syncer) sourceFailed(ctx context.Context, sym string, st *symState, cause error) error {
	since := st.lastOK
	if since.IsZero() {
		since = s.startedAt
	}
	if s.now().Sub(since) <= s.cfg.StaleAfter {
		return fmt.Errorf("拿不到币安盘口(挂单暂时不动): %w", cause)
	}
	if err := s.cancelAll(ctx, sym); err != nil {
		return fmt.Errorf("币安数据已过期但撤单失败: %w", err)
	}
	if !st.stopped {
		st.stopped = true
		log.Printf("[ERROR] 币安数据已经超过%s没更新，已撤掉全部挂单、暂停报价, symbol=%s: %v", s.cfg.StaleAfter, sym, cause)
	}
	return nil
}

// 让买盘、卖盘两个系统账户在订单簿里的挂单跟币安的盘口一致。要求：任何时候不能有一侧是空的(用户的市价单
// 一笔都吃不到)，同时两个系统账户的挂单不能交叉(交叉就会在两个系统账户之间真实成交，把最新成交价拖回过期的价格)。
//
// 难点在行情单边移动的时候。以上涨为例：旧卖单的价格偏低，是过期的报价，必须马上撤；新卖单价格更高，可以同时挂；
// 但新买单会高于还没撤掉的旧卖单，要等旧卖单撤掉才能挂(撤单经Kafka是异步的，撤单请求返回不代表引擎已经摘掉了)。
// 所以：
//   - 新挂的买单必须低于所有还挂着的卖单，新挂的卖单必须高于所有还挂着的买单，被挡下的先不挂
//   - 被挡下的那一侧(这里是买盘)，**旧单先留着不撤**：旧买单只是价格偏低，没有害处，撤了的话这一侧到新单挂出来之前就是空的
//   - 另一侧(卖盘)的旧单立刻撤，新单同时挂出，这一侧也不空
//   - 等旧卖单真的撤掉(轮询，最多cancelWait)，在同一轮里把被挡下的新买单挂上，最后才撤掉旧买单：先挂新的、再撤旧的
//
// 等不到撤单生效就留到下一轮，不会为了补挂而冒交叉的风险
func (s *Syncer) syncLadders(ctx context.Context, sym string, bids, asks []Level) error {
	existingBids, err := s.liveOrders(ctx, sym, s.bidUID(), "long")
	if err != nil {
		return err
	}
	existingAsks, err := s.liveOrders(ctx, sym, s.askUID(), "short")
	if err != nil {
		return err
	}
	cancelBids, placeBids := plan(bids, existingBids)
	cancelAsks, placeAsks := plan(asks, existingAsks)

	// 用撤单之前的快照判断哪些新单会碰到还挂着的对面委托
	minAsk, hasAsk := minLivePrice(existingAsks)
	maxBid, hasBid := maxLivePrice(existingBids)
	nowBids := placesBelow(placeBids, minAsk, hasAsk)
	nowAsks := placesAbove(placeAsks, maxBid, hasBid)
	heldBids, heldAsks := without(placeBids, nowBids), without(placeAsks, nowAsks)

	// 被挡下的那一侧，旧单留到新单挂出来之后再撤。正常情况下只会有一侧被挡(旧盘口没有交叉，新盘口也没有，
	// 两侧不可能同时被对面的旧单挡住)
	var deferred []func() error
	if len(heldBids) > 0 {
		for _, id := range cancelBids {
			deferred = append(deferred, s.cancelFn(ctx, s.bidUID(), id))
		}
		cancelBids = nil
	}
	if len(heldAsks) > 0 {
		for _, id := range cancelAsks {
			deferred = append(deferred, s.cancelFn(ctx, s.askUID(), id))
		}
		cancelAsks = nil
	}

	// 先挂新单、再撤旧单：新旧单在同一侧，互不交叉，先挂的话这一侧任何时候都不会是空的
	if err := s.place(ctx, sym, nowBids, nowAsks); err != nil {
		return err
	}
	var cancels []func() error
	for _, id := range cancelBids {
		cancels = append(cancels, s.cancelFn(ctx, s.bidUID(), id))
	}
	for _, id := range cancelAsks {
		cancels = append(cancels, s.cancelFn(ctx, s.askUID(), id))
	}
	if err := runLimited(cancels); err != nil {
		return err
	}
	if len(heldBids)+len(heldAsks) == 0 {
		return nil
	}

	// 等立刻撤的那些旧单真的撤掉，再按最新的挂单情况补挂被挡下的新单
	if !s.waitCancelled(ctx, sym, append(append([]string(nil), cancelBids...), cancelAsks...)) {
		return nil // 等不到，留到下一轮；延后撤的旧单也还没动，这一侧没有变空
	}
	freshBids, err := s.liveOrders(ctx, sym, s.bidUID(), "long")
	if err != nil {
		return err
	}
	freshAsks, err := s.liveOrders(ctx, sym, s.askUID(), "short")
	if err != nil {
		return err
	}
	minAsk, hasAsk = minLivePrice(freshAsks)
	maxBid, hasBid = maxLivePrice(freshBids)
	if err := s.place(ctx, sym, placesBelow(heldBids, minAsk, hasAsk), placesAbove(heldAsks, maxBid, hasBid)); err != nil {
		return err
	}
	return runLimited(deferred) // 新单已经挂出去了，最后才撤旧单
}

// 挂出这些买单(买盘账户)和卖单(卖盘账户)
func (s *Syncer) place(ctx context.Context, sym string, bids, asks []placement) error {
	var places []func() error
	for _, p := range bids {
		places = append(places, s.placeFn(ctx, s.bidUID(), sym, "long", p))
	}
	for _, p := range asks {
		places = append(places, s.placeFn(ctx, s.askUID(), sym, "short", p))
	}
	return runLimited(places)
}

// all里去掉kept里出现过的价位
func without(all, kept []placement) []placement {
	have := make(map[string]bool, len(kept))
	for _, p := range kept {
		have[p.price.String()] = true
	}
	var out []placement
	for _, p := range all {
		if !have[p.price.String()] {
			out = append(out, p)
		}
	}
	return out
}

// 等这些委托真的从订单簿里撤掉(不再出现在当前委托里)，最多等cancelWait。撤单请求返回只表示事件发出去了，
// 引擎处理完、委托状态变成已撤销才算生效。返回是否等到了
func (s *Syncer) waitCancelled(ctx context.Context, sym string, ids []string) bool {
	if len(ids) == 0 {
		return true
	}
	deadline := time.Now().Add(s.cancelWait)
	for {
		stillThere := false
		for _, uid := range []uint64{s.bidUID(), s.askUID()} {
			live, err := s.liveOrders(ctx, sym, uid, "")
			if err != nil {
				return false
			}
			present := make(map[string]bool, len(live))
			for _, o := range live {
				present[o.id] = true
			}
			for _, id := range ids {
				if present[id] {
					stillThere = true
				}
			}
		}
		if !stillThere {
			return true
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return false
		}
		time.Sleep(s.pollEvery)
	}
}

func (s *Syncer) placeFn(ctx context.Context, uid uint64, sym, side string, p placement) func() error {
	return func() error {
		_, err := s.api.call(ctx, "POST", "/order/add", map[string]any{
			"uid": uid, "symbol": sym, "side": side, "action": "open", "type": "limit",
			"price": p.price.String(), "amount": p.qty.String(), "leverage": s.cfg.Leverage,
			// requestId只允许字母数字和_-，价格里的小数点要换掉
			"requestId": fmt.Sprintf("bs-%d-%s-%s-%d", uid, sym, strings.ReplaceAll(p.price.String(), ".", "_"), time.Now().UnixNano()),
		})
		return err
	}
}

func (s *Syncer) cancelFn(ctx context.Context, uid uint64, id string) func() error {
	return func() error {
		_, err := s.api.call(ctx, "POST", "/order/cancel/"+id, map[string]any{"uid": uid})
		// 已经在撤/已经成交完的单子再撤会报order_not_cancelable，不算错误
		var e *apiError
		if errors.As(err, &e) && e.code == "order_not_cancelable" {
			return nil
		}
		return err
	}
}

// 撤掉这个合约上两个系统账户的全部挂单
func (s *Syncer) cancelAll(ctx context.Context, sym string) error {
	var cancels []func() error
	for _, uid := range []uint64{s.bidUID(), s.askUID()} {
		orders, err := s.liveOrders(ctx, sym, uid, "")
		if err != nil {
			return err
		}
		for _, o := range orders {
			cancels = append(cancels, s.cancelFn(ctx, uid, o.id))
		}
	}
	return runLimited(cancels)
}

// 并发执行(上限4个，别一口气打出几十个请求)，等全部完成，返回遇到的第一个错误
func runLimited(fns []func() error) error {
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	var mu sync.Mutex
	var firstErr error
	for _, fn := range fns {
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

// 这个账户在这个合约上还挂着的限价开仓单。side为空表示不按方向过滤(撤全部时用)
func (s *Syncer) liveOrders(ctx context.Context, sym string, uid uint64, side string) ([]liveOrder, error) {
	raw, err := s.api.call(ctx, "GET", fmt.Sprintf("/order/current?uid=%d&symbol=%s", uid, sym), nil)
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
		if o.Action != "open" || o.Type != "limit" || (side != "" && o.Side != side) {
			continue
		}
		price, _ := decimal.NewFromString(o.Price)
		amount, _ := decimal.NewFromString(o.Amount)
		traded, _ := decimal.NewFromString(o.TradedAmount)
		out = append(out, liveOrder{id: o.OrderID, price: price, amount: amount, remaining: amount.Sub(traded)})
	}
	return out, nil
}

// 建好两个系统账户(已经存在就是no-op)、余额低于一半时补到配置的数、查好每个合约的数量精度和最小下单量
func (s *Syncer) ensureReady(ctx context.Context) error {
	bal, _ := decimal.NewFromString(s.cfg.Balance)
	for _, uid := range []uint64{s.bidUID(), s.askUID()} {
		if _, err := s.api.call(ctx, "POST", "/account/create", map[string]any{"uid": uid}); err != nil {
			return fmt.Errorf("创建系统账户%d: %w", uid, err)
		}
		raw, err := s.api.call(ctx, "GET", fmt.Sprintf("/account/info?uid=%d", uid), nil)
		if err != nil {
			return fmt.Errorf("查系统账户%d: %w", uid, err)
		}
		var info struct {
			Available string `json:"available"`
		}
		_ = json.Unmarshal(raw, &info)
		avail, _ := decimal.NewFromString(info.Available)
		if avail.LessThan(bal.Div(decimal.NewFromInt(2))) {
			if _, err := s.api.call(ctx, "POST", "/account/balance", map[string]any{
				"uid": uid, "amount": s.cfg.Balance, "requestId": fmt.Sprintf("bs-topup-%d-%d", uid, time.Now().UnixNano()),
			}); err != nil {
				return fmt.Errorf("给系统账户%d充值: %w", uid, err)
			}
		}
	}
	for _, sym := range s.cfg.Symbols {
		raw, err := s.api.call(ctx, "GET", "/contract/detail?symbol="+sym, nil)
		if err != nil {
			return fmt.Errorf("取合约%s参数: %w", sym, err)
		}
		var c struct {
			BaseCoinScale int32  `json:"baseCoinScale"`
			MinVolume     string `json:"minVolume"`
		}
		_ = json.Unmarshal(raw, &c)
		minV, _ := decimal.NewFromString(c.MinVolume)
		s.contracts[sym] = contractInfo{qtyDP: c.BaseCoinScale, minVolume: minV}
	}
	return nil
}

// 日志限流：同一个合约的同一类问题每30秒最多一条，同步每秒一轮，出问题时不能刷屏
func (s *Syncer) warn(last *time.Time, format string, args ...any) {
	if !last.IsZero() && s.now().Sub(*last) < 30*time.Second {
		return
	}
	*last = s.now()
	log.Printf(format, args...)
}

// ---- 调后端 ----

type envelope struct {
	Code    int             `json:"code"`
	ErrCode string          `json:"errCode"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

type apiError struct{ code, msg string }

func (e *apiError) Error() string { return e.code + ": " + e.msg }

// 带签名调contract-api，签名用的是生产代码里的api.SignRequest，跟合作方对接用的是同一套。
// 这把密钥要同时有trade(建账户、下单撤单、查询)和ops(给系统账户充值)权限
type apiClient struct {
	base, keyID, secret string
	http                *http.Client
}

// 成功返回data，业务失败(code != 200)当作*apiError
func (a *apiClient) call(ctx context.Context, method, path string, body any) (json.RawMessage, error) {
	var raw []byte
	if body != nil {
		raw, _ = json.Marshal(body)
	}
	u, err := url.Parse(a.base + path)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	ts := strconv.FormatInt(time.Now().UnixMilli(), 10)
	nb := make([]byte, 12)
	_, _ = rand.Read(nb)
	nonce := hex.EncodeToString(nb)
	req.Header.Set("X-Api-Key", a.keyID)
	req.Header.Set("X-Timestamp", ts)
	req.Header.Set("X-Nonce", nonce)
	req.Header.Set("X-Signature", api.SignRequest(a.secret, ts, nonce, method, u.EscapedPath(), u.RawQuery, raw))

	resp, err := a.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	var env envelope
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, fmt.Errorf("响应不是JSON: %.200s", b)
	}
	if env.Code != 200 {
		return nil, &apiError{code: env.ErrCode, msg: env.Message}
	}
	return env.Data, nil
}
