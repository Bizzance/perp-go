// Package booksync 把币安的指数价、K线同步进contract-api，供contract-api对外提供行情(GET /market/ticker、
// GET /kline)。**订单簿镜像不在这里**——那部分要直接碰撮合引擎和账户，已经挪到contract-engine内部
// (internal/service的Mirror组件)，见docs/orderbook-sync.md。
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
	"time"

	"perp-go/internal/api"
	"perp-go/internal/binancefeed"
	"perp-go/internal/model"
)

type Config struct {
	Symbols []string
	// 币安数据超过这么久没拉成功，只影响日志限流频率(下面warn)，指数价/K线推送失败本身不会
	// 累积成任何需要清理的状态，跟"挂单同步"不一样，不需要真的"暂停/恢复"
	Interval time.Duration // 每一轮的间隔
	// 把币安的K线同步进我们系统(POST /kline/sync)：每KlineEvery同步一次最近几根，第一次(和很久没成功之后)补最多
	// KlineBackfill根历史。KlineEvery<=0表示不同步K线。要求contract-api和engine都配了PERP_KLINE_SOURCE=external
	KlineEvery    time.Duration
	KlineBackfill int
}

func (c Config) validate() error {
	switch {
	case len(c.Symbols) == 0:
		return errors.New("Symbols不能为空")
	case c.KlineBackfill < 0 || c.KlineBackfill > binancefeed.MaxKlineLimit:
		return fmt.Errorf("KlineBackfill必须在0到%d之间", binancefeed.MaxKlineLimit)
	case c.Interval <= 0:
		return errors.New("Interval必须大于0")
	}
	return nil
}

// 每个合约的运行状态。每个合约的一轮在自己的goroutine里跑，只碰自己的这一份，不需要锁
type symState struct {
	lastIndexWarn time.Time
	lastKlineWarn time.Time
	klineSynced   map[model.KlineInterval]time.Time // 每个周期上一次成功同步的时间
}

type Syncer struct {
	cfg Config
	api *apiClient
	bn  *binancefeed.Binance
	now func() time.Time // 测试里可以替换

	states map[string]*symState
}

func New(cfg Config, apiURL, keyID, secret string, bn *binancefeed.Binance) (*Syncer, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	s := &Syncer{
		cfg: cfg, bn: bn, now: time.Now,
		api:    &apiClient{base: strings.TrimRight(apiURL, "/"), keyID: keyID, secret: secret, http: &http.Client{Timeout: 10 * time.Second}},
		states: map[string]*symState{},
	}
	for _, sym := range cfg.Symbols {
		s.states[sym] = &symState{klineSynced: map[model.KlineInterval]time.Time{}}
	}
	return s, nil
}

// 定时跑，直到ctx结束
func (s *Syncer) Run(ctx context.Context) {
	t := time.NewTicker(s.cfg.Interval)
	defer t.Stop()
	// K线同步单独一个goroutine：每轮要拉币安六个周期再逐个推给contract-api，耗时不能拖慢指数价推送
	klinesDone := make(chan struct{})
	go func() { defer close(klinesDone); s.runKlines(ctx) }()
	s.Step(ctx)
	for {
		select {
		case <-ctx.Done():
			<-klinesDone
			return
		case <-t.C:
			s.Step(ctx)
		}
	}
}

// 定时同步K线，直到ctx结束
func (s *Syncer) runKlines(ctx context.Context) {
	if s.cfg.KlineEvery <= 0 {
		return
	}
	t := time.NewTicker(s.cfg.KlineEvery)
	defer t.Stop()
	s.SyncKlines(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.SyncKlines(ctx)
		}
	}
}

// 同步一轮K线：每个合约、每个周期把币安最近的几根推给contract-api。第一次同步补KlineBackfill根历史；
// 之后每次只拉"距离上次成功同步过去了多少根、再多2根"(至少3根，最多KlineBackfill根)，这样币安或我们的api
// 中间断了一阵，恢复后会自动把断的那段补上，不用重启
func (s *Syncer) SyncKlines(ctx context.Context) {
	for _, sym := range s.cfg.Symbols {
		st := s.states[sym]
		for _, interval := range model.AllKlineIntervals {
			if err := s.syncKline(ctx, sym, interval, st); err != nil {
				s.warn(&st.lastKlineWarn, "[WARN] 同步币安K线失败, symbol=%s interval=%s: %v", sym, interval, err)
			}
		}
	}
}

func (s *Syncer) syncKline(ctx context.Context, sym string, interval model.KlineInterval, st *symState) error {
	limit := s.cfg.KlineBackfill
	if last, ok := st.klineSynced[interval]; ok {
		step := model.KlineIntervalMillis[interval]
		missed := int(s.now().Sub(last).Milliseconds()/step) + 3
		limit = min(max(missed, 3), s.cfg.KlineBackfill)
	}
	if limit <= 0 {
		return nil // KlineBackfill配成0：不同步
	}
	candles, err := s.bn.Klines(ctx, sym, string(interval), limit)
	if err != nil {
		return err
	}
	if len(candles) == 0 {
		return nil
	}
	rows := make([]map[string]any, len(candles))
	for i, c := range candles {
		rows[i] = map[string]any{"openTime": c.OpenTime, "open": c.Open.String(), "high": c.High.String(), "low": c.Low.String(),
			"close": c.Close.String(), "volume": c.Volume.String(), "tradeCount": c.Trades}
	}
	// 分批推：一次请求的体积有上限(64KB)，补几百根历史时一批推不完。从旧到新，中途失败的话已经推过的不用回滚
	// (整根覆盖，重推结果不变)，klineSynced不更新，下一轮重新补
	for start := 0; start < len(rows); start += klineChunk {
		end := min(start+klineChunk, len(rows))
		if _, err := s.api.call(ctx, "POST", "/kline/sync", map[string]any{"symbol": sym, "interval": string(interval), "candles": rows[start:end]}); err != nil {
			var e *apiError
			if errors.As(err, &e) && e.code == "kline_source_not_external" {
				return fmt.Errorf("contract-api不接收K线同步，contract-api和contract-engine都要设置PERP_KLINE_SOURCE=external: %w", err)
			}
			return err
		}
	}
	st.klineSynced[interval] = s.now()
	return nil
}

// 一次POST /kline/sync最多推多少根，不能超过contract-api的上限(同样受请求体大小限制)
const klineChunk = 200

// 跑一轮：每个合约推一次指数价
func (s *Syncer) Step(ctx context.Context) {
	for _, sym := range s.cfg.Symbols {
		s.pushIndex(ctx, sym, s.states[sym])
	}
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
// 这把密钥只需要ops权限(指数价、K线同步都是运营接口)
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
