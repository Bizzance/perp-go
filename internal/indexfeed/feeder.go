package indexfeed

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

// 把算好的指数价推给contract-api
type Publisher interface {
	Publish(ctx context.Context, symbol string, price decimal.Decimal) error
}

type Config struct {
	Symbols []string
	Sources []Source
	// 至少要有几家来源的价格一致才发布。默认2：只剩一家时不发布，喂价断了标记价会冻结、强平暂停，
	// 比单一来源出错时按错价强平安全
	MinSources int
	// 来源价格偏离中位数超过这个比例就当离群、丢掉。默认0.01(1%)
	Outlier decimal.Decimal
	// 一个周期内相对上次发布的价格变动超过这个比例，先不发布，要连续JumpConfirm个周期都在新的价位
	// 才承认。默认0.03(3%)。真实的闪崩只是延迟几个周期，单个来源的毛刺、抓取错位不会直接穿到标记价上
	MaxJump     decimal.Decimal
	JumpConfirm int           // 默认3
	Timeout     time.Duration // 单个来源的请求超时，默认2秒
}

func (c *Config) applyDefaults() {
	if c.MinSources <= 0 {
		c.MinSources = 2
	}
	if c.Outlier.Sign() <= 0 {
		c.Outlier = decimal.RequireFromString("0.01")
	}
	if c.MaxJump.Sign() <= 0 {
		c.MaxJump = decimal.RequireFromString("0.03")
	}
	if c.JumpConfirm <= 0 {
		c.JumpConfirm = 3
	}
	if c.Timeout <= 0 {
		c.Timeout = 2 * time.Second
	}
}

// 每个合约的发布状态
type symbolState struct {
	last       decimal.Decimal // 上次发布的价格，零=还没发布过
	jumpTarget decimal.Decimal // 正在等待确认的新价位，零=没有
	jumpCount  int             // 已经连续多少个周期出现在这个新价位
}

type Feeder struct {
	cfg Config
	pub Publisher

	mu       sync.Mutex
	states   map[string]*symbolState
	srcOK    map[string]bool // "来源/合约" -> 上一次是不是成功，只在变化时打日志
	lastWarn map[string]time.Time
}

func New(cfg Config, pub Publisher) *Feeder {
	cfg.applyDefaults()
	return &Feeder{cfg: cfg, pub: pub, states: map[string]*symbolState{}, srcOK: map[string]bool{}, lastWarn: map[string]time.Time{}}
}

// 定时跑，直到ctx结束
func (f *Feeder) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	f.Step(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			f.Step(ctx)
		}
	}
}

// 跑一个周期：每个合约并行去各来源取价、聚合、校验、发布
func (f *Feeder) Step(ctx context.Context) {
	var wg sync.WaitGroup
	for _, sym := range f.cfg.Symbols {
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			f.stepSymbol(ctx, sym)
		}(sym)
	}
	wg.Wait()
}

func (f *Feeder) stepSymbol(ctx context.Context, sym string) {
	prices := f.fetchAll(ctx, sym)
	price, used, err := Aggregate(prices, f.cfg.Outlier, f.cfg.MinSources)
	if err != nil {
		f.warn(sym+"/aggregate", "[ERROR] 指数价不发布, symbol=%s: %v (有效来源%d家)", sym, err, len(prices))
		return
	}
	if !f.acceptJump(sym, price) {
		return
	}
	if err := f.pub.Publish(ctx, sym, price); err != nil {
		f.warn(sym+"/publish", "[ERROR] 发布指数价失败, symbol=%s, price=%s: %v", sym, price, err)
		return
	}
	f.mu.Lock()
	f.state(sym).last = price
	f.mu.Unlock()
	if len(used) < len(f.cfg.Sources) {
		f.warn(sym+"/degraded", "[WARN] 指数价只用了%d/%d家来源, symbol=%s, 使用=%v", len(used), len(f.cfg.Sources), sym, used)
	}
}

func (f *Feeder) state(sym string) *symbolState {
	st, ok := f.states[sym]
	if !ok {
		st = &symbolState{}
		f.states[sym] = st
	}
	return st
}

// 并行向全部来源取价，返回成功的(来源名 -> 价格)。来源失败只记日志(状态变化时才记)
func (f *Feeder) fetchAll(ctx context.Context, sym string) map[string]decimal.Decimal {
	type result struct {
		name  string
		price decimal.Decimal
		err   error
	}
	out := make(chan result, len(f.cfg.Sources))
	for _, s := range f.cfg.Sources {
		go func(s Source) {
			cctx, cancel := context.WithTimeout(ctx, f.cfg.Timeout)
			defer cancel()
			p, err := s.Fetch(cctx, sym)
			out <- result{s.Name(), p, err}
		}(s)
	}
	prices := make(map[string]decimal.Decimal, len(f.cfg.Sources))
	for range f.cfg.Sources {
		r := <-out
		key := r.name + "/" + sym
		f.mu.Lock()
		was, seen := f.srcOK[key]
		f.srcOK[key] = r.err == nil
		f.mu.Unlock()
		if r.err != nil {
			if !seen || was {
				log.Printf("[WARN] 指数价来源失败, source=%s symbol=%s: %v", r.name, sym, r.err)
			}
			continue
		}
		if seen && !was {
			log.Printf("[INFO] 指数价来源恢复, source=%s symbol=%s", r.name, sym)
		}
		prices[r.name] = r.price
	}
	return prices
}

// 跳变保护：相对上次发布变动不超过MaxJump直接放行；超过的要连续JumpConfirm个周期都落在同一个新价位
// (彼此偏差不超过Outlier)才放行。返回false=这个周期不发布
func (f *Feeder) acceptJump(sym string, price decimal.Decimal) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	st := f.state(sym)
	if st.last.Sign() == 0 || relDiff(price, st.last).LessThanOrEqual(f.cfg.MaxJump) {
		st.jumpTarget, st.jumpCount = decimal.Zero, 0
		return true
	}
	if st.jumpTarget.Sign() > 0 && relDiff(price, st.jumpTarget).LessThanOrEqual(f.cfg.Outlier) {
		st.jumpCount++
	} else {
		st.jumpTarget, st.jumpCount = price, 1
	}
	if st.jumpCount >= f.cfg.JumpConfirm {
		log.Printf("[WARN] 指数价大幅跳变已确认, symbol=%s, %s -> %s", sym, st.last, price)
		st.jumpTarget, st.jumpCount = decimal.Zero, 0
		return true
	}
	f.warnLocked(sym+"/jump", "[WARN] 指数价相对上次发布跳变超过%s，等待确认(%d/%d), symbol=%s, %s -> %s",
		f.cfg.MaxJump, st.jumpCount, f.cfg.JumpConfirm, sym, st.last, price)
	return false
}

// 日志限流：同一类问题每分钟最多一条，喂价每秒一轮，出问题时不能刷屏
func (f *Feeder) warn(key, format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.warnLocked(key, format, args...)
}

func (f *Feeder) warnLocked(key, format string, args ...any) {
	if t, ok := f.lastWarn[key]; ok && time.Since(t) < time.Minute {
		return
	}
	f.lastWarn[key] = time.Now()
	log.Printf(format, args...)
}

// |a-b|/b
func relDiff(a, b decimal.Decimal) decimal.Decimal {
	return a.Sub(b).Abs().Div(b)
}

// 把各来源的价格聚合成一个指数价：取中位数，丢掉偏离中位数超过outlier的来源，剩下的不足minSources家就
// 拒绝发布。返回的价格是剩下这些来源的中位数，used是它们的名字(排序后)。两家来源的中位数是均值，
// 两家相差超过2*outlier时各自离均值都超过outlier、都被丢掉，也就是"两家对不上就不发布"，不猜哪家对
func Aggregate(prices map[string]decimal.Decimal, outlier decimal.Decimal, minSources int) (decimal.Decimal, []string, error) {
	if len(prices) < minSources {
		return decimal.Zero, nil, fmt.Errorf("只有%d家来源有价格，至少需要%d家", len(prices), minSources)
	}
	m := medianOf(prices)
	var used []string
	kept := map[string]decimal.Decimal{}
	for name, p := range prices {
		if relDiff(p, m).LessThanOrEqual(outlier) {
			used = append(used, name)
			kept[name] = p
		}
	}
	if len(kept) < minSources {
		return decimal.Zero, nil, fmt.Errorf("来源价格对不上: 中位数%s，偏离在%s以内的只有%d家，至少需要%d家", m, outlier, len(kept), minSources)
	}
	sort.Strings(used)
	return medianOf(kept), used, nil
}

// 中位数，偶数个取中间两个的均值
func medianOf(prices map[string]decimal.Decimal) decimal.Decimal {
	v := make([]decimal.Decimal, 0, len(prices))
	for _, p := range prices {
		v = append(v, p)
	}
	sort.Slice(v, func(i, j int) bool { return v[i].LessThan(v[j]) })
	n := len(v)
	if n%2 == 1 {
		return v[n/2]
	}
	return v[n/2-1].Add(v[n/2]).Div(decimal.NewFromInt(2))
}
