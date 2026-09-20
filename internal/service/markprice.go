package service

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/cache"
)

// 标记价格的可调参数
type MarkPriceConfig struct {
	// 指数价超过这么久没更新就算陈旧(喂价断了)：标记价停止更新，强平、资金费率、条件单触发都暂停，
	// 见MarkPriceService.GetFresh
	MaxIndexAge time.Duration
	// 标记价、基差样本相对指数价的最大偏离比例(0.01=1%)。这是操纵能造成的最大偏离的上限。<=0表示不限制
	MaxDeviation decimal.Decimal
	// 盘口基差取多长时间窗口内的平均。窗口越长，越难靠短时间摆盘口去拉动标记价
	BasisWindow time.Duration
	// true=没有指数价就不产生标记价(生产环境应该开)；false=没有喂过指数价时退回"标记价=最新成交价"，
	// 只给本地开发和没有行情源的环境用——最新成交价可以被自成交操纵，见docs/mark-price.md
	RequireIndex bool
	// 服务端的指数价跳变保护(POST /index-price)：一次推送相对当前指数价的变动超过这个比例(0.05=5%)，
	// 先不写，要这个新价位持续IndexJumpConfirm之后才承认，见PushIndexPrice。<=0表示不校验，
	// 默认不校验：本地开发、模拟客户端的行情情景(故意大幅改价)、各种测试都不需要它，生产环境显式打开
	IndexMaxJump     decimal.Decimal
	IndexJumpConfirm time.Duration
}

func DefaultMarkPriceConfig() MarkPriceConfig {
	return MarkPriceConfig{
		MaxIndexAge:      30 * time.Second,
		MaxDeviation:     decimal.RequireFromString("0.01"),
		BasisWindow:      60 * time.Second,
		IndexJumpConfirm: 3 * time.Second,
	}
}

// 判断"这次推送是不是还在同一个待确认价位"的容差(1%)，跟index-feeder的离群阈值默认值一致
var indexJumpTolerance = decimal.RequireFromString("0.01")

// 标记价格服务。标记价格是强平、未实现盈亏、条件单触发、资金费率共同依赖的价格，如果直接用最新
// 成交价，谁能在我们的盘口上成交谁就能推动别人的强平线(两个账户对敲一笔就行)，所以不能单靠成交价：
//
//	标记价 = 中位数(指数价, 指数价×(1+基差均值), 最新成交价)，再夹在指数价±MaxDeviation之内
//
// 中位数的性质是任何一个输入被操纵都动不了结果：三个数的中位数一定落在其中任意两个数之间，
// 所以最新成交价再离谱，标记价也只会在"指数价"和"指数价加基差"这两个数之间。基差是我们盘口买一卖一
// 中价相对指数价的偏离、按时间窗口取平均，要拉动它得长时间持续地摆盘口。
//
// 标记价由拥有这个symbol的engine实例算出来写进Redis，其它进程(contract-api)只读。
type MarkPriceService struct {
	cache *cache.Cache
	cfg   MarkPriceConfig
	now   func() int64 // 毫秒时间戳，测试里可以替换

	// 标记价变化时的回调(engine用它推送标记价)，nil表示不需要
	onChange func(ctx context.Context, symbol string, mark decimal.Decimal)

	mu     sync.Mutex
	states map[string]*markState
}

// 单个symbol的计算状态。锁按symbol分开，不同symbol的成交不会互相排队
type markState struct {
	mu    sync.Mutex
	basis basisWindow
}

func NewMarkPriceService(c *cache.Cache) *MarkPriceService {
	return &MarkPriceService{cache: c, cfg: DefaultMarkPriceConfig(), now: NowMillis, states: make(map[string]*markState)}
}

func (s *MarkPriceService) WithConfig(cfg MarkPriceConfig) *MarkPriceService {
	s.cfg = cfg
	return s
}

// 换掉时间来源(毫秒时间戳)，只给测试用：指数价新鲜度和基差窗口都按它判断
func (s *MarkPriceService) WithClock(now func() int64) *MarkPriceService {
	s.now = now
	return s
}

// 标记价变化时调用fn，只在engine里设置
func (s *MarkPriceService) OnChange(fn func(ctx context.Context, symbol string, mark decimal.Decimal)) {
	s.onChange = fn
}

func (s *MarkPriceService) state(symbol string) *markState {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.states[symbol]
	if !ok {
		st = &markState{}
		s.states[symbol] = st
	}
	return st
}

// 返回(zero, false)表示这个symbol还没有标记价格，调用方要按"没有价格就跳过这一轮判断"处理，不能当0价格用。
// 返回的是最近一次算出来的标记价，喂价断了以后它会停在断之前的值——做风控判断(强平、条件单触发、资金费率)
// 不要用这个，用GetFresh
func (s *MarkPriceService) Get(ctx context.Context, symbol string) (decimal.Decimal, bool) {
	v, err := s.cache.GetMarkPrice(ctx, symbol)
	if err != nil || v == "" {
		return decimal.Zero, false
	}
	d, err := decimal.NewFromString(v)
	if err != nil {
		return decimal.Zero, false
	}
	return d, true
}

// 标记价的新鲜程度
type MarkState int

const (
	MarkMissing MarkState = iota // 没有标记价(新合约还没成交、或RequireIndex下还没喂过指数价)
	MarkFresh                    // 可以用于风控判断
	MarkStale                    // 有标记价，但指数价断供了，标记价已经过期
)

// 查标记价和它的新鲜程度。指数价陈旧(喂价断了)就是MarkStale；没喂过指数价时：RequireIndex=true
// 算MarkStale(生产环境不该出现，不能悄悄退回可被操纵的成交价)，否则按开发环境的
// "标记价=最新成交价"处理，算MarkFresh
func (s *MarkPriceService) Lookup(ctx context.Context, symbol string) (decimal.Decimal, MarkState) {
	mark, ok := s.Get(ctx, symbol)
	if !ok {
		return decimal.Zero, MarkMissing
	}
	_, ts, hasIndex := s.readIndex(ctx, symbol)
	if !hasIndex {
		if s.cfg.RequireIndex {
			return mark, MarkStale
		}
		return mark, MarkFresh
	}
	if s.indexStale(ts) {
		return mark, MarkStale
	}
	return mark, MarkFresh
}

// 风控判断用的标记价：喂价断了或没有标记价时返回false，调用方跳过这一轮。宁可少强平，也不能拿一个
// 已经过期的价格去误强平用户
func (s *MarkPriceService) GetFresh(ctx context.Context, symbol string) (decimal.Decimal, bool) {
	mark, state := s.Lookup(ctx, symbol)
	if state != MarkFresh {
		return decimal.Zero, false
	}
	return mark, true
}

func (s *MarkPriceService) indexStale(ts int64) bool {
	return s.now()-ts > s.cfg.MaxIndexAge.Milliseconds()
}

// 外部行情源推送这个symbol的指数价格。这个进程(contract-api)只存价格，标记价由engine按ticker重算
func (s *MarkPriceService) SetIndexPrice(ctx context.Context, symbol string, price decimal.Decimal) error {
	return s.cache.SetIndexPrice(ctx, symbol, price.String(), s.now())
}

// 指数价推送的结果
type IndexPushResult struct {
	Accepted bool
	// 没被接受时才有意义：当前生效的指数价，以及这个新价位已经持续了多久
	Current decimal.Decimal
	Waited  time.Duration
}

// POST /index-price走这个：在SetIndexPrice前面加一道跳变保护。接口本身没有别的校验，拿着ops密钥
// 的人可以推任意价格，这道保护限制的是"变动的速度"：
//
//   - 相对当前指数价变动不超过IndexMaxJump：直接写
//   - 超过：不写，把这个新价位记下来；后续推送持续落在同一价位(容差1%)满IndexJumpConfirm之后才承认。
//     期间任何一次落在正常范围内的推送(真实的喂价器每秒都在推)都会清掉这个待确认状态，
//     所以靠短时间内连推几次来"凑够确认"是行不通的
//   - 当前指数价不存在或已经陈旧：直接写。陈旧的价格不是可靠的参照，卡住它等于喂价断了以后
//     再也恢复不了；这也是保护的上限——真实的闪崩最多被拦到断供阈值(MaxIndexAge)为止
//
// 保护不了的：拿着密钥的人每次只挪一小步。所以密钥仍然要单独发、只给ops。
// 待确认状态放Redis(contract-api多实例)，读-判断-写不是原子的，但并发只可能出现在"同一个
// 喂价方"和"攻击者"之间，前者每秒清一次待确认状态，攻击者攒不出满IndexJumpConfirm的持续时间
func (s *MarkPriceService) PushIndexPrice(ctx context.Context, symbol string, price decimal.Decimal) (IndexPushResult, error) {
	if s.cfg.IndexMaxJump.Sign() <= 0 {
		return IndexPushResult{Accepted: true}, s.SetIndexPrice(ctx, symbol, price)
	}
	cur, curTs, hasCur := s.readIndex(ctx, symbol)
	pending, hasPending, err := s.cache.GetIndexJump(ctx, symbol)
	if err != nil {
		return IndexPushResult{}, err
	}
	now := s.now()
	accept, next, continued := decideIndexPush(s.cfg, now, price, cur, curTs, hasCur, pending, hasPending)
	if !accept {
		if err := s.cache.SetIndexJump(ctx, symbol, next); err != nil {
			return IndexPushResult{}, err
		}
		if !continued {
			log.Printf("[WARN] 指数价跳变被服务端保护拦下, 等待确认, symbol=%s, 当前=%s, 推送=%s, 阈值=%s",
				symbol, cur, price, s.cfg.IndexMaxJump)
		}
		return IndexPushResult{Current: cur, Waited: time.Duration(now-next.FirstTs) * time.Millisecond}, nil
	}
	if err := s.SetIndexPrice(ctx, symbol, price); err != nil {
		return IndexPushResult{}, err
	}
	if hasPending {
		if err := s.cache.ClearIndexJump(ctx, symbol); err != nil {
			// 价格已经写进去了。残留的待确认状态无害：下一次落在正常范围内的推送会再清一次
			log.Printf("[WARN] 清理待确认的指数价跳变失败, symbol=%s: %v", symbol, err)
		}
	}
	if continued {
		log.Printf("[WARN] 指数价大幅跳变已确认, symbol=%s, %s -> %s", symbol, cur, price)
	}
	return IndexPushResult{Accepted: true}, nil
}

// PushIndexPrice的判断部分，不碰Redis也不读时钟，方便单测。
// accept=true写入；false则不写，要把next记为待确认状态。continued=true表示这次推送延续了已有的待确认
// 价位(而不是新起一个价位)——调用方用它决定要不要打日志，避免持续的推送每次都打一条
func decideIndexPush(cfg MarkPriceConfig, now int64, price, cur decimal.Decimal, curTs int64, hasCur bool,
	pending cache.IndexJump, hasPending bool) (accept bool, next cache.IndexJump, continued bool) {
	maxAge := cfg.MaxIndexAge.Milliseconds()
	if cfg.IndexMaxJump.Sign() <= 0 || !hasCur || now-curTs > maxAge ||
		price.Sub(cur).Abs().Div(cur).LessThanOrEqual(cfg.IndexMaxJump) {
		return true, cache.IndexJump{}, false
	}
	if hasPending && now-pending.LastTs <= maxAge {
		if target, err := decimal.NewFromString(pending.Target); err == nil && target.Sign() > 0 &&
			price.Sub(target).Abs().Div(target).LessThanOrEqual(indexJumpTolerance) {
			continued = true
		}
	}
	if continued {
		next = pending
		next.LastTs = now
	} else {
		next = cache.IndexJump{Target: price.String(), FirstTs: now, LastTs: now}
	}
	if now-next.FirstTs >= cfg.IndexJumpConfirm.Milliseconds() {
		return true, cache.IndexJump{}, continued
	}
	return false, next, continued
}

// 返回(zero, false)表示这个symbol还没有任何外部行情源喂过指数价格。不管陈不陈旧都返回，
// 调用方要判断新鲜度的话用GetFresh
func (s *MarkPriceService) GetIndexPrice(ctx context.Context, symbol string) (decimal.Decimal, bool) {
	p, _, ok := s.readIndex(ctx, symbol)
	return p, ok
}

func (s *MarkPriceService) readIndex(ctx context.Context, symbol string) (decimal.Decimal, int64, bool) {
	v, ts, err := s.cache.GetIndexPrice(ctx, symbol)
	if err != nil || v == "" {
		return decimal.Zero, 0, false
	}
	d, err := decimal.NewFromString(v)
	if err != nil || d.Sign() <= 0 {
		return decimal.Zero, 0, false
	}
	return d, ts, true
}

// 记录一笔成交价并重算标记价。最新成交价只是标记价的三个输入之一
func (s *MarkPriceService) UpdateFromTrade(ctx context.Context, symbol string, price decimal.Decimal) error {
	st := s.state(symbol)
	st.mu.Lock()
	defer st.mu.Unlock()
	if err := s.cache.SetLastTradePrice(ctx, symbol, price.String()); err != nil {
		return err
	}
	return s.recompute(ctx, symbol, st, decimal.Zero, decimal.Zero)
}

// 定时刷新：用当前盘口的买一卖一采一个基差样本，然后重算标记价。指数价变了、成交没有变的时候，
// 标记价靠这个跟上。买一或卖一缺失(单边盘口)传zero，这一轮不采样
func (s *MarkPriceService) Refresh(ctx context.Context, symbol string, bestBid, bestAsk decimal.Decimal) error {
	st := s.state(symbol)
	st.mu.Lock()
	defer st.mu.Unlock()
	return s.recompute(ctx, symbol, st, bestBid, bestAsk)
}

// 调用方必须持有st.mu
func (s *MarkPriceService) recompute(ctx context.Context, symbol string, st *markState, bestBid, bestAsk decimal.Decimal) error {
	last, hasLast, err := s.readLast(ctx, symbol)
	if err != nil {
		return err
	}
	index, ts, hasIndex := s.readIndex(ctx, symbol)

	var mark decimal.Decimal
	switch {
	case !hasIndex:
		if s.cfg.RequireIndex || !hasLast {
			return nil
		}
		mark = last // 开发环境退路：没有行情源，只能用最新成交价
	case s.indexStale(ts):
		return nil // 喂价断了：标记价冻结在断之前的值，不拿过期的指数价去算，见GetFresh
	default:
		now := s.now()
		if sample, ok := basisSample(index, bestBid, bestAsk, s.cfg.MaxDeviation); ok {
			st.basis.add(now, sample)
		}
		mark = computeMark(index, st.basis.average(now, s.cfg.BasisWindow), last, hasLast, s.cfg.MaxDeviation)
	}

	prev, hadPrev := s.Get(ctx, symbol)
	if hadPrev && prev.Equal(mark) {
		return nil
	}
	if err := s.cache.SetMarkPrice(ctx, symbol, mark.String()); err != nil {
		return err
	}
	if s.onChange != nil {
		s.onChange(ctx, symbol, mark)
	}
	return nil
}

func (s *MarkPriceService) readLast(ctx context.Context, symbol string) (decimal.Decimal, bool, error) {
	v, err := s.cache.GetLastTradePrice(ctx, symbol)
	if err != nil {
		return decimal.Zero, false, err
	}
	if v == "" {
		return decimal.Zero, false, nil
	}
	d, err := decimal.NewFromString(v)
	if err != nil || d.Sign() <= 0 {
		log.Printf("[WARN] 最新成交价数据损坏, symbol=%s, value=%q", symbol, v)
		return decimal.Zero, false, nil
	}
	return d, true, nil
}

// 三个数的中位数
func median3(a, b, c decimal.Decimal) decimal.Decimal {
	v := []decimal.Decimal{a, b, c}
	sort.Slice(v, func(i, j int) bool { return v[i].LessThan(v[j]) })
	return v[1]
}

// 把x夹在[lo, hi]之内
func clampDecimal(x, lo, hi decimal.Decimal) decimal.Decimal {
	if x.LessThan(lo) {
		return lo
	}
	if x.GreaterThan(hi) {
		return hi
	}
	return x
}

// 标记价 = 中位数(指数价, 指数价×(1+基差均值), 最新成交价)，夹在指数价±maxDev之内(maxDev<=0不夹)。
// basis是基差比例((盘口中价-指数价)/指数价)的均值，没有最新成交价时用指数价加基差。
// 夹在指数价±maxDev之内在数学上是冗余的(基差已经先夹过了，而中位数一定落在指数价和指数价加基差之间)，
// 留着是为了以后改公式时不会悄悄丢掉这个上限
func computeMark(index, basis, last decimal.Decimal, hasLast bool, maxDev decimal.Decimal) decimal.Decimal {
	one := decimal.NewFromInt(1)
	if maxDev.Sign() > 0 {
		basis = clampDecimal(basis, maxDev.Neg(), maxDev)
	}
	adjusted := index.Mul(one.Add(basis))
	mark := adjusted
	if hasLast {
		mark = median3(index, adjusted, last)
	}
	if maxDev.Sign() > 0 {
		mark = clampDecimal(mark, index.Mul(one.Sub(maxDev)), index.Mul(one.Add(maxDev)))
	}
	return mark
}

// 用当前买一卖一算一个基差样本((中价-指数价)/指数价)。返回false=这一轮不采样：
//   - 单边盘口(没有买一或卖一)、买卖倒挂
//   - 买卖价差比允许的最大偏离的两倍还宽：盘口太薄，中价没有意义，采进去只会让人靠撤掉一边的
//     挂单来控制基差
//
// 样本本身也夹在±maxDev之内，避免一个极端样本把窗口平均带偏太多
func basisSample(index, bid, ask, maxDev decimal.Decimal) (decimal.Decimal, bool) {
	if index.Sign() <= 0 || bid.Sign() <= 0 || ask.Sign() <= 0 || ask.LessThan(bid) {
		return decimal.Zero, false
	}
	mid := bid.Add(ask).Div(decimal.NewFromInt(2))
	if maxDev.Sign() > 0 && ask.Sub(bid).Div(mid).GreaterThan(maxDev.Mul(decimal.NewFromInt(2))) {
		return decimal.Zero, false
	}
	sample := mid.Sub(index).Div(index)
	if maxDev.Sign() > 0 {
		sample = clampDecimal(sample, maxDev.Neg(), maxDev)
	}
	return sample, true
}

// 基差样本的滑动时间窗口
type basisWindow struct {
	samples []basisPoint
}

type basisPoint struct {
	ts    int64
	ratio decimal.Decimal
}

func (w *basisWindow) add(now int64, ratio decimal.Decimal) {
	w.samples = append(w.samples, basisPoint{ts: now, ratio: ratio})
}

// 窗口内样本的平均，窗口外的顺手丢掉。一个样本都没有(刚启动、盘口一直是单边)返回0，
// 也就是标记价退回指数价——没有依据的时候往外部真实价格靠
func (w *basisWindow) average(now int64, window time.Duration) decimal.Decimal {
	cutoff := now - window.Milliseconds()
	i := 0
	for i < len(w.samples) && w.samples[i].ts < cutoff {
		i++
	}
	w.samples = w.samples[i:]
	if len(w.samples) == 0 {
		return decimal.Zero
	}
	sum := decimal.Zero
	for _, p := range w.samples {
		sum = sum.Add(p.ratio)
	}
	return sum.Div(decimal.NewFromInt(int64(len(w.samples))))
}
