package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/binancefeed"
	"perp-go/internal/model"
	"perp-go/internal/repo"
)

// UID 镜像挂单用的系统账户，固定值、不需要配置：它不对外暴露、不需要按部署环境区分，
// 就是"系统"这个角色在数据库里的一个记账标识，一个账户就能同时持有多空两个方向的仓位
// (positions表按uid+symbol+side存)。选一个不会跟真实合作方uid撞的号段
const UID uint64 = 9000000

// MirrorConfig 配置这个contract-engine实例要把币安订单簿镜像哪些合约、多少档、多快。
type MirrorConfig struct {
	Symbols  []string
	Levels   int           // 每侧镜像币安盘口的前多少档
	Interval time.Duration // 每一轮的间隔
	Leverage int           // 镜像挂单的杠杆。全部保证金分档里最低的最大杠杆是5，用5在哪个档位都不会被拒
	// 币安数据超过这么久没拉成功，就撤掉这个合约的全部镜像挂单、暂停报价：过期的报价留在订单簿里，
	// 谁比我们更早看到币安的价格，谁就能按旧价成交
	StaleAfter time.Duration
}

func (c MirrorConfig) validate() error {
	switch {
	case len(c.Symbols) == 0:
		return errors.New("Symbols不能为空")
	case c.Levels <= 0:
		return errors.New("Levels必须大于0")
	case c.Interval <= 0 || c.StaleAfter <= 0:
		return errors.New("Interval和StaleAfter必须大于0")
	case c.Leverage <= 0:
		return errors.New("Leverage必须大于0")
	}
	return nil
}

type mirrorContractInfo struct {
	qtyDP      int32           // 数量的小数位数
	volumeStep decimal.Decimal // 数量步长，0=合约不校验
	minVolume  decimal.Decimal // 最小下单量
}

// 每个合约的运行状态，只被这个合约自己的goroutine读写，不需要锁
type mirrorSymState struct {
	lastOK   time.Time // 上一次成功拉到币安盘口的时间
	stopped  bool      // 已经因为币安数据过期撤了全部镜像挂单
	lastWarn time.Time
}

// MirrorService 把币安的订单簿直接镜像成系统账户在撮合引擎里的真实挂单。
//
// 跟早期版本(独立的orderbook-sync进程，通过公开下单/撤单API+分布式锁挂单)的关键区别：这里是
// contract-engine进程内部直接调用AccountService.FreezeMargin/OrderRepo/EngineService.SubmitOrder，
// 不经HTTP、不经Kafka、不需要service.LockService分布式锁——镜像挂单从来只有这一个来源(这个进程
// 自己的这一个goroutine)，不存在"多进程/多实例抢同一个uid+symbol+side"的并发场景，
// EngineService内部已经靠matching.Book自己的锁保证了并发安全，套用为多进程设计的分布式锁纯属
// 多余开销。撤单也是同步生效(函数调用直接改内存订单簿+落库)，不再需要"发出去、轮询等生效"，
// 因此不再需要旧版本里"先挂新的、等旧的真撤了再补"的复杂编排：新挂单如果恰好跟本账户自己还没
// 撤掉的旧挂单交叉，matching.Book的自成交保护(STP)会自动把那笔旧挂单摘掉而不产生成交，等价于
// 帮我们兜底了"不能自己的多空撮合成交"这条要求。见docs/orderbook-sync.md。
type MirrorService struct {
	cfg        MirrorConfig
	bn         *binancefeed.Binance
	depthLimit int
	accounts   *AccountService
	orders     *repo.OrderRepo
	coins      *repo.CoinRepo
	engine     *EngineService
	now        func() time.Time // 测试里可以替换

	startedAt time.Time
	ready     bool
	contracts map[string]mirrorContractInfo
	states    map[string]*mirrorSymState
}

func NewMirrorService(cfg MirrorConfig, bn *binancefeed.Binance, accounts *AccountService, orders *repo.OrderRepo, coins *repo.CoinRepo, engine *EngineService) (*MirrorService, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	limit, ok := binancefeed.DepthLimitFor(cfg.Levels)
	if !ok {
		return nil, fmt.Errorf("Levels=%d超过币安深度接口的上限1000", cfg.Levels)
	}
	m := &MirrorService{
		cfg: cfg, bn: bn, depthLimit: limit, accounts: accounts, orders: orders, coins: coins, engine: engine, now: time.Now,
		contracts: map[string]mirrorContractInfo{}, states: map[string]*mirrorSymState{},
	}
	for _, sym := range cfg.Symbols {
		m.states[sym] = &mirrorSymState{}
	}
	m.startedAt = m.now()
	return m, nil
}

// 定时跑，直到ctx结束
func (m *MirrorService) Run(ctx context.Context) {
	t := time.NewTicker(m.cfg.Interval)
	defer t.Stop()
	m.Step(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.Step(ctx)
		}
	}
}

// 跑一轮：账户和合约参数没准备好就先准备，然后每个自己负责的合约并行镜像一次(并行是为了不让
// 多个合约的币安HTTP请求互相排队拖慢整体延迟；下单/撤单本身是进程内调用，很快，并发对它们
// 没有性能意义，只是跟着深度拉取一起并行)
func (m *MirrorService) Step(ctx context.Context) {
	if !m.ready {
		if err := m.ensureReady(ctx); err != nil {
			log.Printf("[ERROR] 订单簿镜像: 准备系统账户/合约参数失败: %v", err)
			return
		}
		m.ready = true
	}
	var wg sync.WaitGroup
	for _, sym := range m.cfg.Symbols {
		if !m.engine.OwnsSymbol(sym) {
			continue // 分片部署下这个symbol的真实订单簿不归这个实例负责，见docs/engine-sharding.md
		}
		wg.Add(1)
		go func(sym string) {
			defer wg.Done()
			if err := m.cycle(ctx, sym); err != nil {
				m.warn(m.states[sym], "[WARN] 订单簿镜像失败, symbol=%s: %v", sym, err)
			}
		}(sym)
	}
	wg.Wait()
}

// 建好系统账户(已经存在就是no-op)、查好每个合约的数量精度和最小下单量。系统账户不需要
// 充值/维护余额——AccountService.FreezeMargin对这个uid有无条件成功的特殊路径，
// balance/credit只是跟着真实成交自然变化的记账值，不影响能不能挂单
func (m *MirrorService) ensureReady(ctx context.Context) error {
	if _, _, err := m.accounts.Create(ctx, UID); err != nil {
		return fmt.Errorf("创建系统账户%d: %w", UID, err)
	}
	for _, sym := range m.cfg.Symbols {
		coin, err := m.coins.FindBySymbol(ctx, sym)
		if err != nil {
			return fmt.Errorf("查合约%s参数: %w", sym, err)
		}
		if coin == nil {
			return fmt.Errorf("合约%s不存在，检查PERP_MIRROR_SYMBOLS/PERP_ENGINE_SYMBOLS和coins表是否配置一致", sym)
		}
		m.contracts[sym] = mirrorContractInfo{qtyDP: coin.BaseCoinScale, volumeStep: coin.VolumeStep, minVolume: coin.MinVolume}
	}
	return nil
}

func (m *MirrorService) cycle(ctx context.Context, sym string) error {
	st := m.states[sym]
	bids, asks, err := m.bn.Depth(ctx, sym, m.depthLimit)
	if err != nil {
		return m.sourceFailed(ctx, sym, st, err)
	}
	ci := m.contracts[sym]
	wantBids := binancefeed.DesiredLevels(bids, m.cfg.Levels, ci.qtyDP, ci.volumeStep, ci.minVolume)
	wantAsks := binancefeed.DesiredLevels(asks, m.cfg.Levels, ci.qtyDP, ci.volumeStep, ci.minVolume)
	if len(wantBids) == 0 || len(wantAsks) == 0 {
		// 币安的盘口有数据，但按我们合约的数量精度取整后一侧没有可挂的档位：不能只挂一侧
		return m.sourceFailed(ctx, sym, st, errors.New("按合约的数量精度取整后，盘口一侧没有可挂的档位"))
	}
	st.lastOK = m.now()
	if st.stopped {
		st.stopped = false
		log.Printf("[INFO] 币安数据恢复，重新镜像订单簿, symbol=%s", sym)
	}
	return m.syncLadders(ctx, sym, wantBids, wantAsks)
}

// 拿不到可信的币安盘口：短时间内不动已经挂着的镜像单(可能只是一次抖动)，超过StaleAfter就全部撤掉、暂停报价
func (m *MirrorService) sourceFailed(ctx context.Context, sym string, st *mirrorSymState, cause error) error {
	since := st.lastOK
	if since.IsZero() {
		since = m.startedAt
	}
	if m.now().Sub(since) <= m.cfg.StaleAfter {
		return fmt.Errorf("拿不到币安盘口(镜像挂单暂时不动): %w", cause)
	}
	if err := m.cancelAllLive(ctx, sym); err != nil {
		return fmt.Errorf("币安数据已过期但撤单失败: %w", err)
	}
	if !st.stopped {
		st.stopped = true
		log.Printf("[ERROR] 币安数据已经超过%s没更新，已撤掉全部镜像挂单、暂停报价, symbol=%s: %v", m.cfg.StaleAfter, sym, cause)
	}
	return nil
}

// 让这个合约上的镜像挂单跟币安的盘口一致。挂单/撤单都是进程内同步调用，函数返回就是生效的，
// 不需要旧版本那套"先挂新的、等旧的真撤了再补"的编排：即使新挂单跟本账户自己还没撤掉的旧挂单
// 撞上了，matching.Book的自成交保护会自动摘掉那笔旧挂单(不产生成交)，等价于帮我们兜底了
// "不能自己的多空撮合成交"这条要求，所以这里可以先统一挂新单、再统一撤旧单，顺序不影响正确性。
func (m *MirrorService) syncLadders(ctx context.Context, sym string, bids, asks []binancefeed.Level) error {
	live, err := m.liveOrders(ctx, sym)
	if err != nil {
		return err
	}
	var existingBids, existingAsks []binancefeed.LiveOrder
	for _, o := range live {
		if o.Side == model.SideLong {
			existingBids = append(existingBids, toLiveOrder(o))
		} else {
			existingAsks = append(existingAsks, toLiveOrder(o))
		}
	}
	cancelBids, placeBids := binancefeed.Plan(bids, existingBids)
	cancelAsks, placeAsks := binancefeed.Plan(asks, existingAsks)

	// 买卖两边并发挂单，不要串行：串行的话一侧要等另一侧全部挂完才开始挂，冷启动时每侧有几十档，
	// 页面会先看到一侧、隔好一会儿才看到另一侧——每笔挂单只要真的挂进订单簿就会实时推一次深度快照
	// (EngineService.submitOrder)。两个goroutine挂的是同一个symbol的不同side，NextID()全程持锁、
	// matching.Book自己的锁序列化真正的撮合/挂单、FreezeUnconditional是一条原子的UPDATE，
	// 并发是安全的，见本文件顶部doc注释
	var wg sync.WaitGroup
	var bidErr, askErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		for _, p := range placeBids {
			if err := m.placeOrder(ctx, sym, model.SideLong, p); err != nil {
				bidErr = fmt.Errorf("挂买单失败, symbol=%s price=%s: %w", sym, p.Price, err)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		for _, p := range placeAsks {
			if err := m.placeOrder(ctx, sym, model.SideShort, p); err != nil {
				askErr = fmt.Errorf("挂卖单失败, symbol=%s price=%s: %w", sym, p.Price, err)
				return
			}
		}
	}()
	wg.Wait()
	if bidErr != nil {
		return bidErr
	}
	if askErr != nil {
		return askErr
	}
	byID := make(map[uint64]*model.Order, len(live))
	for i := range live {
		byID[live[i].OrderID] = &live[i]
	}
	for _, id := range append(append([]uint64(nil), cancelBids...), cancelAsks...) {
		o, ok := byID[id]
		if !ok {
			continue // 理论不会发生：id来自liveOrders查出的同一批live
		}
		if err := m.engine.CancelOrder(ctx, o); err != nil {
			return fmt.Errorf("撤单失败, symbol=%s orderId=%d: %w", sym, id, err)
		}
	}
	return nil
}

func (m *MirrorService) placeOrder(ctx context.Context, sym string, side model.Side, p binancefeed.Placement) error {
	leverage := decimal.NewFromInt(int64(m.cfg.Leverage))
	requiredMargin := p.Qty.Mul(p.Price).Div(leverage)
	freeze, err := m.accounts.FreezeMargin(ctx, UID, requiredMargin)
	if err != nil {
		return fmt.Errorf("冻结保证金失败: %w", err)
	}
	now := NowMillis()
	o := &model.Order{
		OrderID:      NextID(),
		UID:          UID,
		Symbol:       sym,
		Side:         side,
		Action:       model.ActionOpen,
		Type:         model.OrderTypeLimit,
		Price:        p.Price,
		Amount:       p.Qty,
		Leverage:     uint32(m.cfg.Leverage),
		Status:       model.OrderStatusOpen,
		CreateTime:   now,
		UpdateTime:   now,
		FrozenMargin: freeze.FromAvailable,
		FrozenCredit: freeze.FromCredit,
	}
	if err := m.orders.Insert(ctx, o); err != nil {
		return fmt.Errorf("落库失败: %w", err)
	}
	return m.engine.SubmitOrder(ctx, o, time.Now().UnixNano())
}

// 撤掉这个合约上系统账户的全部镜像挂单
func (m *MirrorService) cancelAllLive(ctx context.Context, sym string) error {
	live, err := m.liveOrders(ctx, sym)
	if err != nil {
		return err
	}
	for i := range live {
		if err := m.engine.CancelOrder(ctx, &live[i]); err != nil {
			return err
		}
	}
	return nil
}

// 系统账户在这个合约上还挂着的限价开仓单(镜像挂单只会是这一种：开仓限价单)
func (m *MirrorService) liveOrders(ctx context.Context, sym string) ([]model.Order, error) {
	orders, err := m.orders.FindActiveByUID(ctx, UID, sym)
	if err != nil {
		return nil, err
	}
	out := orders[:0]
	for _, o := range orders {
		if o.Action == model.ActionOpen && o.Type == model.OrderTypeLimit {
			out = append(out, o)
		}
	}
	return out, nil
}

// 日志限流：同一个合约的同一类问题每30秒最多一条，镜像每秒一轮，出问题时不能刷屏
func (m *MirrorService) warn(st *mirrorSymState, format string, args ...any) {
	if !st.lastWarn.IsZero() && m.now().Sub(st.lastWarn) < 30*time.Second {
		return
	}
	st.lastWarn = m.now()
	log.Printf(format, args...)
}

// 把一笔真实的model.Order转成binancefeed.Plan需要的通用形状
func toLiveOrder(o model.Order) binancefeed.LiveOrder {
	return binancefeed.LiveOrder{ID: o.OrderID, Price: o.Price, Amount: o.Amount, Remaining: o.RemainingAmount()}
}
