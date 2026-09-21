//go:build integration

package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"perp-go/internal/service"
	"perp-go/internal/testutil"
)

// POST /index-price的服务端跳变保护，走真实Redis(待确认状态存在Redis里)。判断逻辑的边界在单元测试里
// (TestDecideIndexPush_*)，这里验证接上Redis之后的整体行为，重点是"喂价方持续在推时攻击者攒不出确认时间"。
// 只需要Redis，不需要MySQL。时钟是手动拨的：阈值5%，确认3秒，断供30秒

type pushEnv struct {
	svc   *service.MarkPriceService
	clock *fakeClock
}

func newPushEnv(t *testing.T, maxJump string) *pushEnv {
	t.Helper()
	testutil.ResetPriceKeys(t, testSymbol, "ETHUSDT")
	cfg := service.DefaultMarkPriceConfig()
	if maxJump != "" {
		cfg.IndexMaxJump = decimal.RequireFromString(maxJump)
	}
	clock := newFakeClock()
	svc := service.NewMarkPriceService(testutil.NewCache(t)).WithConfig(cfg).WithClock(clock.now)
	return &pushEnv{svc: svc, clock: clock}
}

func (e *pushEnv) push(t *testing.T, symbol, price string) service.IndexPushResult {
	t.Helper()
	res, err := e.svc.PushIndexPrice(context.Background(), symbol, decimalOf(t, price))
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func (e *pushEnv) index(t *testing.T, symbol string) string {
	t.Helper()
	p, ok := e.svc.GetIndexPrice(context.Background(), symbol)
	if !ok {
		t.Fatalf("%s应该有指数价", symbol)
	}
	return p.String()
}

// 正常波动直接写，不留任何待确认状态
func TestPushIndexPrice_NormalMovesPassThrough(t *testing.T) {
	e := newPushEnv(t, "0.05")
	for _, p := range []string{"60000", "60500", "59900", "62895"} { // 最后一步相对59900恰好+5%(59900*1.05=62895)
		if res := e.push(t, testSymbol, p); !res.Accepted {
			t.Fatalf("%s应该直接写", p)
		}
		if e.index(t, testSymbol) != p {
			t.Fatalf("指数价 = %s, want %s", e.index(t, testSymbol), p)
		}
		e.clock.advance(time.Second)
	}
}

// 一步跳变：不写、报告当前值和已持续的时间；同一价位持续满3秒才承认，之后回到正常
func TestPushIndexPrice_BigJumpHeldUntilConfirmed(t *testing.T) {
	e := newPushEnv(t, "0.05")
	e.push(t, testSymbol, "60000")
	e.clock.advance(time.Second)

	res := e.push(t, testSymbol, "70000") // +16.7%
	if res.Accepted || !res.Current.Equal(decimalOf(t, "60000")) || res.Waited != 0 {
		t.Fatalf("第一次推送应该被拦下: %+v", res)
	}
	if e.index(t, testSymbol) != "60000" {
		t.Fatal("被拦下的推送不能写进去")
	}
	for i, wantWaited := range []time.Duration{time.Second, 2 * time.Second} {
		e.clock.advance(time.Second)
		res = e.push(t, testSymbol, "70050") // 容差1%以内的小波动仍算同一价位
		if res.Accepted || res.Waited != wantWaited {
			t.Fatalf("第%d次续推: %+v, want 拦下且已持续%s", i+2, res, wantWaited)
		}
	}
	if e.index(t, testSymbol) != "60000" {
		t.Fatal("确认时间没到，指数价不能变")
	}
	e.clock.advance(time.Second) // 持续满3秒
	if res = e.push(t, testSymbol, "70020"); !res.Accepted {
		t.Fatalf("持续3秒应该承认: %+v", res)
	}
	if e.index(t, testSymbol) != "70020" {
		t.Fatalf("指数价 = %s, want 70020", e.index(t, testSymbol))
	}

	// 承认之后待确认状态清掉了：紧接着又一次大跳变要重新等，不能沿用上一次的时间
	e.clock.advance(time.Second)
	if res = e.push(t, testSymbol, "90000"); res.Accepted || res.Waited != 0 {
		t.Fatalf("新的跳变要重新起头: %+v", res)
	}
}

// 关键性质：真正的行情源(orderbook-sync)每秒都在推正常价格，每一次都会清掉待确认状态，攻击者攒不出连续3秒。
// 每个"秒"里攻击者推离谱价格、行情源推正常价格，跑20秒，离谱价格一次都进不去
func TestPushIndexPrice_LegitFeedKeepsAttackerFromConfirming(t *testing.T) {
	e := newPushEnv(t, "0.05")
	e.push(t, testSymbol, "60000")
	for sec := 0; sec < 20; sec++ {
		e.clock.advance(time.Second)
		if res := e.push(t, testSymbol, "90000"); res.Accepted {
			t.Fatalf("第%d秒：攻击者的价格不该被承认", sec)
		}
		e.clock.advance(500 * time.Millisecond)
		if res := e.push(t, testSymbol, "60010"); !res.Accepted { // 真实喂价
			t.Fatalf("第%d秒：正常价格应该直接写", sec)
		}
	}
	if e.index(t, testSymbol) != "60010" {
		t.Fatalf("指数价 = %s", e.index(t, testSymbol))
	}
}

// 另一种打法：同一时刻连推多次，凑"次数"。确认看的是持续时间，不是次数
func TestPushIndexPrice_BurstOfPushesDoesNotConfirm(t *testing.T) {
	e := newPushEnv(t, "0.05")
	e.push(t, testSymbol, "60000")
	for i := 0; i < 10; i++ {
		if res := e.push(t, testSymbol, "90000"); res.Accepted || res.Waited != 0 {
			t.Fatalf("同一时刻第%d次: %+v", i+1, res)
		}
	}
	if e.index(t, testSymbol) != "60000" {
		t.Fatal("指数价不能变")
	}
}

// 期间价位又变了：重新计时
func TestPushIndexPrice_LevelChangeRestartsTheClock(t *testing.T) {
	e := newPushEnv(t, "0.05")
	e.push(t, testSymbol, "60000")
	e.push(t, testSymbol, "70000")
	e.clock.advance(2 * time.Second)
	if res := e.push(t, testSymbol, "80000"); res.Accepted || res.Waited != 0 { // 新价位从0开始
		t.Fatalf("换了价位要重新计时: %+v", res)
	}
	e.clock.advance(2 * time.Second) // 距离80000起头才2秒，距离70000起头已经4秒
	if res := e.push(t, testSymbol, "80000"); res.Accepted || res.Waited != 2*time.Second {
		t.Fatalf("不能继承70000的计时: %+v", res)
	}
}

// 当前指数价陈旧(喂价断了超过30秒)：任何价格都直接写，不然喂价断了以后再也恢复不了。
// 保护的上限就是这里：真实的闪崩最多被拦到断供阈值
func TestPushIndexPrice_StaleIndexAcceptsAnyPrice(t *testing.T) {
	e := newPushEnv(t, "0.05")
	e.push(t, testSymbol, "60000")
	e.clock.advance(30 * time.Second)
	if res := e.push(t, testSymbol, "120000"); res.Accepted {
		t.Fatal("恰好30秒还不算陈旧，要拦")
	}
	e.clock.advance(time.Second)
	if res := e.push(t, testSymbol, "120000"); !res.Accepted {
		t.Fatal("陈旧之后应该直接写")
	}
	if e.index(t, testSymbol) != "120000" {
		t.Fatalf("指数价 = %s", e.index(t, testSymbol))
	}
}

// 第一次喂价(还没有指数价)：不拦
func TestPushIndexPrice_FirstPushIsNotGuarded(t *testing.T) {
	e := newPushEnv(t, "0.05")
	if res := e.push(t, testSymbol, "60000"); !res.Accepted {
		t.Fatal("第一次喂价没有参照，应该直接写")
	}
}

// 不同合约的待确认状态互不影响
func TestPushIndexPrice_SymbolsAreIndependent(t *testing.T) {
	e := newPushEnv(t, "0.05")
	e.push(t, testSymbol, "60000")
	e.push(t, "ETHUSDT", "3000")
	e.push(t, testSymbol, "90000") // BTC起头
	e.clock.advance(3 * time.Second)
	if res := e.push(t, "ETHUSDT", "3600"); res.Accepted || res.Waited != 0 {
		t.Fatalf("ETH的跳变不能借用BTC的计时: %+v", res)
	}
	if res := e.push(t, testSymbol, "90000"); !res.Accepted {
		t.Fatal("BTC自己持续了3秒，应该承认")
	}
	if e.index(t, "ETHUSDT") != "3000" {
		t.Fatal("ETH的指数价不能变")
	}
}

// 默认(阈值不设)：完全不校验，行为跟以前的SetIndexPrice一样
func TestPushIndexPrice_DisabledByDefault(t *testing.T) {
	e := newPushEnv(t, "")
	e.push(t, testSymbol, "60000")
	if res := e.push(t, testSymbol, "600000"); !res.Accepted {
		t.Fatal("没开保护时任何价格都直接写")
	}
	if e.index(t, testSymbol) != "600000" {
		t.Fatalf("指数价 = %s", e.index(t, testSymbol))
	}
}

// 承认了跳变之后，标记价跟着新指数价走(保护只管指数价什么时候写进去，不改变下游)
func TestPushIndexPrice_ConfirmedJumpFlowsIntoMarkPrice(t *testing.T) {
	e := newPushEnv(t, "0.05")
	ctx := context.Background()
	e.push(t, testSymbol, "60000")
	if err := e.svc.Refresh(ctx, testSymbol, decimal.Zero, decimal.Zero); err != nil {
		t.Fatal(err)
	}
	e.clock.advance(time.Second)
	e.push(t, testSymbol, "70000")
	if err := e.svc.Refresh(ctx, testSymbol, decimal.Zero, decimal.Zero); err != nil {
		t.Fatal(err)
	}
	if m, _ := e.svc.Get(ctx, testSymbol); !m.Equal(decimalOf(t, "60000")) {
		t.Fatalf("被拦下期间标记价不能动: %s", m)
	}
	e.clock.advance(3 * time.Second)
	e.push(t, testSymbol, "70000")
	if err := e.svc.Refresh(ctx, testSymbol, decimal.Zero, decimal.Zero); err != nil {
		t.Fatal(err)
	}
	if m, _ := e.svc.Get(ctx, testSymbol); !m.Equal(decimalOf(t, "70000")) {
		t.Fatalf("承认之后标记价 = %s, want 70000", m)
	}
}
