package indexfeed

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// 手动拨的时钟：状态和健康检查按它判断，测试里不用真的等
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// 带手动时钟的喂价器，健康阈值15秒(默认值)
func newClockedFeeder(pub Publisher, srcs ...Source) (*Feeder, *fakeClock) {
	clock := &fakeClock{t: time.UnixMilli(1_700_000_000_000)}
	f := newFeeder(pub, srcs...)
	f.now = clock.now
	f.startedAt = clock.now()
	return f, clock
}

func symbolStatus(t *testing.T, f *Feeder, sym string) SymbolStatus {
	t.Helper()
	for _, s := range f.Status().Symbols {
		if s.Symbol == sym {
			return s
		}
	}
	t.Fatalf("状态里没有%s", sym)
	return SymbolStatus{}
}

// 刚启动、还没来得及发布：不算不健康(不然每次重启都会误报)；但超过阈值还没发布成功就不健康了
func TestStatus_NeverPublishedIsHealthyOnlyWithinGracePeriod(t *testing.T) {
	a, b := &fakeSource{name: "a"}, &fakeSource{name: "b"}
	f, clock := newClockedFeeder(&fakePublisher{}, a, b)

	st := f.Status()
	if !st.Healthy || !st.Symbols[0].Healthy {
		t.Fatal("刚启动应该算健康")
	}
	if st.Symbols[0].PublishAgeMs != -1 || st.Symbols[0].LastPrice != "" || st.Symbols[0].LastPublishAt != 0 {
		t.Fatalf("没发布过: %+v", st.Symbols[0])
	}
	clock.advance(15 * time.Second) // 恰好等于阈值：还算健康
	if !f.Status().Healthy {
		t.Fatal("恰好15秒还没超过阈值")
	}
	clock.advance(time.Millisecond)
	if f.Status().Healthy {
		t.Fatal("超过15秒一次都没发布成功，应该不健康")
	}
}

// 成功发布之后的状态字段，以及"多久没发布成功"的健康判断
func TestStatus_PublishedFieldsAndStaleness(t *testing.T) {
	a, b, c := &fakeSource{name: "a"}, &fakeSource{name: "b"}, &fakeSource{name: "c"}
	pub := &fakePublisher{}
	f, clock := newClockedFeeder(pub, a, b, c)
	setAll("BTCUSDT", "60000", a, b, c)
	f.Step(context.Background())

	s := symbolStatus(t, f, "BTCUSDT")
	if s.LastPrice != "60000" || s.LastPublishAt != clock.now().UnixMilli() || s.PublishAgeMs != 0 {
		t.Fatalf("发布后的状态不对: %+v", s)
	}
	if strings.Join(s.LastSources, ",") != "a,b,c" {
		t.Fatalf("LastSources = %v", s.LastSources)
	}
	if s.Issue != "" || s.FailStreak != 0 {
		t.Fatalf("成功发布后不该有问题记录: %+v", s)
	}

	clock.advance(10 * time.Second)
	if s := symbolStatus(t, f, "BTCUSDT"); !s.Healthy || s.PublishAgeMs != 10_000 {
		t.Fatalf("10秒前发布的还健康: %+v", s)
	}
	clock.advance(6 * time.Second) // 16秒
	if f.Status().Healthy {
		t.Fatal("16秒没成功发布，应该不健康")
	}
	f.Step(context.Background()) // 又发布成功
	if !f.Status().Healthy {
		t.Fatal("发布成功之后应该恢复健康")
	}
}

// 没发布成功的原因要能看出来：来源不足、发布失败、服务端跳变保护在等，连续失败的周期数会累加，成功后清空
func TestStatus_IssueAndFailStreak(t *testing.T) {
	a, b := &fakeSource{name: "a"}, &fakeSource{name: "b"}
	pub := &fakePublisher{}
	f, clock := newClockedFeeder(pub, a, b)
	ctx := context.Background()
	setAll("BTCUSDT", "60000", a, b)
	f.Step(ctx)

	// 来源不足
	b.set("BTCUSDT", "")
	clock.advance(time.Second)
	f.Step(ctx)
	f.Step(ctx)
	s := symbolStatus(t, f, "BTCUSDT")
	if !strings.Contains(s.Issue, "来源") || s.FailStreak != 2 || s.IssueAt != clock.now().UnixMilli() {
		t.Fatalf("来源不足: %+v", s)
	}

	// 发布失败
	b.set("BTCUSDT", "60000")
	pub.fail = true
	f.Step(ctx)
	if s := symbolStatus(t, f, "BTCUSDT"); !strings.Contains(s.Issue, "发布失败") || s.FailStreak != 3 {
		t.Fatalf("发布失败: %+v", s)
	}

	// 服务端跳变保护在等：要能和"接口故障"区分开
	pub.failWith = fmt.Errorf("%w: 等待确认", ErrJumpGuard)
	f.Step(ctx)
	if s := symbolStatus(t, f, "BTCUSDT"); !strings.Contains(s.Issue, "服务端跳变保护") || s.FailStreak != 4 {
		t.Fatalf("服务端跳变保护: %+v", s)
	}

	// 恢复
	pub.failWith, pub.fail = nil, false
	f.Step(ctx)
	if s := symbolStatus(t, f, "BTCUSDT"); s.Issue != "" || s.IssueAt != 0 || s.FailStreak != 0 {
		t.Fatalf("成功后应清空: %+v", s)
	}
}

// 喂价器自己的跳变保护在等确认：状态里能看到进度
func TestStatus_JumpPendingIssue(t *testing.T) {
	a, b := &fakeSource{name: "a"}, &fakeSource{name: "b"}
	f, _ := newClockedFeeder(&fakePublisher{}, a, b)
	ctx := context.Background()
	setAll("BTCUSDT", "60000", a, b)
	f.Step(ctx)
	setAll("BTCUSDT", "66000", a, b)
	f.Step(ctx)
	s := symbolStatus(t, f, "BTCUSDT")
	if !strings.Contains(s.Issue, "等待确认(1/3)") || s.FailStreak != 1 || s.LastPrice != "60000" {
		t.Fatalf("跳变等待确认: %+v", s)
	}
}

// 每个来源每个合约的取价状态：失败的来源能看到原因，恢复后清空，按合约、来源名排序
func TestStatus_SourceStates(t *testing.T) {
	a, b := &fakeSource{name: "binance"}, &fakeSource{name: "okx"}
	f, clock := newClockedFeeder(&fakePublisher{}, a, b)
	ctx := context.Background()
	setAll("BTCUSDT", "60000", a, b)
	b.set("BTCUSDT", "")
	f.Step(ctx)

	srcs := f.Status().Sources
	if len(srcs) != 2 || srcs[0].Source != "binance" || srcs[1].Source != "okx" {
		t.Fatalf("来源状态: %+v", srcs)
	}
	if !srcs[0].OK || srcs[0].LastOKAt != clock.now().UnixMilli() || srcs[0].LastError != "" {
		t.Fatalf("binance应该正常: %+v", srcs[0])
	}
	if srcs[1].OK || srcs[1].LastOKAt != 0 || srcs[1].LastError == "" || srcs[1].LastErrorAt != clock.now().UnixMilli() {
		t.Fatalf("okx应该失败: %+v", srcs[1])
	}

	b.set("BTCUSDT", "60000")
	clock.advance(time.Second)
	f.Step(ctx)
	if s := f.Status().Sources[1]; !s.OK || s.LastError != "" || s.LastOKAt != clock.now().UnixMilli() {
		t.Fatalf("okx恢复后: %+v", s)
	}
}

// 多个合约：只要有一个不健康，整体就不健康；状态不含合约之间的串扰
func TestStatus_OneUnhealthySymbolMakesWholeUnhealthy(t *testing.T) {
	a, b := &fakeSource{name: "a"}, &fakeSource{name: "b"}
	clock := &fakeClock{t: time.UnixMilli(1_700_000_000_000)}
	f := New(Config{Symbols: []string{"BTCUSDT", "ETHUSDT"}, Sources: []Source{a, b}}, &fakePublisher{})
	f.now, f.startedAt = clock.now, clock.now()
	setAll("BTCUSDT", "60000", a, b)
	setAll("ETHUSDT", "3000", a, b)
	f.Step(context.Background())
	if !f.Status().Healthy {
		t.Fatal("两个都发布了，应该健康")
	}
	a.set("ETHUSDT", "") // ETH只剩一家，发布不了
	for i := 0; i < 20; i++ {
		clock.advance(time.Second)
		f.Step(context.Background())
	}
	st := f.Status()
	if st.Symbols[0].Symbol != "BTCUSDT" || st.Symbols[1].Symbol != "ETHUSDT" {
		t.Fatalf("合约顺序: %+v", st.Symbols)
	}
	if !st.Symbols[0].Healthy {
		t.Fatal("BTC一直在发布，应该健康")
	}
	if st.Symbols[1].Healthy {
		t.Fatal("ETH停了20秒，应该不健康")
	}
	if st.Healthy {
		t.Fatal("有一个合约不健康，整体就该不健康")
	}
}

// HealthMaxAge可配置
func TestStatus_HealthMaxAgeIsConfigurable(t *testing.T) {
	a, b := &fakeSource{name: "a"}, &fakeSource{name: "b"}
	clock := &fakeClock{t: time.UnixMilli(1_700_000_000_000)}
	f := New(Config{Symbols: []string{"BTCUSDT"}, Sources: []Source{a, b}, HealthMaxAge: 5 * time.Second}, &fakePublisher{})
	f.now, f.startedAt = clock.now, clock.now()
	setAll("BTCUSDT", "60000", a, b)
	f.Step(context.Background())
	clock.advance(5 * time.Second)
	if !f.Status().Healthy {
		t.Fatal("恰好5秒还健康")
	}
	clock.advance(time.Millisecond)
	if f.Status().Healthy {
		t.Fatal("超过5秒应该不健康")
	}
}

func get(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(method, path, nil))
	return w
}

// /health：健康200，不健康503；两种情况body都是完整状态(不健康时更需要看原因)。/status始终200
func TestHandler_HealthAndStatus(t *testing.T) {
	a, b := &fakeSource{name: "a"}, &fakeSource{name: "b"}
	f, clock := newClockedFeeder(&fakePublisher{}, a, b)
	setAll("BTCUSDT", "60000", a, b)
	f.Step(context.Background())
	h := f.Handler()

	w := get(t, h, "GET", "/health")
	if w.Code != 200 {
		t.Fatalf("健康时/health = %d", w.Code)
	}
	var st Status
	if err := json.Unmarshal(w.Body.Bytes(), &st); err != nil || !st.Healthy || st.Symbols[0].LastPrice != "60000" {
		t.Fatalf("body: %s %v", w.Body.String(), err)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type = %q", ct)
	}

	setAll("BTCUSDT", "", a, b)
	clock.advance(20 * time.Second)
	f.Step(context.Background())
	w = get(t, h, "GET", "/health")
	if w.Code != 503 {
		t.Fatalf("不健康时/health = %d, want 503", w.Code)
	}
	if err := json.Unmarshal(w.Body.Bytes(), &st); err != nil || st.Healthy || st.Symbols[0].Issue == "" {
		t.Fatalf("503的body也要带完整状态: %s %v", w.Body.String(), err)
	}
	if w := get(t, h, "GET", "/status"); w.Code != 200 {
		t.Fatalf("/status不健康时也要200, got %d", w.Code)
	}
}

// 只读：不接受GET以外的方法，也没有别的路径
func TestHandler_ReadOnly(t *testing.T) {
	a, b := &fakeSource{name: "a"}, &fakeSource{name: "b"}
	f, _ := newClockedFeeder(&fakePublisher{}, a, b)
	h := f.Handler()
	for _, tc := range []struct{ method, path string }{{"POST", "/health"}, {"PUT", "/status"}, {"DELETE", "/status"}} {
		if w := get(t, h, tc.method, tc.path); w.Code != 405 {
			t.Errorf("%s %s = %d, want 405", tc.method, tc.path, w.Code)
		}
	}
	if w := get(t, h, "GET", "/nope"); w.Code != 404 {
		t.Errorf("未知路径 = %d, want 404", w.Code)
	}
}
