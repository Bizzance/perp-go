package indexfeed

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

// ---- 聚合 ----

// 期望值都按定义手算：中位数，丢掉偏离中位数超过outlier的来源，剩下的再取一次中位数
func TestAggregate(t *testing.T) {
	cases := []struct {
		name    string
		prices  map[string]string
		outlier string
		min     int
		want    string
		used    []string
		wantErr bool
	}{
		{"三家都在1%以内：取中位数", map[string]string{"a": "100", "b": "101", "c": "102"}, "0.01", 2, "101", []string{"a", "b", "c"}, false},
		{"一家离谱被丢：剩下两家取均值", map[string]string{"a": "100", "b": "101", "c": "110"}, "0.01", 2, "100.5", []string{"a", "b"}, false},
		{"一家被操纵到200：中位数100.4，丢掉它，剩下的均值100.2", map[string]string{"a": "100", "b": "100.4", "c": "200"}, "0.01", 2, "100.2", []string{"a", "b"}, false},
		{"两家一致：均值", map[string]string{"a": "100", "b": "100.5"}, "0.01", 2, "100.25", []string{"a", "b"}, false},
		{"两家对不上(差10%)：都偏离均值超过1%，拒绝", map[string]string{"a": "100", "b": "110"}, "0.01", 2, "", nil, true},
		{"只有一家：不够2家，拒绝", map[string]string{"a": "100"}, "0.01", 2, "", nil, true},
		{"没有来源", map[string]string{}, "0.01", 2, "", nil, true},
		{"四家：偶数个取中间两个的均值", map[string]string{"a": "100", "b": "100.2", "c": "100.4", "d": "100.6"}, "0.01", 2, "100.3", []string{"a", "b", "c", "d"}, false},
		{"偏离恰好等于outlier不算离群(1/100=1%)", map[string]string{"a": "100", "b": "100", "c": "101"}, "0.01", 2, "100", []string{"a", "b", "c"}, false},
		{"偏离略超过outlier算离群", map[string]string{"a": "100", "b": "100", "c": "101.01"}, "0.01", 2, "100", []string{"a", "b"}, false},
		{"minSources=1时单家也发布", map[string]string{"a": "100"}, "0.01", 1, "100", []string{"a"}, false},
		{"要求3家但离群后只剩2家：拒绝", map[string]string{"a": "100", "b": "100.1", "c": "150"}, "0.01", 3, "", nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			in := map[string]decimal.Decimal{}
			for k, v := range c.prices {
				in[k] = d(v)
			}
			got, used, err := Aggregate(in, d(c.outlier), c.min)
			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, c.wantErr)
			}
			if c.wantErr {
				return
			}
			if !got.Equal(d(c.want)) {
				t.Fatalf("价格 = %s, want %s", got, c.want)
			}
			if len(used) != len(c.used) {
				t.Fatalf("used = %v, want %v", used, c.used)
			}
			for i := range used {
				if used[i] != c.used[i] {
					t.Fatalf("used = %v, want %v", used, c.used)
				}
			}
		})
	}
}

// ---- 喂价循环 ----

type fakeSource struct {
	name string
	mu   sync.Mutex
	next map[string]string // symbol -> 价格，缺=失败
}

func (s *fakeSource) Name() string { return s.name }
func (s *fakeSource) Fetch(_ context.Context, symbol string) (decimal.Decimal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.next[symbol]
	if !ok {
		return decimal.Zero, errors.New("模拟来源故障")
	}
	return d(v), nil
}
func (s *fakeSource) set(symbol, price string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.next == nil {
		s.next = map[string]string{}
	}
	if price == "" {
		delete(s.next, symbol)
	} else {
		s.next[symbol] = price
	}
}

type fakePublisher struct {
	mu       sync.Mutex
	got      []published
	fail     bool
	failWith error // 非nil时发布失败并返回这个错误，优先于fail
	attempts int   // Publish被调用的总次数，成功失败都算
}

type published struct {
	symbol string
	price  decimal.Decimal
}

func (p *fakePublisher) Publish(_ context.Context, symbol string, price decimal.Decimal) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attempts++
	if p.failWith != nil {
		return p.failWith
	}
	if p.fail {
		return errors.New("模拟发布失败")
	}
	p.got = append(p.got, published{symbol, price})
	return nil
}
func (p *fakePublisher) prices(symbol string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for _, g := range p.got {
		if g.symbol == symbol {
			out = append(out, g.price.String())
		}
	}
	return out
}

func newFeeder(pub Publisher, srcs ...Source) *Feeder {
	return New(Config{Symbols: []string{"BTCUSDT"}, Sources: srcs}, pub)
}

func setAll(sym, price string, srcs ...*fakeSource) {
	for _, s := range srcs {
		s.set(sym, price)
	}
}

func eq(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("发布了%v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("发布了%v, want %v", got, want)
		}
	}
}

func TestFeeder_PublishesMedianOfAllSources(t *testing.T) {
	a, b, c := &fakeSource{name: "a"}, &fakeSource{name: "b"}, &fakeSource{name: "c"}
	a.set("BTCUSDT", "60000")
	b.set("BTCUSDT", "60030")
	c.set("BTCUSDT", "60100")
	pub := &fakePublisher{}
	newFeeder(pub, a, b, c).Step(context.Background())
	eq(t, pub.prices("BTCUSDT"), "60030")
}

func TestFeeder_OneSourceDownStillPublishesWithTheRest(t *testing.T) {
	a, b, c := &fakeSource{name: "a"}, &fakeSource{name: "b"}, &fakeSource{name: "c"}
	a.set("BTCUSDT", "60000")
	b.set("BTCUSDT", "60040")
	// c故障
	pub := &fakePublisher{}
	newFeeder(pub, a, b, c).Step(context.Background())
	eq(t, pub.prices("BTCUSDT"), "60020")
}

func TestFeeder_TooFewSourcesPublishesNothing(t *testing.T) {
	a, b, c := &fakeSource{name: "a"}, &fakeSource{name: "b"}, &fakeSource{name: "c"}
	a.set("BTCUSDT", "60000") // 只剩一家
	pub := &fakePublisher{}
	newFeeder(pub, a, b, c).Step(context.Background())
	eq(t, pub.prices("BTCUSDT"))
}

func TestFeeder_DisagreeingSourcesPublishNothing(t *testing.T) {
	a, b := &fakeSource{name: "a"}, &fakeSource{name: "b"}
	a.set("BTCUSDT", "60000")
	b.set("BTCUSDT", "66000") // 两家差10%，不猜哪家对
	pub := &fakePublisher{}
	newFeeder(pub, a, b).Step(context.Background())
	eq(t, pub.prices("BTCUSDT"))
}

func TestFeeder_ManipulatedSourceIsIgnored(t *testing.T) {
	a, b, c := &fakeSource{name: "a"}, &fakeSource{name: "b"}, &fakeSource{name: "c"}
	a.set("BTCUSDT", "60000")
	b.set("BTCUSDT", "60040")
	c.set("BTCUSDT", "90000") // 一家被操纵
	pub := &fakePublisher{}
	newFeeder(pub, a, b, c).Step(context.Background())
	eq(t, pub.prices("BTCUSDT"), "60020")
}

// 跳变保护：变动超过3%先不发布，连续3个周期都在新价位才承认
func TestFeeder_BigJumpNeedsThreeConsecutiveConfirmations(t *testing.T) {
	a, b := &fakeSource{name: "a"}, &fakeSource{name: "b"}
	pub := &fakePublisher{}
	f := newFeeder(pub, a, b)
	ctx := context.Background()
	setAll("BTCUSDT", "60000", a, b)
	f.Step(ctx) // 第一次没有上一次的价格，直接发布

	setAll("BTCUSDT", "66000", a, b) // +10%
	f.Step(ctx)
	f.Step(ctx)
	eq(t, pub.prices("BTCUSDT"), "60000") // 前两个周期都不发布
	f.Step(ctx)
	eq(t, pub.prices("BTCUSDT"), "60000", "66000") // 第三个周期承认

	setAll("BTCUSDT", "66100", a, b) // 之后的正常波动照常发布
	f.Step(ctx)
	eq(t, pub.prices("BTCUSDT"), "60000", "66000", "66100")
}

// 变动恰好等于3%不算跳变，直接放行(60000 -> 61800)；略超过(61801)要等确认
func TestFeeder_JumpExactlyAtLimitIsAccepted(t *testing.T) {
	a, b := &fakeSource{name: "a"}, &fakeSource{name: "b"}
	pub := &fakePublisher{}
	f := newFeeder(pub, a, b)
	ctx := context.Background()
	setAll("BTCUSDT", "60000", a, b)
	f.Step(ctx)
	setAll("BTCUSDT", "61800", a, b)
	f.Step(ctx)
	eq(t, pub.prices("BTCUSDT"), "60000", "61800")
	setAll("BTCUSDT", "63655", a, b) // 相对61800是+3.0016%
	f.Step(ctx)
	eq(t, pub.prices("BTCUSDT"), "60000", "61800")
}

// 一个周期的毛刺(第二个周期又回到原价位)不会被承认，确认计数重来
func TestFeeder_TransientSpikeIsNeverPublished(t *testing.T) {
	a, b := &fakeSource{name: "a"}, &fakeSource{name: "b"}
	pub := &fakePublisher{}
	f := newFeeder(pub, a, b)
	ctx := context.Background()
	setAll("BTCUSDT", "60000", a, b)
	f.Step(ctx)
	setAll("BTCUSDT", "66000", a, b)
	f.Step(ctx)
	f.Step(ctx) // 已经确认了两次
	setAll("BTCUSDT", "60100", a, b)
	f.Step(ctx) // 回到正常范围，直接发布，并且清掉确认计数
	setAll("BTCUSDT", "66000", a, b)
	f.Step(ctx)
	f.Step(ctx)
	eq(t, pub.prices("BTCUSDT"), "60000", "60100") // 计数是重来的，这两个周期还不够3次
}

// 跳变期间价位又变了：新价位从1重新计数
func TestFeeder_JumpTargetChangeRestartsCount(t *testing.T) {
	a, b := &fakeSource{name: "a"}, &fakeSource{name: "b"}
	pub := &fakePublisher{}
	f := newFeeder(pub, a, b)
	ctx := context.Background()
	setAll("BTCUSDT", "60000", a, b)
	f.Step(ctx)
	setAll("BTCUSDT", "66000", a, b)
	f.Step(ctx)
	f.Step(ctx)
	setAll("BTCUSDT", "72000", a, b) // 和66000差9%，不是同一个价位
	f.Step(ctx)
	f.Step(ctx)
	eq(t, pub.prices("BTCUSDT"), "60000")
	f.Step(ctx)
	eq(t, pub.prices("BTCUSDT"), "60000", "72000")
}

// 发布失败不更新"上次发布的价格"：跳变判断还是相对真正发布出去的那个价格。
// 60000发布成功；61500(+2.5%)发布失败；62000相对60000是+3.33%(超过3%，要等确认)，
// 相对失败的61500只有+0.8%——如果错把失败的当成上次发布的，62000会被直接放行
func TestFeeder_FailedPublishDoesNotAdvanceLast(t *testing.T) {
	a, b := &fakeSource{name: "a"}, &fakeSource{name: "b"}
	pub := &fakePublisher{}
	f := newFeeder(pub, a, b)
	ctx := context.Background()
	setAll("BTCUSDT", "60000", a, b)
	f.Step(ctx)
	pub.fail = true
	setAll("BTCUSDT", "61500", a, b)
	f.Step(ctx)
	pub.fail = false
	setAll("BTCUSDT", "62000", a, b)
	f.Step(ctx)
	eq(t, pub.prices("BTCUSDT"), "60000")
}

func TestFeeder_SymbolsAreIndependent(t *testing.T) {
	a, b := &fakeSource{name: "a"}, &fakeSource{name: "b"}
	a.set("BTCUSDT", "60000")
	b.set("BTCUSDT", "60000")
	a.set("ETHUSDT", "3000")
	b.set("ETHUSDT", "3010")
	pub := &fakePublisher{}
	f := New(Config{Symbols: []string{"BTCUSDT", "ETHUSDT"}, Sources: []Source{a, b}}, pub)
	f.Step(context.Background())
	eq(t, pub.prices("BTCUSDT"), "60000")
	eq(t, pub.prices("ETHUSDT"), "3005")
	// ETH的一家来源挂了，不影响BTC
	b.set("ETHUSDT", "")
	f.Step(context.Background())
	eq(t, pub.prices("BTCUSDT"), "60000", "60000")
	eq(t, pub.prices("ETHUSDT"), "3005")
}

// 跳变确认之后，发布没成功(服务端的跳变保护还在等)，下个周期要继续推，不用从头再数3个周期：
// 从头数的话，服务端等确认的这段时间里喂价器每3个周期才推一次
func TestFeeder_ConfirmedJumpKeepsPublishingWhileServerGuardWaits(t *testing.T) {
	a, b := &fakeSource{name: "a"}, &fakeSource{name: "b"}
	pub := &fakePublisher{}
	f := newFeeder(pub, a, b)
	ctx := context.Background()
	setAll("BTCUSDT", "60000", a, b)
	f.Step(ctx) // attempts=1

	setAll("BTCUSDT", "66000", a, b)
	pub.failWith = fmt.Errorf("%w: 等待确认", ErrJumpGuard)
	f.Step(ctx)
	f.Step(ctx)
	if pub.attempts != 1 {
		t.Fatalf("喂价器自己的确认还没满3个周期，不该推: attempts=%d", pub.attempts)
	}
	f.Step(ctx) // 第3个周期确认，推了一次，被服务端拦下
	f.Step(ctx)
	f.Step(ctx)
	if pub.attempts != 4 {
		t.Fatalf("确认之后每个周期都要继续推: attempts=%d, want 4", pub.attempts)
	}
	eq(t, pub.prices("BTCUSDT"), "60000") // 一直没成功，last还是60000

	pub.failWith = nil // 服务端承认了
	f.Step(ctx)
	eq(t, pub.prices("BTCUSDT"), "60000", "66000")
	setAll("BTCUSDT", "66100", a, b) // 之后是正常波动，直接发布
	f.Step(ctx)
	eq(t, pub.prices("BTCUSDT"), "60000", "66000", "66100")
}

// 确认之后价位又变了(离开了确认过的价位)：重新数，不能拿着旧的确认放行一个没确认过的价位
func TestFeeder_ConfirmedJumpDoesNotVouchForAnotherLevel(t *testing.T) {
	a, b := &fakeSource{name: "a"}, &fakeSource{name: "b"}
	pub := &fakePublisher{}
	f := newFeeder(pub, a, b)
	ctx := context.Background()
	setAll("BTCUSDT", "60000", a, b)
	f.Step(ctx)
	pub.failWith = fmt.Errorf("%w: 等待确认", ErrJumpGuard)
	setAll("BTCUSDT", "66000", a, b)
	f.Step(ctx)
	f.Step(ctx)
	f.Step(ctx) // 66000已确认(attempts=2)
	setAll("BTCUSDT", "72000", a, b)
	before := pub.attempts
	f.Step(ctx)
	if pub.attempts != before {
		t.Fatal("72000是新的价位，要重新确认，不能直接推")
	}
}
