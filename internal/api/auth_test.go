package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"perp-go/internal/config"
)

const (
	testSecret     = "0123456789abcdef0123456789abcdef"
	testOpsSecret  = "ops-secret-0123456789abcdef"
	testTradeKeyID = "partner-a"
	testOpsKeyID   = "feeder"
)

// 内存版nonce存储，测试里代替Redis
type memNonces struct {
	mu   sync.Mutex
	seen map[string]bool
	err  error
}

func (m *memNonces) ClaimNonce(_ context.Context, key string, _ time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return false, m.err
	}
	if m.seen == nil {
		m.seen = map[string]bool{}
	}
	if m.seen[key] {
		return false, nil
	}
	m.seen[key] = true
	return true, nil
}

var testNow = time.UnixMilli(1_789_800_000_000)

func newTestRouter(disabled bool, nonces NonceStore) (*gin.Engine, *string) {
	auth := NewAuth(disabled, []config.APIKey{
		{ID: testTradeKeyID, Secret: testSecret, Scopes: []string{"trade"}},
		{ID: testOpsKeyID, Secret: testOpsSecret, Scopes: []string{"ops"}},
	}, nonces)
	auth.now = func() time.Time { return testNow }
	var gotBody string
	r := gin.New()
	r.Use(auth.Middleware())
	auth.Route(r, "POST", "/order/add", ScopeTrade, func(c *gin.Context) {
		b, _ := io.ReadAll(c.Request.Body)
		gotBody = string(b)
		ok(c, "done")
	})
	auth.Route(r, "POST", "/account/balance", ScopeOps, func(c *gin.Context) { ok(c, "done") })
	auth.Route(r, "GET", "/account/info", ScopeTrade, func(c *gin.Context) { ok(c, c.Query("uid")) })
	auth.Route(r, "GET", "/echo/:v", ScopeTrade, func(c *gin.Context) { ok(c, c.Param("v")) })
	// 故意不通过Route注册的路由：用来验证"没声明权限范围就默认拒绝"
	r.GET("/forgotten", func(c *gin.Context) { ok(c, "should never be reached") })
	r.GET("/health", func(c *gin.Context) { ok(c, "up") })
	return r, &gotBody
}

type signedReq struct {
	keyID, secret, nonce string
	ts                   int64
	method, path, query  string
	body                 string
}

func (s signedReq) build() *http.Request {
	target := s.path
	if s.query != "" {
		target += "?" + s.query
	}
	req := httptest.NewRequest(s.method, target, strings.NewReader(s.body))
	tsStr := strconv.FormatInt(s.ts, 10)
	req.Header.Set("X-Api-Key", s.keyID)
	req.Header.Set("X-Timestamp", tsStr)
	req.Header.Set("X-Nonce", s.nonce)
	req.Header.Set("X-Signature", SignRequest(s.secret, tsStr, s.nonce, s.method, s.path, s.query, []byte(s.body)))
	return req
}

func validTrade() signedReq {
	return signedReq{keyID: testTradeKeyID, secret: testSecret, nonce: "nonce-0000000001", ts: testNow.UnixMilli(),
		method: "POST", path: "/order/add", body: `{"uid":10001,"amount":1}`}
}

func do(r *gin.Engine, req *http.Request) map[string]any {
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return body2map(w)
}

func body2map(w *httptest.ResponseRecorder) map[string]any {
	m := map[string]any{}
	_ = json.Unmarshal(w.Body.Bytes(), &m)
	return m
}

func TestAuth_ValidRequestPassesAndBodyIsRestored(t *testing.T) {
	r, gotBody := newTestRouter(false, &memNonces{})
	m := do(r, validTrade().build())
	if m["code"] != float64(200) {
		t.Fatalf("合法签名应该通过, got %v", m)
	}
	if *gotBody != `{"uid":10001,"amount":1}` {
		t.Errorf("鉴权读完请求体后必须放回去，handler才能继续读, got %q", *gotBody)
	}
}

func TestAuth_MissingHeaders(t *testing.T) {
	r, _ := newTestRouter(false, &memNonces{})
	for _, h := range []string{"X-Api-Key", "X-Timestamp", "X-Nonce", "X-Signature"} {
		req := validTrade().build()
		req.Header.Del(h)
		if m := do(r, req); m["errCode"] != ErrAuthMissing || m["code"] != float64(401) {
			t.Errorf("缺%s应该是401 auth_missing, got %v", h, m)
		}
	}
	// nonce太短/带非法字符也算格式不对
	for _, n := range []string{"short", "has space in nonce 000", "非法字符非法字符非法字符非法字符"} {
		s := validTrade()
		s.nonce = n
		if m := do(r, s.build()); m["errCode"] != ErrAuthMissing {
			t.Errorf("nonce=%q 格式不对应该auth_missing, got %v", n, m)
		}
	}
	// 时间戳不是整数
	req := validTrade().build()
	req.Header.Set("X-Timestamp", "abc")
	if m := do(r, req); m["errCode"] != ErrAuthMissing {
		t.Errorf("时间戳不是整数应该auth_missing, got %v", m)
	}
}

func TestAuth_TimestampWindow(t *testing.T) {
	r, _ := newTestRouter(false, &memNonces{})
	cases := []struct {
		name   string
		offset time.Duration
		want   string
	}{
		{"刚好29秒前", -29 * time.Second, ""},
		{"刚好29秒后", 29 * time.Second, ""},
		{"31秒前(过期)", -31 * time.Second, ErrAuthExpired},
		{"31秒后(时钟超前太多)", 31 * time.Second, ErrAuthExpired},
	}
	for i, tc := range cases {
		s := validTrade()
		s.ts = testNow.Add(tc.offset).UnixMilli()
		s.nonce = "nonce-window-" + strconv.Itoa(1000+i)
		m := do(r, s.build())
		got, _ := m["errCode"].(string)
		if got != tc.want {
			t.Errorf("%s: errCode=%q want %q (%v)", tc.name, got, tc.want, m)
		}
	}
}

func TestAuth_WrongSignatureAndUnknownKeyLookIdentical(t *testing.T) {
	r, _ := newTestRouter(false, &memNonces{})
	bad := validTrade()
	bad.secret = "wrong-secret-wrong-secret-wrong"
	m1 := do(r, bad.build())

	unknown := validTrade()
	unknown.keyID = "no-such-key"
	unknown.nonce = "nonce-0000000002"
	m2 := do(r, unknown.build())

	if m1["errCode"] != ErrAuthInvalidSignature || m2["errCode"] != ErrAuthInvalidSignature {
		t.Fatalf("签名错和key不存在都应该是auth_invalid_signature, got %v / %v", m1, m2)
	}
	if m1["message"] != m2["message"] || m1["code"] != m2["code"] {
		t.Error("两种失败的响应必须完全一致，不能让调用方靠响应差异探测有效的key")
	}
}

func TestAuth_TamperedRequestIsRejected(t *testing.T) {
	r, _ := newTestRouter(false, &memNonces{})
	cases := map[string]func(*http.Request){
		"改请求体": func(req *http.Request) { req.Body = io.NopCloser(strings.NewReader(`{"uid":10001,"amount":9999}`)) },
		"改路径":  func(req *http.Request) { req.URL.Path = "/account/balance" },
		"改方法":  func(req *http.Request) { req.Method = "GET" },
		"改查询串": func(req *http.Request) { req.URL.RawQuery = "uid=2" },
	}
	i := 0
	for name, mutate := range cases {
		i++
		s := validTrade()
		s.nonce = "nonce-tamper-" + strconv.Itoa(1000+i)
		req := s.build()
		mutate(req)
		if m := do(r, req); m["errCode"] != ErrAuthInvalidSignature {
			t.Errorf("%s: 应该签名校验失败, got %v", name, m)
		}
	}
}

func TestAuth_ReplayIsRejectedAndOnlyAfterValidSignature(t *testing.T) {
	nonces := &memNonces{}
	r, _ := newTestRouter(false, nonces)

	if m := do(r, validTrade().build()); m["code"] != float64(200) {
		t.Fatalf("第一次应该通过, got %v", m)
	}
	if m := do(r, validTrade().build()); m["errCode"] != ErrAuthReplayed {
		t.Errorf("同一个nonce第二次应该auth_replayed, got %v", m)
	}

	// 签名错误的请求不能消耗nonce，否则没有密钥的人也能拿假请求把nonce存储塞满
	bad := validTrade()
	bad.nonce = "nonce-fake-0000001"
	bad.secret = "wrong-secret-wrong-secret-wrong"
	do(r, bad.build())
	if len(nonces.seen) != 1 {
		t.Errorf("签名错误的请求不应该记录nonce, seen=%d", len(nonces.seen))
	}
}

func TestAuth_NonceStoreFailureFailsClosed(t *testing.T) {
	r, _ := newTestRouter(false, &memNonces{err: errors.New("redis down")})
	m := do(r, validTrade().build())
	if m["code"] != float64(500) {
		t.Errorf("nonce存储不可用时必须拒绝请求(失败关闭)，不能放行, got %v", m)
	}
}

func TestAuth_ScopeEnforcement(t *testing.T) {
	r, _ := newTestRouter(false, &memNonces{})

	// trade密钥不能调运营接口(加钱扣钱)
	s := validTrade()
	s.path, s.nonce = "/account/balance", "nonce-scope-000001"
	if m := do(r, s.build()); m["errCode"] != ErrForbidden || m["code"] != float64(403) {
		t.Errorf("trade密钥调ops接口应该403 forbidden, got %v", m)
	}

	// ops密钥不能下单
	o := signedReq{keyID: testOpsKeyID, secret: testOpsSecret, nonce: "nonce-scope-000002", ts: testNow.UnixMilli(),
		method: "POST", path: "/order/add", body: `{}`}
	if m := do(r, o.build()); m["errCode"] != ErrForbidden {
		t.Errorf("ops密钥调trade接口应该forbidden, got %v", m)
	}

	// ops密钥能调运营接口
	o.path, o.nonce = "/account/balance", "nonce-scope-000003"
	if m := do(r, o.build()); m["code"] != float64(200) {
		t.Errorf("ops密钥调ops接口应该通过, got %v", m)
	}
}

func TestAuth_GETWithQueryString(t *testing.T) {
	r, _ := newTestRouter(false, &memNonces{})
	s := signedReq{keyID: testTradeKeyID, secret: testSecret, nonce: "nonce-get-0000001", ts: testNow.UnixMilli(),
		method: "GET", path: "/account/info", query: "uid=10001&x=a%20b"}
	m := do(r, s.build())
	if m["code"] != float64(200) || m["data"] != "10001" {
		t.Errorf("带查询串的GET应该通过, got %v", m)
	}
}

func TestAuth_HealthIsExemptAndDisabledModeSkipsEverything(t *testing.T) {
	r, _ := newTestRouter(false, &memNonces{})
	req := httptest.NewRequest("GET", "/health", nil)
	if m := do(r, req); m["code"] != float64(200) {
		t.Errorf("/health必须免鉴权(编排探针用), got %v", m)
	}

	r2, _ := newTestRouter(true, &memNonces{})
	if m := do(r2, httptest.NewRequest("POST", "/account/balance", strings.NewReader(`{}`))); m["code"] != float64(200) {
		t.Errorf("鉴权关闭时任何请求都放行(仅本地开发), got %v", m)
	}
}

func TestAuth_OversizedBodyRejected(t *testing.T) {
	r, _ := newTestRouter(false, &memNonces{})
	s := validTrade()
	s.body = strings.Repeat("a", maxAuthBodyBytes+10)
	if m := do(r, s.build()); m["code"] != float64(400) {
		t.Errorf("超过请求体上限应该被拒绝, got %v", m)
	}
}

// 固定向量：合作方按文档自己实现签名时可以拿它对照，保证跨语言实现一致。期望值是用openssl
// 独立算出来的，不是拿Go的输出抄回来的
func TestSignRequest_KnownVector(t *testing.T) {
	const want = "0a5fba8e94ec1c1e8d97dc4cfb5d3434b50b1c9c53dd6e8836344e403741dfeb"
	got := SignRequest("secret-secret-secret-1", "1789800000000", "nonce-0000000001", "POST", "/order/add",
		"a=1", []byte(`{"uid":1}`))
	if got != want {
		t.Fatalf("签名和固定向量不一致: got %s want %s", got, want)
	}
}

// 忘了声明权限范围的路由必须默认拒绝，而不是任何签名有效的密钥都能调
func TestAuth_UndeclaredRouteIsDeniedByDefault(t *testing.T) {
	r, _ := newTestRouter(false, &memNonces{})
	for i, keyID := range []string{testTradeKeyID, testOpsKeyID} {
		secret := map[string]string{testTradeKeyID: testSecret, testOpsKeyID: testOpsSecret}[keyID]
		s := signedReq{keyID: keyID, secret: secret, nonce: "nonce-deny-000000" + strconv.Itoa(i), ts: testNow.UnixMilli(),
			method: "GET", path: "/forgotten"}
		m := do(r, s.build())
		if m["errCode"] != ErrForbidden || m["data"] != nil {
			t.Errorf("没声明权限范围的路由应该默认拒绝(%s), got %v", keyID, m)
		}
	}
}

// 真实的路由表里，除了/health每个路由都必须声明过权限范围。以后新增接口忘了用Route注册，
// 这个测试会失败；就算漏过了测试，运行时也是默认拒绝
func TestRealRouters_EveryRouteDeclaresAScope(t *testing.T) {
	keys := []config.APIKey{{ID: "k", Secret: testSecret, Scopes: []string{"trade", "ops"}}}

	auth := NewAuth(false, keys, &memNonces{})
	apiRoutes := (&Server{auth: auth}).Router().Routes()
	engineAuth := NewAuth(false, keys, &memNonces{})
	engineRoutes := (&EngineServer{auth: engineAuth}).Router().Routes()

	check := func(name string, a *Auth, routes gin.RoutesInfo) {
		if len(routes) == 0 {
			t.Fatalf("%s没有路由", name)
		}
		for _, ri := range routes {
			if ri.Path == "/health" {
				continue
			}
			if _, ok := a.routeScopes[ri.Method+" "+ri.Path]; !ok {
				t.Errorf("%s: 路由 %s %s 没有声明权限范围", name, ri.Method, ri.Path)
			}
		}
	}
	check("contract-api", auth, apiRoutes)
	check("contract-engine", engineAuth, engineRoutes)

	// 运营类接口必须是ops，不能因为漏改被降成trade
	for _, p := range []string{"/account/balance", "/account/credit", "/account/insured", "/index-price", "/kline/sync"} {
		if got := auth.routeScopes["POST "+p]; got != ScopeOps {
			t.Errorf("POST %s 必须是ops权限, got %q", p, got)
		}
	}
	// 反过来：下单、查询这类必须是trade
	for _, p := range []string{"/order/add", "/account/create", "/account/round/close"} {
		if got := auth.routeScopes["POST "+p]; got != ScopeTrade {
			t.Errorf("POST %s 必须是trade权限, got %q", p, got)
		}
	}
}

// 客户端按"实际发送的原始路径"签名，服务端必须也用原始(百分号编码的)路径验证，不能用解码后的
func TestAuth_SignsRawEscapedPath(t *testing.T) {
	r, _ := newTestRouter(false, &memNonces{})
	rawPath := "/echo/a%20b"
	s := signedReq{keyID: testTradeKeyID, secret: testSecret, nonce: "nonce-escape-00001", ts: testNow.UnixMilli(),
		method: "GET", path: rawPath}
	req := s.build()
	if m := do(r, req); m["code"] != float64(200) || m["data"] != "a b" {
		t.Errorf("按原始路径签名的请求应该通过, got %v", m)
	}
}

// 没有Content-Length(分块传输)的超大请求体也要被拒绝，并且读取量有上限
func TestAuth_ChunkedOversizedBodyRejected(t *testing.T) {
	r, _ := newTestRouter(false, &memNonces{})
	s := validTrade()
	big := strings.Repeat("a", maxAuthBodyBytes+100)
	req := s.build()
	req.Body = io.NopCloser(strings.NewReader(big))
	req.ContentLength = -1
	tsStr := req.Header.Get("X-Timestamp")
	req.Header.Set("X-Signature", SignRequest(s.secret, tsStr, s.nonce, s.method, s.path, s.query, []byte(big)))
	if m := do(r, req); m["code"] != float64(400) {
		t.Errorf("分块传输的超大请求体应该被拒绝, got %v", m)
	}
}
