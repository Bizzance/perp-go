package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"perp-go/internal/api"
)

// 假的上游：按生产的验签方式重新算一遍签名，并把收到的请求记下来
type upstream struct {
	t       *testing.T
	secret  string
	gotPath string
	gotQ    string
	gotBody string
	gotHdr  http.Header
	sigOK   bool
}

func (u *upstream) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		u.gotPath, u.gotQ, u.gotBody, u.gotHdr = r.URL.EscapedPath(), r.URL.RawQuery, string(b), r.Header.Clone()
		want := api.SignRequest(u.secret, r.Header.Get("X-Timestamp"), r.Header.Get("X-Nonce"), r.Method, r.URL.EscapedPath(), r.URL.RawQuery, b)
		u.sigOK = want == r.Header.Get("X-Signature")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"code":200,"data":"pong"}`))
	})
}

func newTestServer(t *testing.T, upstreamURL string, keyID, secret string) *httptest.Server {
	t.Helper()
	s := newServer(config{apiURL: upstreamURL, engineURL: upstreamURL, keyID: keyID, secret: secret})
	ts := httptest.NewServer(s.routes())
	t.Cleanup(ts.Close)
	return ts
}

func do(t *testing.T, method, url, body string, headers map[string]string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

var guarded = map[string]string{guardHeader: "1"}

// 转发时请求被签名，签名覆盖的路径、查询串、请求体跟实际转发出去的一致；/api前缀被去掉
func TestForward_SignsExactlyWhatItForwards(t *testing.T) {
	up := &upstream{t: t, secret: "secret-secret-secret-1"}
	us := httptest.NewServer(up.handler())
	defer us.Close()
	ts := newTestServer(t, us.URL, "k1", "secret-secret-secret-1")

	resp, body := do(t, "POST", ts.URL+"/api/order/cancel/123?a=1&b=x%20y", `{"uid":1}`, guarded)

	if resp.StatusCode != 200 || !strings.Contains(body, "pong") {
		t.Fatalf("应该原样返回上游响应: %d %s", resp.StatusCode, body)
	}
	if up.gotPath != "/order/cancel/123" || up.gotQ != "a=1&b=x%20y" || up.gotBody != `{"uid":1}` {
		t.Fatalf("转发的路径/查询串/请求体不对: %q %q %q", up.gotPath, up.gotQ, up.gotBody)
	}
	if !up.sigOK {
		t.Fatal("上游按同样的方式验签应该通过")
	}
	if up.gotHdr.Get("X-Api-Key") != "k1" || up.gotHdr.Get(guardHeader) != "" {
		t.Fatalf("应该带上API Key，且不能把页面的防护头转给上游: %v", up.gotHdr)
	}
}

// 没配密钥就不签名(后端关了鉴权时用)
func TestForward_NoKeyMeansNoSignature(t *testing.T) {
	up := &upstream{t: t}
	us := httptest.NewServer(up.handler())
	defer us.Close()
	ts := newTestServer(t, us.URL, "", "")

	do(t, "GET", ts.URL+"/api/health", "", guarded)

	if up.gotHdr.Get("X-Signature") != "" || up.gotHdr.Get("X-Api-Key") != "" {
		t.Fatalf("没配密钥不应该带签名头: %v", up.gotHdr)
	}
}

// 引擎的深度接口走/engine前缀
func TestForward_EnginePrefix(t *testing.T) {
	up := &upstream{t: t, secret: "s-s-s-s-s-s-s-s-s-s"}
	us := httptest.NewServer(up.handler())
	defer us.Close()
	ts := newTestServer(t, us.URL, "k", "s-s-s-s-s-s-s-s-s-s")

	do(t, "GET", ts.URL+"/engine/depth?symbol=BTCUSDT&levels=10", "", guarded)

	if up.gotPath != "/depth" || up.gotQ != "symbol=BTCUSDT&levels=10" || !up.sigOK {
		t.Fatalf("/engine应该转成上游的/depth并签名: %q %q ok=%v", up.gotPath, up.gotQ, up.sigOK)
	}
}

// 防护一：没有自定义请求头的请求被拒(别的网站的页面借浏览器发请求，加不上这个头)
func TestGuard_RequiresCustomHeader(t *testing.T) {
	up := &upstream{t: t}
	us := httptest.NewServer(up.handler())
	defer us.Close()
	ts := newTestServer(t, us.URL, "k", "s-s-s-s-s-s-s-s-s-s")

	resp, _ := do(t, "POST", ts.URL+"/api/account/balance", `{}`, nil)

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("没有%s应该被拒绝, got %d", guardHeader, resp.StatusCode)
	}
	if up.gotPath != "" {
		t.Fatal("被拒绝的请求不应该转发到上游")
	}
}

// 防护二：Host不是回环名字的请求被拒(防DNS重绑定)
func TestGuard_RejectsForeignHost(t *testing.T) {
	up := &upstream{t: t}
	us := httptest.NewServer(up.handler())
	defer us.Close()
	ts := newTestServer(t, us.URL, "k", "s-s-s-s-s-s-s-s-s-s")

	req, _ := http.NewRequest("GET", ts.URL+"/api/health", nil)
	req.Host = "evil.example.com"
	req.Header.Set(guardHeader, "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden || up.gotPath != "" {
		t.Fatalf("外来Host应该被拒绝且不转发: %d", resp.StatusCode)
	}
}

// 防护三：WebSocket必须是本代理自己的页面发起的(校验Origin)；没有Origin也拒绝
func TestGuard_WebSocketChecksOrigin(t *testing.T) {
	us := httptest.NewServer(http.NotFoundHandler())
	defer us.Close()
	ts := newTestServer(t, us.URL, "k", "s-s-s-s-s-s-s-s-s-s")
	wsAddr := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	for name, origin := range map[string]string{"外站": "http://evil.example.com", "没有Origin": ""} {
		h := http.Header{}
		if origin != "" {
			h.Set("Origin", origin)
		}
		if _, resp, err := websocket.DefaultDialer.Dial(wsAddr, h); err == nil {
			t.Fatalf("%s的WebSocket握手应该被拒绝", name)
		} else if resp == nil || resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s应该返回403, got %v %v", name, resp, err)
		}
	}
	h := http.Header{}
	h.Set("Origin", ts.URL)
	conn, _, err := websocket.DefaultDialer.Dial(wsAddr, h)
	if err != nil {
		t.Fatalf("同源的握手应该通过校验: %v", err)
	}
	conn.Close()
}

// 监听地址检查：默认只允许回环地址
func TestCheckListenAddr(t *testing.T) {
	for _, ok := range []string{"127.0.0.1:8088", "localhost:8088", "[::1]:8088"} {
		if err := checkListenAddr(ok, false); err != nil {
			t.Errorf("%s应该允许: %v", ok, err)
		}
	}
	for _, bad := range []string{"0.0.0.0:8088", "192.168.1.5:8088", ":8088"} {
		if err := checkListenAddr(bad, false); err == nil {
			t.Errorf("%s应该被拒绝", bad)
		}
		if err := checkListenAddr(bad, true); err != nil {
			t.Errorf("%s加了allow-remote应该允许: %v", bad, err)
		}
	}
}

// 上游返回重定向时代理不跟随：请求带着签名头(和API Key)，跟着重定向去了别的地址就把它们送出去了
func TestForward_DoesNotFollowRedirects(t *testing.T) {
	var leaked http.Header
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaked = r.Header.Clone()
	}))
	defer other.Close()
	redirecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/steal", http.StatusFound)
	}))
	defer redirecting.Close()
	ts := newTestServer(t, redirecting.URL, "k1", "secret-secret-secret-1")

	resp, _ := do(t, "GET", ts.URL+"/api/anything", "", guarded)

	if resp.StatusCode != http.StatusFound {
		t.Fatalf("应该把重定向原样交回页面, got %d", resp.StatusCode)
	}
	if leaked != nil {
		t.Fatalf("不能跟着重定向去别的地址，签名头会泄露: %v", leaked)
	}
}

// ---- 两把密钥：交易类请求用trade密钥，页面标了ops的请求用ops密钥 ----

// 假的上游：记录每个请求用的是哪把密钥(X-Api-Key)，并用那把密钥的secret验签；也接受/ws的WebSocket握手
type keyedUpstream struct {
	secrets map[string]string // keyId -> secret
	mu      sync.Mutex
	got     []keyedReq
}

// 已经收到的请求(处理请求的goroutine和测试的goroutine不是同一个，读写都要加锁)
func (u *keyedUpstream) received() []keyedReq {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]keyedReq(nil), u.got...)
}

type keyedReq struct {
	path, key string
	sigOK     bool
	hdr       http.Header
}

func (u *keyedUpstream) handler() http.Handler {
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		key := r.Header.Get("X-Api-Key")
		want := api.SignRequest(u.secrets[key], r.Header.Get("X-Timestamp"), r.Header.Get("X-Nonce"), r.Method, r.URL.EscapedPath(), r.URL.RawQuery, b)
		u.mu.Lock()
		u.got = append(u.got, keyedReq{path: r.URL.EscapedPath(), key: key, sigOK: want == r.Header.Get("X-Signature"), hdr: r.Header.Clone()})
		u.mu.Unlock()
		if r.URL.Path == "/ws" {
			if c, err := up.Upgrade(w, r, nil); err == nil {
				c.Close()
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":200,"data":"pong"}`))
	})
}

func newKeyedServer(t *testing.T, cfg config) (*httptest.Server, *keyedUpstream) {
	t.Helper()
	u := &keyedUpstream{secrets: map[string]string{"ops-key": "ops-secret-ops-secret-1", "trade-key": "trade-secret-trade-1"}}
	us := httptest.NewServer(u.handler())
	t.Cleanup(us.Close)
	cfg.apiURL, cfg.engineURL = us.URL, us.URL
	ts := httptest.NewServer(newServer(cfg).routes())
	t.Cleanup(ts.Close)
	return ts, u
}

var twoKeys = config{keyID: "ops-key", secret: "ops-secret-ops-secret-1", tradeKeyID: "trade-key", tradeSecret: "trade-secret-trade-1"}

// 配了trade密钥：没标运营的请求(下单、查询、深度)用trade密钥，标了X-Sim-Scope: ops的用ops密钥，各自用自己的secret验签；
// 这个头是页面和代理之间的，不转发给后端。合作方的用法就是这样，接口权限范围分错了会直接返回forbidden
func TestScope_TwoKeysAreChosenByTheDeclaredScope(t *testing.T) {
	ts, up := newKeyedServer(t, twoKeys)
	do(t, "GET", ts.URL+"/api/account/info?uid=1", "", guarded)
	do(t, "POST", ts.URL+"/api/order/add", `{}`, guarded)
	do(t, "GET", ts.URL+"/engine/depth?symbol=BTCUSDT", "", guarded)
	do(t, "POST", ts.URL+"/api/account/balance", `{}`, map[string]string{guardHeader: "1", scopeHeader: "ops"})
	do(t, "POST", ts.URL+"/api/index-price", `{}`, map[string]string{guardHeader: "1", scopeHeader: "ops"})

	want := []string{"trade-key", "trade-key", "trade-key", "ops-key", "ops-key"}
	got := up.received()
	if len(got) != len(want) {
		t.Fatalf("收到%d个请求, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].key != w || !got[i].sigOK {
			t.Errorf("请求%d(%s): 用了%s(签名正确=%v), want %s", i, got[i].path, got[i].key, got[i].sigOK, w)
		}
		if got[i].hdr.Get(scopeHeader) != "" {
			t.Errorf("请求%d: %s不该转发给后端", i, scopeHeader)
		}
	}
}

// 只有"ops"这个值才用ops密钥：别的值(大小写不同、乱写)一律当交易类，不能靠随便一个头升级成运营权限
func TestScope_OnlyExactOpsSelectsTheOpsKey(t *testing.T) {
	ts, up := newKeyedServer(t, twoKeys)
	for _, v := range []string{"OPS", "admin", "true", "ops,trade"} {
		do(t, "GET", ts.URL+"/api/account/info?uid=1", "", map[string]string{guardHeader: "1", scopeHeader: v})
	}
	for i, r := range up.received() {
		if r.key != "trade-key" {
			t.Errorf("第%d个: scope值不是ops，不该用ops密钥, got %s", i, r.key)
		}
	}
}

// 没配trade密钥：所有请求都用主密钥(跟以前一样)，不管有没有标ops
func TestScope_SingleKeyUsedForEverythingWhenNoTradeKey(t *testing.T) {
	ts, up := newKeyedServer(t, config{keyID: "ops-key", secret: "ops-secret-ops-secret-1"})
	do(t, "GET", ts.URL+"/api/account/info?uid=1", "", guarded)
	do(t, "POST", ts.URL+"/api/account/balance", `{}`, map[string]string{guardHeader: "1", scopeHeader: "ops"})
	for i, r := range up.received() {
		if r.key != "ops-key" || !r.sigOK {
			t.Errorf("第%d个请求应该用主密钥并验签通过: %+v", i, r)
		}
	}
}

// WebSocket握手需要trade权限：配了trade密钥就用它，没配用主密钥
func TestScope_WebSocketUsesTheTradeKey(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  config
		want string
	}{"两把密钥": {twoKeys, "trade-key"}, "一把密钥": {config{keyID: "ops-key", secret: "ops-secret-ops-secret-1"}, "ops-key"}} {
		t.Run(name, func(t *testing.T) {
			ts, up := newKeyedServer(t, tc.cfg)
			h := http.Header{}
			h.Set("Origin", ts.URL)
			conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http")+"/ws", h)
			if err != nil {
				t.Fatal(err)
			}
			conn.Close()
			time.Sleep(50 * time.Millisecond)
			var ws *keyedReq
			all := up.received()
			for i := range all {
				if all[i].path == "/ws" {
					ws = &all[i]
				}
			}
			if ws == nil || ws.key != tc.want || !ws.sigOK {
				t.Fatalf("WS握手应该用%s签名: %+v", tc.want, ws)
			}
		})
	}
}
