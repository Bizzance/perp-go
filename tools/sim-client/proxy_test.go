package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
