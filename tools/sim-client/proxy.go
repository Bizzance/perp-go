package main

import (
	"bytes"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"perp-go/internal/api"
)

//go:embed web
var webFS embed.FS

// 页面调用代理时必须带的请求头。浏览器里的跨站脚本没法在不触发CORS预检的前提下加自定义请求头，
// 而这个代理不响应预检，所以别的网站的页面没法借用户的浏览器去调这个持有ops密钥的代理
const guardHeader = "X-Sim-Client"

// 页面标明这次调用是运营类接口(充值、冻结、发额度、设投保、设指数价)的请求头，值为"ops"。合作方的交易后端用trade密钥、
// 运营用ops密钥，所以这里也一样：标了ops的用ops密钥签名，其余的用trade密钥(配置了的话)。由页面(而不是代理)决定，
// 这样页面选错密钥、或者服务端把某个接口的权限范围分错了，都会直接返回forbidden暴露出来。这个头不转发给后端
const scopeHeader = "X-Sim-Scope"

type server struct {
	cfg         config
	signer      signer // 主密钥(ops)：没配trade密钥时所有请求都用它
	tradeSigner signer // trade密钥，keyID为空表示没配
	client      *http.Client
}

func newServer(cfg config) *server {
	return &server{cfg: cfg, signer: signer{keyID: cfg.keyID, secret: cfg.secret}, tradeSigner: signer{keyID: cfg.tradeKeyID, secret: cfg.tradeSecret}, client: &http.Client{Timeout: 20 * time.Second, CheckRedirect: noRedirect}}
}

// 这次请求用哪把密钥签名：页面标了ops的用主密钥；否则配置了trade密钥就用它，没配就还是主密钥
func (s *server) signerFor(r *http.Request) signer {
	if r.Header.Get(scopeHeader) == "ops" || s.tradeSigner.keyID == "" {
		return s.signer
	}
	return s.tradeSigner
}

// 不跟随重定向：请求带着签名头，跟着重定向去了别的地址就把签名(和API Key)送出去了；后端本来也不会重定向
func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/api/", s.guard(s.forward(s.cfg.apiURL, "/api")))
	mux.Handle("/engine/", s.guard(s.forward(s.cfg.engineURL, "/engine")))
	mux.Handle("/ws", s.guardWS(http.HandlerFunc(s.ws)))
	mux.Handle("/sim/config", s.guard(http.HandlerFunc(s.simConfig)))
	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		log.Fatal(err)
	}
	static := http.FileServer(http.FS(sub))
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store") // 开发工具：改了页面刷新就要生效
		static.ServeHTTP(w, r)
	}))
	return s.hostCheck(mux)
}

// 只接受用回环名字访问的请求，防DNS重绑定：攻击者的域名解析到127.0.0.1后，浏览器发出的请求Host头
// 仍然是攻击者的域名，这里就能识别出来拒绝
func (s *server) hostCheck(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.hostAllowed(r.Host) {
			http.Error(w, "forbidden host", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *server) hostAllowed(hostport string) bool {
	if s.cfg.allowRemote {
		return true
	}
	host, _, err := net.SplitHostPort(hostport)
	if err != nil {
		host = hostport
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

func (s *server) guard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(guardHeader) == "" {
			http.Error(w, "missing "+guardHeader, http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// WebSocket握手没法带自定义请求头，改校验Origin：必须是本代理自己的页面发起的
func (s *server) guardWS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !sameOrigin(r) {
			http.Error(w, "forbidden origin", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}
	u, err := url.Parse(origin)
	return err == nil && u.Host == r.Host
}

// ---- 签名 ----

type signer struct{ keyID, secret string }

// 生成签名请求头，签名直接用生产代码里的api.SignRequest，跟合作方对接用的是同一套。
// 没配密钥返回nil(不签名)
func (s signer) headers(method, escapedPath, rawQuery string, body []byte) http.Header {
	if s.keyID == "" {
		return nil
	}
	ts := strconv.FormatInt(time.Now().UnixMilli(), 10)
	nb := make([]byte, 12)
	_, _ = rand.Read(nb)
	nonce := hex.EncodeToString(nb)
	h := http.Header{}
	h.Set("X-Api-Key", s.keyID)
	h.Set("X-Timestamp", ts)
	h.Set("X-Nonce", nonce)
	h.Set("X-Signature", api.SignRequest(s.secret, ts, nonce, method, escapedPath, rawQuery, body))
	return h
}

// ---- HTTP转发 ----

const maxBodyBytes = 1 << 20

// 把页面的请求原样(方法、路径、查询串、请求体)签名后转发给上游，响应原样回给页面。
// 路径用EscapedPath、查询串用原始字符串签名，跟服务端验签用的是同一份
func (s *server) forward(base, strip string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
		if err != nil {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		target, err := url.Parse(base + strings.TrimPrefix(r.URL.EscapedPath(), strip))
		if err != nil {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		target.RawQuery = r.URL.RawQuery
		req, err := http.NewRequestWithContext(r.Context(), r.Method, target.String(), bytes.NewReader(body))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if ct := r.Header.Get("Content-Type"); ct != "" {
			req.Header.Set("Content-Type", ct)
		}
		for k, v := range s.signerFor(r).headers(r.Method, target.EscapedPath(), target.RawQuery, body) {
			req.Header[k] = v
		}
		resp, err := s.client.Do(req)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"code": 502, "errCode": "upstream_unreachable", "message": "连不上后端: " + err.Error()})
			return
		}
		defer resp.Body.Close()
		if ct := resp.Header.Get("Content-Type"); ct != "" {
			w.Header().Set("Content-Type", ct)
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *server) simConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"authEnabled": s.cfg.keyID != "",
		"apiURL":      s.cfg.apiURL,
	})
}

// ---- WebSocket中继 ----

var upgrader = websocket.Upgrader{CheckOrigin: sameOrigin}

func wsURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

// 页面 <-> 代理 <-> contract-api的/ws。上游握手由代理签名；之后订阅、推送都是原样中继
func (s *server) ws(w http.ResponseWriter, r *http.Request) {
	browser, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer browser.Close()

	// /ws需要trade权限，没配trade密钥时用主密钥
	up, resp, err := websocket.DefaultDialer.Dial(wsURL(s.cfg.apiURL)+"/ws", s.signerFor(r).headers("GET", "/ws", "", nil))
	if err != nil {
		msg := "连不上后端WebSocket: " + err.Error()
		if resp != nil {
			// 握手被拒时后端返回的是普通JSON响应(比如鉴权失败)，把里面的信息带给页面
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
			resp.Body.Close()
			msg += " " + string(b)
		}
		_ = browser.WriteJSON(map[string]any{"channel": "sim:error", "data": msg})
		return
	}
	defer up.Close()

	var once sync.Once
	done := make(chan struct{})
	finish := func() { once.Do(func() { close(done) }) }
	relay := func(from, to *websocket.Conn) {
		defer finish()
		for {
			mt, msg, err := from.ReadMessage()
			if err != nil {
				return
			}
			if err := to.WriteMessage(mt, msg); err != nil {
				return
			}
		}
	}
	go relay(browser, up)
	go relay(up, browser)
	<-done
}
