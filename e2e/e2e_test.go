//go:build e2e

// 端到端冒烟测试：连一个真实跑起来的整套系统(contract-api、contract-engine、MySQL、Redis、Kafka)，
// 只通过对外接口和WebSocket操作，验证"下单 -> Kafka -> 引擎 -> 撮合结算 -> 账户/持仓/推送"这条
// 全链路。集成测试(make test-integration)不起Kafka、直接调服务，覆盖不到消费者订阅、topic发现、
// 重启恢复后消费者是否还活着这类基础设施层的问题——部署验证时手工发现过几个，这个测试把那套手工
// 流程固化下来。由scripts/e2e.sh(make test-e2e)拉起一次性的compose环境并设置下面的环境变量。
package e2e

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"

	"perp-go/internal/api"
)

const symbol = "BTCUSDT"

type env struct {
	apiURL, engineURL string
	keyID, secret     string
	restartEngineCmd  string
}

func loadEnv(t *testing.T) env {
	t.Helper()
	get := func(name string) string {
		v := os.Getenv(name)
		if v == "" {
			t.Fatalf("缺少环境变量%s：端到端测试需要一套真实运行的系统，请用 make test-e2e 运行", name)
		}
		return v
	}
	return env{
		apiURL: get("E2E_API_URL"), engineURL: get("E2E_ENGINE_URL"),
		keyID: get("E2E_KEY_ID"), secret: get("E2E_KEY_SECRET"),
		restartEngineCmd: get("E2E_RESTART_ENGINE_CMD"),
	}
}

type envelope struct {
	Code    int             `json:"code"`
	ErrCode string          `json:"errCode"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func (e envelope) obj(t *testing.T) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(e.Data, &m); err != nil {
		t.Fatalf("data不是对象: %v (%s)", err, e.Data)
	}
	return m
}

func (e envelope) list(t *testing.T) []map[string]any {
	t.Helper()
	var l []map[string]any
	if err := json.Unmarshal(e.Data, &l); err != nil {
		t.Fatalf("data不是数组: %v (%s)", err, e.Data)
	}
	return l
}

func (e envelope) mustOK(t *testing.T, what string) envelope {
	t.Helper()
	if e.Code != 200 {
		t.Fatalf("%s应该成功: code=%d errCode=%s message=%s", what, e.Code, e.ErrCode, e.Message)
	}
	return e
}

func (e envelope) mustFail(t *testing.T, what, errCode string) {
	t.Helper()
	if e.Code == 200 || e.ErrCode != errCode {
		t.Fatalf("%s应该失败且errCode=%s: code=%d errCode=%s message=%s", what, errCode, e.Code, e.ErrCode, e.Message)
	}
}

func nonce() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// 生成带签名的请求头，签名算法直接用生产代码里的api.SignRequest，跟合作方对接时用的是同一套
func (e env) signedHeaders(method, path, rawQuery string, body []byte) http.Header {
	ts := strconv.FormatInt(time.Now().UnixMilli(), 10)
	n := nonce()
	h := http.Header{}
	h.Set("X-Api-Key", e.keyID)
	h.Set("X-Timestamp", ts)
	h.Set("X-Nonce", n)
	h.Set("X-Signature", api.SignRequest(e.secret, ts, n, method, path, rawQuery, body))
	return h
}

func (e env) call(t *testing.T, base, method, rawURL string, body any) envelope {
	t.Helper()
	var raw []byte
	if body != nil {
		raw, _ = json.Marshal(body)
	}
	u, err := url.Parse(base + rawURL)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(method, u.String(), bytes.NewReader(raw))
	for k, v := range e.signedHeaders(method, u.EscapedPath(), u.RawQuery, raw) {
		req.Header[k] = v
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return doRequest(t, req)
}

func doRequest(t *testing.T, req *http.Request) envelope {
	t.Helper()
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("请求%s %s失败: %v", req.Method, req.URL, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var env envelope
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatalf("响应不是JSON: %v (%s)", err, b)
	}
	return env
}

func (e env) post(t *testing.T, path string, body any) envelope {
	return e.call(t, e.apiURL, "POST", path, body)
}
func (e env) get(t *testing.T, path string) envelope { return e.call(t, e.apiURL, "GET", path, nil) }

var uidSeq = uint64(time.Now().UnixMilli()) * 10

func newUID() uint64 { uidSeq++; return uidSeq }

func reqID() string { return "e2e-" + nonce() }

// 建账户并充值
func (e env) newFundedAccount(t *testing.T, amount string) uint64 {
	t.Helper()
	uid := newUID()
	e.post(t, "/account/create", map[string]any{"uid": uid}).mustOK(t, "创建账户")
	e.post(t, "/account/balance", map[string]any{"uid": uid, "amount": json.Number(amount), "requestId": reqID()}).mustOK(t, "充值")
	return uid
}

type orderReq struct {
	uid    uint64
	side   string
	action string
	price  string
	amount string
}

func (e env) placeOrder(t *testing.T, o orderReq) envelope {
	t.Helper()
	return e.post(t, "/order/add", map[string]any{
		"uid": o.uid, "symbol": symbol, "side": o.side, "action": o.action, "type": "limit",
		"price": json.Number(o.price), "amount": json.Number(o.amount), "leverage": 10, "requestId": reqID(),
	})
}

func (e env) accountInfo(t *testing.T, uid uint64) map[string]any {
	t.Helper()
	return e.get(t, fmt.Sprintf("/account/info?uid=%d", uid)).mustOK(t, "查账户").obj(t)
}

func dec(t *testing.T, v any) decimal.Decimal {
	t.Helper()
	s, ok := v.(string)
	if !ok {
		t.Fatalf("期望是字符串小数, got %T %v", v, v)
	}
	d, err := decimal.NewFromString(s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func eventually(t *testing.T, what string, timeout time.Duration, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := ""
	for time.Now().Before(deadline) {
		ok, detail := cond()
		if ok {
			return
		}
		last = detail
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("等待超时(%s): %s", timeout, what+"; 最后一次状态: "+last)
}

func fieldIs(t *testing.T, m map[string]any, field, want string) (bool, string) {
	t.Helper()
	got := dec(t, m[field])
	return got.Equal(decimal.RequireFromString(want)), fmt.Sprintf("%s=%s(期望%s)", field, got, want)
}

// ---- 用例 ----

// 整套系统起来了；一次性设好指数价，让限价单的价格保护带有参照
func TestE2E_01_HealthAndSetup(t *testing.T) {
	e := loadEnv(t)
	for name, base := range map[string]string{"contract-api": e.apiURL, "contract-engine": e.engineURL} {
		resp, err := http.Get(base + "/health")
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("%s的/health不健康: %v %v", name, resp, err)
		}
		resp.Body.Close()
	}
	e.post(t, "/index-price", map[string]any{"symbol": symbol, "price": 65000}).mustOK(t, "设置指数价")
}

// 没签名、签名不对都被挡在外面
func TestE2E_02_AuthRejectsUnsignedAndBadSignature(t *testing.T) {
	e := loadEnv(t)
	req, _ := http.NewRequest("GET", e.apiURL+"/account/info?uid=1", nil)
	doRequest(t, req).mustFail(t, "没有签名头", "auth_missing")

	bad := env{apiURL: e.apiURL, engineURL: e.engineURL, keyID: e.keyID, secret: e.secret + "-wrong"}
	bad.get(t, "/account/info?uid=1").mustFail(t, "签名不对", "auth_invalid_signature")
}

// 下单 -> Kafka -> 引擎撮合 -> 双方成交结算：余额、仓位保证金、权益都对
func TestE2E_03_OrderFlowThroughKafkaFillsAndSettles(t *testing.T) {
	e := loadEnv(t)
	short := e.newFundedAccount(t, "10000")
	long := e.newFundedAccount(t, "10000")
	e.placeOrder(t, orderReq{short, "short", "open", "65000", "0.1"}).mustOK(t, "maker挂卖单")
	e.placeOrder(t, orderReq{long, "long", "open", "65000", "0.1"}).mustOK(t, "taker买入吃单")

	eventually(t, "多头仓位建立", 20*time.Second, func() (bool, string) {
		pos := e.get(t, fmt.Sprintf("/position/current?uid=%d", long)).mustOK(t, "查持仓").list(t)
		if len(pos) != 1 {
			return false, fmt.Sprintf("持仓数=%d", len(pos))
		}
		return fieldIs(t, pos[0], "volume", "0.1")
	})
	// 0.1 BTC*65000=6500名义价值：保证金650，taker手续费3.25，maker手续费1.3
	acc := e.accountInfo(t, long)
	for field, want := range map[string]string{"available": "9346.75", "positionMargin": "650", "frozenMargin": "0", "equity": "9996.75"} {
		if ok, detail := fieldIs(t, acc, field, want); !ok {
			t.Fatalf("多头账户字段不对: %s", detail)
		}
	}
	eventually(t, "空头账户结算", 20*time.Second, func() (bool, string) {
		return fieldIs(t, e.accountInfo(t, short), "available", "9348.7")
	})
}

// 挂单冻结保证金，撤单事件经Kafka到引擎，摘单并退回
func TestE2E_04_CancelFlowThroughKafkaRefundsMargin(t *testing.T) {
	e := loadEnv(t)
	uid := e.newFundedAccount(t, "10000")
	placed := e.placeOrder(t, orderReq{uid, "long", "open", "64000", "0.1"}).mustOK(t, "挂单").obj(t)
	orderID := placed["orderId"].(string)
	eventually(t, "挂单冻结保证金640", 20*time.Second, func() (bool, string) {
		return fieldIs(t, e.accountInfo(t, uid), "frozenMargin", "640")
	})

	e.post(t, "/order/cancel/"+orderID, map[string]any{"uid": uid}).mustOK(t, "撤单")

	eventually(t, "撤单后保证金退回", 20*time.Second, func() (bool, string) {
		acc := e.accountInfo(t, uid)
		if ok, d := fieldIs(t, acc, "frozenMargin", "0"); !ok {
			return false, d
		}
		return fieldIs(t, acc, "available", "10000")
	})
}

// 冻结账户：开仓被拒；存量开仓挂单被清理(撤单事件经Kafka)；解冻后恢复
func TestE2E_05_FreezeFlowSweepsOrdersAndBlocksNewOnes(t *testing.T) {
	e := loadEnv(t)
	uid := e.newFundedAccount(t, "10000")
	e.placeOrder(t, orderReq{uid, "long", "open", "64000", "0.1"}).mustOK(t, "冻结前挂单")
	eventually(t, "挂单冻结保证金", 20*time.Second, func() (bool, string) {
		return fieldIs(t, e.accountInfo(t, uid), "frozenMargin", "640")
	})

	res := e.post(t, "/account/status", map[string]any{"uid": uid, "status": "frozen", "reason": "e2e"}).mustOK(t, "冻结").obj(t)
	if res["cancelRequested"] != float64(1) || res["changed"] != true {
		t.Fatalf("冻结应该清理1笔开仓挂单: %v", res)
	}
	e.placeOrder(t, orderReq{uid, "long", "open", "64000", "0.1"}).mustFail(t, "冻结后开仓", "account_frozen")
	eventually(t, "存量开仓挂单被撤、保证金退回", 20*time.Second, func() (bool, string) {
		return fieldIs(t, e.accountInfo(t, uid), "frozenMargin", "0")
	})

	e.post(t, "/account/status", map[string]any{"uid": uid, "status": "active"}).mustOK(t, "解冻")
	e.placeOrder(t, orderReq{uid, "long", "open", "64000", "0.1"}).mustOK(t, "解冻后开仓")
}

// 结束本轮：事件经Kafka到引擎，round推进、信用额度清零、投保复位
func TestE2E_06_RoundCloseFlowThroughKafka(t *testing.T) {
	e := loadEnv(t)
	uid := e.newFundedAccount(t, "1000")
	e.post(t, "/account/credit", map[string]any{"uid": uid, "amount": 500, "requestId": reqID()}).mustOK(t, "发信用额度")
	e.post(t, "/account/insured", map[string]any{"uid": uid, "insured": true}).mustOK(t, "设投保")
	if ok, d := fieldIs(t, e.accountInfo(t, uid), "credit", "500"); !ok {
		t.Fatalf("信用额度应该是500: %s", d)
	}

	e.post(t, "/account/round/close", map[string]any{"uid": uid, "round": 0}).mustOK(t, "结束本轮")

	eventually(t, "round推进到1、信用额度清零", 20*time.Second, func() (bool, string) {
		acc := e.accountInfo(t, uid)
		if acc["round"] != float64(1) {
			return false, fmt.Sprintf("round=%v", acc["round"])
		}
		if acc["isInsured"] != false {
			return false, "投保状态还没复位"
		}
		return fieldIs(t, acc, "credit", "0")
	})
}

// WebSocket：握手要签名；订阅后，下单引起的深度变化和账户快照能实时推过来
func TestE2E_07_WebSocketPushesDepthAndUserSnapshot(t *testing.T) {
	e := loadEnv(t)
	wsURL := "ws" + strings.TrimPrefix(e.apiURL, "http") + "/ws"

	// 项目约定HTTP状态码固定200、错误信息在body里，握手被拒时没有升级成WebSocket，客户端拿到的是
	// 一个普通的JSON响应
	if _, resp, err := websocket.DefaultDialer.Dial(wsURL, nil); err == nil {
		t.Fatal("没有签名的WebSocket握手应该被拒绝")
	} else if resp == nil {
		t.Fatalf("握手被拒时应该能拿到HTTP响应: %v", err)
	} else {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var env envelope
		if json.Unmarshal(body, &env) != nil || env.ErrCode != "auth_missing" {
			t.Fatalf("没有签名的握手应该返回auth_missing, got %s", body)
		}
	}

	uid := e.newFundedAccount(t, "10000")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, e.signedHeaders("GET", "/ws", "", nil))
	if err != nil {
		t.Fatalf("签名的WebSocket握手失败: %v", err)
	}
	defer conn.Close()
	sub := map[string]any{"op": "subscribe", "channels": []string{fmt.Sprintf("user:%d", uid), "depth:" + symbol}}
	if err := conn.WriteJSON(sub); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond) // 让Hub先在Redis上订阅好，再触发推送

	// 用一个独特的价格，确保收到的深度里能认出是这笔单子
	e.placeOrder(t, orderReq{uid, "long", "open", "64111", "0.1"}).mustOK(t, "挂单")

	gotDepth, gotUser := false, false
	deadline := time.Now().Add(20 * time.Second)
	for !(gotDepth && gotUser) && time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		var msg struct {
			Channel string          `json:"channel"`
			Data    json.RawMessage `json:"data"`
		}
		if err := conn.ReadJSON(&msg); err != nil {
			t.Fatalf("读WebSocket消息失败(已收到 depth=%v user=%v): %v", gotDepth, gotUser, err)
		}
		switch msg.Channel {
		case "depth:" + symbol:
			if strings.Contains(string(msg.Data), "64111") {
				gotDepth = true
			}
		case fmt.Sprintf("user:%d", uid):
			if strings.Contains(string(msg.Data), `"activeOrders":[{`) {
				gotUser = true
			}
		}
	}
	if !gotDepth || !gotUser {
		t.Fatalf("应该收到包含新挂单的深度推送和账户快照: depth=%v user=%v", gotDepth, gotUser)
	}
}

func (e env) depthHasBid(t *testing.T, price string) bool {
	t.Helper()
	d := e.call(t, e.engineURL, "GET", "/depth?symbol="+symbol, nil).mustOK(t, "引擎/depth").obj(t)
	raw, _ := json.Marshal(d["bids"])
	return strings.Contains(string(raw), price)
}

// 必须放在最后：会重启引擎。重启后订单簿要从数据库恢复挂单，Kafka消费者要重新订阅上——部署验证时
// 手工发现过"消费者收不到新消息"这类问题，只有真的重启一次引擎、再发一笔会成交的单才能验证。
// 前面的用例会在订单簿里留下一些买单(比如64111、64000)，撮合是价格优先，卖单会先吃最高的买价；所以
// 这里的买单要挂得比它们都高(64500，仍在价格保护带内)，才能保证卖单吃到的是这一笔
func TestE2E_99_EngineRestartRecoversOrderBookAndKeepsConsuming(t *testing.T) {
	const price = "64500"
	e := loadEnv(t)
	maker := e.newFundedAccount(t, "10000")
	taker := e.newFundedAccount(t, "10000")
	e.placeOrder(t, orderReq{maker, "long", "open", price, "0.1"}).mustOK(t, "重启前挂买单")
	eventually(t, "引擎订单簿里出现这笔买单", 20*time.Second, func() (bool, string) {
		return e.depthHasBid(t, price), "订单簿里还没有" + price
	})

	out, err := exec.Command("sh", "-c", e.restartEngineCmd).CombinedOutput()
	if err != nil {
		t.Fatalf("重启引擎失败: %v\n%s", err, out)
	}
	eventually(t, "引擎重启后健康", 60*time.Second, func() (bool, string) {
		resp, err := http.Get(e.engineURL + "/health")
		if err != nil {
			return false, err.Error()
		}
		resp.Body.Close()
		return resp.StatusCode == 200, fmt.Sprintf("status=%d", resp.StatusCode)
	})
	eventually(t, "重启后订单簿从数据库恢复出这笔买单", 20*time.Second, func() (bool, string) {
		return e.depthHasBid(t, price), "订单簿里没有" + price
	})

	// 重启后的消费者还活着：一笔会成交的卖单发出去，要能被引擎撮合
	e.placeOrder(t, orderReq{taker, "short", "open", price, "0.1"}).mustOK(t, "重启后挂卖单")
	eventually(t, "重启后新下的单能被消费并成交(maker出现多头仓位)", 30*time.Second, func() (bool, string) {
		pos := e.get(t, fmt.Sprintf("/position/current?uid=%d", maker)).mustOK(t, "查持仓").list(t)
		if len(pos) != 1 {
			return false, fmt.Sprintf("持仓数=%d", len(pos))
		}
		return fieldIs(t, pos[0], "volume", "0.1")
	})
	if e.depthHasBid(t, price) {
		t.Fatal("成交后订单簿里不应该还有这笔买单")
	}
}
