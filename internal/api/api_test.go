package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"

	"perp-go/internal/model"
	"perp-go/internal/service"
)

func init() { gin.SetMode(gin.TestMode) }

func newCtx(query string) (*gin.Context, *httptest.ResponseRecorder) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/x?"+query, nil)
	return c, w
}

func body(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil {
		t.Fatalf("响应不是合法JSON: %v, body=%s", err, w.Body.String())
	}
	return m
}

func TestNormalizeRequestID(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{"", "", false},
		{"abc-123_X", "abc-123_X", false},
		{"a b", "", true},
		{"中文", "", true},
		{"a/b", "", true},
	}
	for _, tc := range cases {
		got, msg := normalizeRequestID(tc.in)
		if (msg != "") != tc.wantErr || got != tc.want {
			t.Errorf("normalizeRequestID(%q)=(%q,%q), want (%q, err=%v)", tc.in, got, msg, tc.want, tc.wantErr)
		}
	}
	long := make([]byte, 65)
	for i := range long {
		long[i] = 'a'
	}
	if _, msg := normalizeRequestID(string(long)); msg == "" {
		t.Error("65个字符应该被拒绝")
	}
}

func TestPageParams(t *testing.T) {
	c, _ := newCtx("")
	limit, before, ok := pageParams(c)
	if !ok || limit != 100 || before != 0 {
		t.Errorf("默认值应该是limit=100,before=0, got %d,%d,%v", limit, before, ok)
	}

	c, _ = newCtx("limit=500&before=226750310570262528")
	limit, before, ok = pageParams(c)
	if !ok || limit != 500 || before != 226750310570262528 {
		t.Errorf("got %d,%d,%v", limit, before, ok)
	}

	// 超出上限必须拒绝，不能悄悄截断
	for _, q := range []string{"limit=501", "limit=0", "limit=-1", "limit=abc", "before=abc", "before=0", "before=-5"} {
		c, w := newCtx(q)
		if _, _, ok := pageParams(c); ok {
			t.Errorf("%s 应该被拒绝", q)
			continue
		}
		m := body(t, w)
		if m["errCode"] != ErrInvalidParam {
			t.Errorf("%s 的errCode应该是invalid_param, got %v", q, m["errCode"])
		}
	}
}

func TestFailCarriesErrCode(t *testing.T) {
	c, w := newCtx("")
	failC(c, 400, ErrInsufficientMargin, "余额不足")
	m := body(t, w)
	if w.Code != http.StatusOK {
		t.Errorf("HTTP状态码固定200, got %d", w.Code)
	}
	if m["code"] != float64(400) || m["errCode"] != "insufficient_margin" || m["message"] != "余额不足" {
		t.Errorf("unexpected body %v", m)
	}
}

func TestFailDefaultErrCodes(t *testing.T) {
	for code, want := range map[int]string{400: ErrInvalidParam, 429: ErrServerBusy, 500: ErrInternal, 503: ErrInternal} {
		c, w := newCtx("")
		fail(c, code, "x")
		if got := body(t, w)["errCode"]; got != want {
			t.Errorf("code=%d 兜底errCode应该是%s, got %v", code, want, got)
		}
	}
}

// 列表类接口"没有数据"必须是[]，不能是null
func TestOkSerializesNilSliceAsEmptyArray(t *testing.T) {
	var nilSlice []string
	c, w := newCtx("")
	ok(c, nilSlice)
	if got := w.Body.String(); !contains(got, `"data":[]`) {
		t.Errorf("nil切片应该序列化成[], got %s", got)
	}
	c, w = newCtx("")
	ok(c, nil)
	if got := w.Body.String(); !contains(got, `"data":null`) {
		t.Errorf("nil本身保持null, got %s", got)
	}
}

func TestPlaceOrderResultJSON(t *testing.T) {
	b, _ := json.Marshal(placeOrderResult{OrderID: 226750310570262528, RequestID: "c1", Duplicate: true})
	if string(b) != `{"orderId":"226750310570262528","requestId":"c1","duplicate":true}` {
		t.Errorf("got %s", b)
	}
	b, _ = json.Marshal(placeOrderResult{OrderID: 5})
	if string(b) != `{"orderId":"5"}` {
		t.Errorf("没传requestId且不是重复请求时不带这两个字段, got %s", b)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestRequireRequestID(t *testing.T) {
	if _, msg := requireRequestID(""); msg == "" {
		t.Error("资金类接口的requestId必填，空串应该被拒绝")
	}
	if got, msg := requireRequestID("dep-001"); msg != "" || got != "dep-001" {
		t.Errorf("合法的requestId应该通过, got (%q,%q)", got, msg)
	}
	if _, msg := requireRequestID("bad id"); msg == "" {
		t.Error("格式不合法的requestId应该被拒绝")
	}
}

func TestDecPtrStr_DistinguishesNilFromZero(t *testing.T) {
	zero := decimal.Zero
	one := decimal.RequireFromString("1.50")
	if decPtrStr(nil) == decPtrStr(&zero) {
		t.Error("没传和传了0在指纹里必须能区分开")
	}
	if got := decPtrStr(&one); got != "1.5" {
		t.Errorf("小数应该规范化(去掉末尾的0), got %s", got)
	}
}

func TestIdempotencyConflict(t *testing.T) {
	a, b := "hash-a", "hash-b"
	empty := ""
	cases := []struct {
		name   string
		stored *string
		cur    string
		want   bool
	}{
		{"参数一致", &a, "hash-a", false},
		{"参数不一致", &a, "hash-b", true},
		{"历史数据没存过指纹", nil, "hash-b", false},
		{"指纹是空串", &empty, "hash-b", false},
	}
	for _, tc := range cases {
		if got := idempotencyConflict(tc.stored, tc.cur); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.name, got, tc.want)
		}
	}
	_ = b
}

func TestHashIfKeyed(t *testing.T) {
	if hashIfKeyed("", "h") != nil {
		t.Error("没传requestId不存指纹")
	}
	if p := hashIfKeyed("r1", "h"); p == nil || *p != "h" {
		t.Error("传了requestId要存指纹")
	}
}

func TestFundOpAndCloseRoundResultJSON(t *testing.T) {
	b, _ := json.Marshal(fundOpResult{RequestID: "r1"})
	if string(b) != `{"requestId":"r1"}` {
		t.Errorf("got %s", b)
	}
	b, _ = json.Marshal(fundOpResult{RequestID: "r1", Duplicate: true})
	if string(b) != `{"requestId":"r1","duplicate":true}` {
		t.Errorf("got %s", b)
	}
	b, _ = json.Marshal(closeRoundResult{Round: 1, Status: "submitted"})
	if string(b) != `{"round":1,"status":"submitted"}` {
		t.Errorf("round字段必须序列化出来, got %s", b)
	}
}

func TestCreateAccountResultJSON_FlattensAccountViewAndCreatedFlag(t *testing.T) {
	view := &service.AccountView{UID: 10001, Round: 1}
	b, _ := json.Marshal(createAccountResult{AccountView: view, Created: true})
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if m["uid"] != float64(10001) || m["created"] != true {
		t.Errorf("uid和created应该在同一层, got %s", b)
	}
	for _, k := range []string{"round", "available", "credit", "equity", "isInsured", "positionMargin"} {
		if _, ok := m[k]; !ok {
			t.Errorf("账户视图的字段%s应该被展开到同一层, got %s", k, b)
		}
	}
}

func TestRejectIfFrozen(t *testing.T) {
	c, w := newCtx("")
	if rejectIfFrozen(c, &model.Account{Status: model.AccountStatusActive}) {
		t.Fatal("active账户不应该被拒绝")
	}
	if w.Body.Len() != 0 {
		t.Fatalf("active账户不应该写响应: %s", w.Body.String())
	}

	c, w = newCtx("")
	if !rejectIfFrozen(c, &model.Account{Status: model.AccountStatusFrozen}) {
		t.Fatal("frozen账户应该被拒绝")
	}
	m := body(t, w)
	if m["errCode"] != "account_frozen" || m["code"].(float64) != 400 {
		t.Fatalf("冻结拒绝响应不对: %v", m)
	}
}

func TestAccountViewJSON_IncludesStatus(t *testing.T) {
	v := &service.AccountView{UID: 10001, Status: string(model.AccountStatusFrozen)}
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(string(b), `"status":"frozen"`) {
		t.Fatalf("账户信息缺少status: %s", b)
	}
}

func TestSetAccountStatusResultJSON(t *testing.T) {
	b, err := json.Marshal(setAccountStatusResult{UID: 10001, Status: model.AccountStatusFrozen, Changed: true, CancelRequested: 2})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"uid", "status", "changed", "cancelRequested", "cancelRequestFailed", "conditionalCanceled", "conditionalFailed"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("冻结结果缺少字段%s: %s", k, b)
		}
	}
	if m["status"] != "frozen" || m["changed"] != true {
		t.Fatalf("字段取值不对: %s", b)
	}
}
