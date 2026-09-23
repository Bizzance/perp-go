//go:build integration

package api

import (
	"strconv"
	"testing"
)

// 下单接口的marginAmount(按保证金金额下单)：服务端按 数量 = 保证金 × 杠杆 ÷ 价格 换算，向下取到数量精度。
// 价格：限价单用委托价，市价单用标记价。这一组测试锁定现有行为，之前没有任何测试

func (e *apiEnv) currentAmounts(t *testing.T, uid uint64) []string {
	t.Helper()
	resp := e.get(t, "/order/current?uid="+strconv.FormatUint(uid, 10)+"&symbol="+testSymbol)
	list, _ := resp["data"].([]any)
	out := make([]string, len(list))
	for i, o := range list {
		out[i], _ = o.(map[string]any)["amount"].(string)
	}
	return out
}

func TestMarginAmount_ConvertsToAmountByLeverageAndPrice(t *testing.T) {
	e := newAPIEnv(t) // 标记价65000
	uid := e.newAccount(t, 1, "100000")
	order := func(rid string, typ string, extra map[string]any) map[string]any {
		b := map[string]any{"uid": uid, "symbol": testSymbol, "side": "long", "action": "open", "type": typ, "leverage": 10, "requestId": rid}
		for k, v := range extra {
			b[k] = v
		}
		return e.post(t, "/order/add", b)
	}

	// 限价单：保证金650 × 杠杆10 ÷ 委托价65000.0 = 0.1
	data(t, order("m1", "limit", map[string]any{"price": "65000.0", "marginAmount": "650"}))
	// 市价单：用标记价65000换算，同样是0.1
	data(t, order("m2", "market", map[string]any{"marginAmount": "650"}))
	// 两个都传：拒绝，不允许有歧义
	requireErr(t, order("m3", "limit", map[string]any{"price": "65000.0", "marginAmount": "650", "amount": "5"}), 400, ErrInvalidParam)
	// 向下取到数量精度(BTC是3位)：100 × 3 ÷ 65000.0 = 0.004615...，向下取成0.004
	resp := e.post(t, "/order/add", map[string]any{"uid": uid, "symbol": testSymbol, "side": "long", "action": "open", "type": "limit",
		"leverage": 3, "requestId": "m4", "price": "65000.0", "marginAmount": "100"})
	data(t, resp)

	got := e.currentAmounts(t, uid)
	want := map[string]int{"0.1": 1, "0.004": 1} // 市价单立刻按盘口成交(测试里订单簿是空的，剩余撤销)，不在当前委托里
	count := map[string]int{}
	for _, a := range got {
		count[a]++
	}
	if count["0.1"] < 1 || count["0.004"] != want["0.004"] {
		t.Fatalf("当前委托的数量 = %v, want 至少一笔0.1和一笔0.004", got)
	}

	// 保证金太小，换算出来数量是0：拒绝
	requireErr(t, order("m5", "limit", map[string]any{"price": "65000.0", "marginAmount": "0.5"}), 400, ErrInvalidParam)
	// 没传marginAmount也没传amount：拒绝
	requireErr(t, order("m6", "limit", map[string]any{"price": "65000.0"}), 400, ErrInvalidParam)
	// marginAmount不合法
	requireErr(t, order("m7", "limit", map[string]any{"price": "65000.0", "marginAmount": "0"}), 400, ErrInvalidParam)
	// 换算出来的数量同样要符合最小量：保证金1 × 杠杆10 ÷ 65000.0 = 0.000153 -> 0.000，不足
	requireErr(t, order("m8", "limit", map[string]any{"price": "65000.0", "marginAmount": "1"}), 400, ErrInvalidParam)
}

// 条件单创建接口跟/order/add共用同一套marginAmount/amount二选一校验，这里锁定"两个都传拒绝"这一条
// (换算逻辑本身已经被上面那组测试覆盖，不重复验证)
func TestMarginAmount_ConditionalOrderRejectsBothProvided(t *testing.T) {
	e := newAPIEnv(t) // 标记价65000
	uid := e.newAccount(t, 1, "100000")
	requireErr(t, e.post(t, "/order/conditional/add", map[string]any{"uid": uid, "symbol": testSymbol, "side": "long",
		"action": "open", "triggerPrice": "60000", "triggerDirection": "lte", "type": "limit", "price": "60000",
		"leverage": 10, "marginAmount": "650", "amount": "5", "requestId": "co1"}), 400, ErrInvalidParam)
}
