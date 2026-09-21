//go:build integration

package api

import (
	"strings"
	"testing"
)

// 合约的价格步长(priceTick)和数量步长(volumeStep)存在数据库里，通过GET /contract/list和/contract/detail对外提供，
// 对接方靠它们知道输入框的步进；服务端也据此拒绝位数超标的订单。种子数据里跟价格/数量精度一致(10^-scale)

func contractOf(t *testing.T, e *apiEnv, symbol string) map[string]any {
	t.Helper()
	list, _ := e.get(t, "/contract/list")["data"].([]any)
	for _, c := range list {
		if m := c.(map[string]any); m["symbol"] == symbol {
			return m
		}
	}
	t.Fatalf("合约列表里没有%s", symbol)
	return nil
}

// 接口对外提供的精度信息：位数、步长、最小量，跟种子数据一致；list和detail给的一样
func TestContractRules_AreExposedByTheAPI(t *testing.T) {
	e := newAPIEnv(t)
	want := map[string][6]any{ // priceScale, baseCoinScale, priceTick, volumeStep, minVolume, maxVolume
		"BTCUSDT": {float64(1), float64(3), "0.1", "0.001", "0.001", "0"},
		"ETHUSDT": {float64(2), float64(2), "0.01", "0.01", "0.01", "0"},
	}
	for sym, w := range want {
		c := contractOf(t, e, sym)
		got := [6]any{c["priceScale"], c["baseCoinScale"], c["priceTick"], c["volumeStep"], c["minVolume"], c["maxVolume"]}
		if got != w {
			t.Errorf("%s list: got %v, want %v", sym, got, w)
		}
		d := data(t, e.get(t, "/contract/detail?symbol="+sym))
		gotD := [6]any{d["priceScale"], d["baseCoinScale"], d["priceTick"], d["volumeStep"], d["minVolume"], d["maxVolume"]}
		if gotD != w {
			t.Errorf("%s detail: got %v, want %v", sym, gotD, w)
		}
	}
}

// 下单按步长校验：价格不是priceTick的整数倍、数量不是volumeStep的整数倍或低于最小量，都被拒绝，
// 提示里带上具体的步长；市价单不校验价格(价格是参考价)但校验数量；符合规则的正常下单
func TestAddOrder_FollowsContractTickAndStep(t *testing.T) {
	e := newAPIEnv(t)
	uid := e.newAccount(t, 1, "100000")
	order := func(rid string, price any, amount any, typ string) map[string]any {
		b := map[string]any{"uid": uid, "symbol": testSymbol, "side": "long", "action": "open", "type": typ, "amount": amount, "leverage": 10, "requestId": rid}
		if price != nil {
			b["price"] = price
		}
		return e.post(t, "/order/add", b)
	}
	msgOf := func(resp map[string]any) string { m, _ := resp["message"].(string); return m }

	// 价格步长0.1
	resp := order("t1", "65000.15", "0.01", "limit")
	requireErr(t, resp, 400, ErrPriceTickInvalid)
	if !strings.Contains(msgOf(resp), "0.1") {
		t.Fatalf("提示里要带上步长: %q", msgOf(resp))
	}
	requireErr(t, order("t2", "65000.05", "0.01", "limit"), 400, ErrPriceTickInvalid)

	// 数量步长0.001，最小0.001
	resp = order("t3", "65000.1", "0.0015", "limit")
	requireErr(t, resp, 400, ErrVolumeOutOfRange)
	if !strings.Contains(msgOf(resp), "0.001") {
		t.Fatalf("提示里要带上步长: %q", msgOf(resp))
	}
	resp = order("t4", "65000.1", "0.0005", "limit")
	requireErr(t, resp, 400, ErrVolumeOutOfRange)
	requireErr(t, order("t5", nil, "0.0015", "market"), 400, ErrVolumeOutOfRange) // 市价单数量也要符合步长

	// 符合规则的：价格是0.1的整数倍、数量是0.001的整数倍
	if d := data(t, order("t6", "65000.1", "0.001", "limit")); d["orderId"] == nil {
		t.Fatalf("符合规则的委托应该成功: %v", d)
	}
	if d := data(t, order("t7", "65000.0", "0.123", "limit")); d["orderId"] == nil {
		t.Fatalf("%v", d)
	}
}

// 条件单的触发价、委托价、数量同样按步长校验
func TestAddConditionalOrder_FollowsContractTickAndStep(t *testing.T) {
	e := newAPIEnv(t)
	uid := e.newAccount(t, 1, "100000")
	body := func(rid string, trigger, amount any) map[string]any {
		return map[string]any{"uid": uid, "symbol": testSymbol, "side": "long", "action": "open", "type": "market", "amount": amount,
			"leverage": 10, "triggerPrice": trigger, "triggerDirection": "lte", "requestId": rid}
	}
	requireErr(t, e.post(t, "/order/conditional/add", body("c1", "64000.05", "0.01")), 400, ErrPriceTickInvalid)
	requireErr(t, e.post(t, "/order/conditional/add", body("c2", "64000.0", "0.0015")), 400, ErrVolumeOutOfRange)
}
