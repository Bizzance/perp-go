//go:build integration

package api

import (
	"context"
	"testing"

	"github.com/shopspring/decimal"
)

// 直接落库一个多头仓位(不走撮合)：0.1 BTC，开仓均价65000，保证金650，10倍杠杆
func (e *apiEnv) seedLongPosition(t *testing.T, uid uint64) {
	t.Helper()
	if _, err := e.db.Exec(`INSERT INTO positions (uid, symbol, side, volume, avg_entry_price, position_margin, credit_margin, leverage, status, update_time)
		VALUES (?, ?, 'long', 0.1, 65000, 650, 0, 10, 'normal', 1)`, uid, testSymbol); err != nil {
		t.Fatal(err)
	}
}

func (e *apiEnv) setMark(t *testing.T, price int64) {
	t.Helper()
	if err := e.srv.markPrice.UpdateFromTrade(context.Background(), testSymbol, decimal.NewFromInt(price)); err != nil {
		t.Fatal(err)
	}
}

// 通过真实的下单接口：账户浮亏累累、满足强平条件(权益21.75 <= 维持保证金22.1)，available还有346.75，
// 新开仓也要被拒绝(insufficient_margin)——买力要扣浮亏；标记价回到没有浮亏时同样的委托就能下成功。
// 之前账户只看available，浮亏975的账户能拿346.75去开新仓
func TestAddOrder_RejectsNewOpenWhenUnrealizedLossExhaustsBuyingPower(t *testing.T) {
	e := newAPIEnv(t)
	uid := e.newAccount(t, 1, "346.75") // 已经扣过保证金650和手续费3.25的余额
	e.seedLongPosition(t, uid)

	e.setMark(t, 55250) // 0.1*(55250-65000)=-975
	loss := map[string]any{"uid": uid, "symbol": testSymbol, "side": "long", "action": "open", "type": "limit",
		"price": 55000, "amount": 0.05, "leverage": 10, "requestId": "bp-1"} // 需要保证金275，available够
	requireErr(t, e.post(t, "/order/add", loss), 400, "insufficient_margin")
	if !e.account(t, uid).FrozenMargin.IsZero() {
		t.Fatal("被拒绝的委托不应该冻结保证金")
	}

	e.setMark(t, 65000) // 没有浮亏了
	ok := map[string]any{"uid": uid, "symbol": testSymbol, "side": "long", "action": "open", "type": "limit",
		"price": 64500, "amount": 0.05, "leverage": 10, "requestId": "bp-2"} // 需要保证金322.5
	if d := data(t, e.post(t, "/order/add", ok)); d["orderId"] == nil {
		t.Fatalf("没有浮亏时同样能下的委托应该成功: %v", d)
	}
}
