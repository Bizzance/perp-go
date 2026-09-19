package model

import (
	"encoding/json"
	"testing"

	"github.com/shopspring/decimal"
)

func toMap(t *testing.T, v any) map[string]any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// 雪花ID超过JS的Number.MAX_SAFE_INTEGER(2^53-1)，必须序列化成字符串，不然经过JS/double
// 解析会丢精度
func TestOrderJSON_SnowflakeIDIsStringAndKeysAreCamelCase(t *testing.T) {
	cid := "abc-1"
	o := Order{OrderID: 226750310570262528, UID: 10001, Symbol: "BTCUSDT", Side: SideLong, Action: ActionOpen,
		Type: OrderTypeLimit, Price: decimal.NewFromInt(65000), Amount: decimal.RequireFromString("0.1"),
		Status: OrderStatusOpen, CreateTime: 1, UpdateTime: 2, RequestID: &cid}
	m := toMap(t, o)

	if got, ok := m["orderId"].(string); !ok || got != "226750310570262528" {
		t.Fatalf("orderId应该是字符串, got %#v", m["orderId"])
	}
	for _, k := range []string{"uid", "symbol", "side", "action", "type", "price", "amount", "tradedAmount",
		"avgDealPrice", "frozenMargin", "frozenCredit", "leverage", "reduceOnly", "liquidation", "status",
		"createTime", "updateTime", "requestId"} {
		if _, ok := m[k]; !ok {
			t.Errorf("缺少字段 %s, 实际keys=%v", k, m)
		}
	}
	if _, ok := m["OrderID"]; ok {
		t.Error("不应该再出现大写开头的字段名")
	}
}

func TestOrderJSON_RequestIDOmittedWhenNil(t *testing.T) {
	m := toMap(t, Order{OrderID: 1})
	if _, ok := m["requestId"]; ok {
		t.Error("没传requestId时不应该出现这个字段")
	}
}

func TestTradeJSON_AllOrderIDsAreStrings(t *testing.T) {
	m := toMap(t, Trade{TradeID: 226750310570262528, BuyOrderID: 226750310570262529, SellOrderID: 226750310570262530,
		MakerOrderID: 226750310570262529})
	for _, k := range []string{"tradeId", "buyOrderId", "sellOrderId", "makerOrderId"} {
		if _, ok := m[k].(string); !ok {
			t.Errorf("%s应该是字符串, got %#v", k, m[k])
		}
	}
}

func TestPositionJSON_HidesInternalVersion(t *testing.T) {
	m := toMap(t, Position{ID: 7, UID: 1, Version: 9})
	if _, ok := m["version"]; ok {
		t.Error("乐观锁version是内部字段，不应该对外暴露")
	}
	if _, ok := m["positionMargin"]; !ok {
		t.Error("缺少positionMargin")
	}
}

// 公开成交视图不能泄露买卖双方的uid和委托id
func TestPublicTrade_HidesParticipantsAndDerivesTakerSide(t *testing.T) {
	// maker是买单 -> 吃单方是卖出
	tr := Trade{TradeID: 1, Symbol: "BTCUSDT", Price: decimal.NewFromInt(1), Volume: decimal.NewFromInt(1),
		BuyOrderID: 10, SellOrderID: 11, BuyUID: 100, SellUID: 200, MakerOrderID: 10}
	pub := tr.Public()
	if pub.TakerSide != "sell" {
		t.Errorf("maker=买单时taker应该是sell, got %s", pub.TakerSide)
	}
	tr.MakerOrderID = 11
	if got := tr.Public().TakerSide; got != "buy" {
		t.Errorf("maker=卖单时taker应该是buy, got %s", got)
	}
	m := toMap(t, pub)
	for _, k := range []string{"buyUid", "sellUid", "buyOrderId", "sellOrderId", "makerOrderId"} {
		if _, ok := m[k]; ok {
			t.Errorf("公开成交不应该包含%s", k)
		}
	}
}
