package service

import (
	"testing"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
)

// 验证分片归属判断(docs/engine-sharding.md)：没配置分片时
// (ownedSymbols为nil)恒为true，等同于单实例部署负责全部symbol；配置了分片时只有列在
// 里面的symbol才算自己的——这个判断是SubmitOrder/CancelOrder/RecoverOrderBook/强平/
// 条件单触发等一系列"绝不能操作到别的实例订单簿"检查的唯一依据，判断错了后果是数据不一致
// (见docs/engine-sharding.md里列的几个具体故障场景)，必须用测试钉死
func TestEngineService_OwnsSymbol(t *testing.T) {
	unsharded := &EngineService{}
	for _, sym := range []string{"BTCUSDT", "ETHUSDT", "ANYTHING"} {
		if !unsharded.OwnsSymbol(sym) {
			t.Fatalf("没配置分片(ownedSymbols=nil)时，%s应该被判定为自己负责", sym)
		}
	}

	sharded := &EngineService{ownedSymbols: map[string]bool{"BTCUSDT": true}}
	if !sharded.OwnsSymbol("BTCUSDT") {
		t.Fatalf("配置了分片且BTCUSDT在列表里，应该被判定为自己负责")
	}
	if sharded.OwnsSymbol("ETHUSDT") {
		t.Fatalf("配置了分片但ETHUSDT不在列表里，不应该被判定为自己负责")
	}
}

func newTestOrder(symbol string) model.Order {
	return model.Order{Symbol: symbol}
}

func newTestConditionalOrder(symbol string) model.ConditionalOrder {
	return model.ConditionalOrder{Symbol: symbol}
}

func newTestPosition(symbol string, volume string) model.Position {
	return model.Position{Symbol: symbol, Volume: decimal.RequireFromString(volume)}
}

// 验证结束本轮(docs/engine-sharding.md"结束本轮的异步化")
// 涉及到的symbol并集算得对：三个来源(挂单/条件单/持仓)都要覆盖到、跨来源的重复symbol
// 只算一次、volume<=0的"空"持仓不该被当成还有事要处理——这个并集决定了round_close_progress
// 表要给哪些symbol占坑，漏算一个symbol会导致结束本轮永远等不到那个symbol"done"、永远
// 卡在AllDone之前；多算一个不存在的symbol会导致进度表多一行永远等不到分片实例处理的死数据
func TestCollectRoundCloseSymbols(t *testing.T) {
	orders := []model.Order{newTestOrder("BTCUSDT"), newTestOrder("ETHUSDT")}
	conditional := []model.ConditionalOrder{newTestConditionalOrder("ETHUSDT"), newTestConditionalOrder("SOLUSDT")}
	positions := []model.Position{
		newTestPosition("BTCUSDT", "0.5"),  // 跟挂单的symbol重复，只应该算一次
		newTestPosition("DOGEUSDT", "100"), // 只出现在持仓里
		newTestPosition("ADAUSDT", "0"),    // volume=0，已经空仓，不该被计入
	}

	got := collectRoundCloseSymbols(orders, conditional, positions)

	want := map[string]bool{"BTCUSDT": true, "ETHUSDT": true, "SOLUSDT": true, "DOGEUSDT": true}
	if len(got) != len(want) {
		t.Fatalf("期望%d个symbol, 实际%d个: %v", len(want), len(got), got)
	}
	seen := make(map[string]bool, len(got))
	for _, s := range got {
		if seen[s] {
			t.Fatalf("symbol %s重复出现在结果里: %v", s, got)
		}
		seen[s] = true
		if !want[s] {
			t.Fatalf("不该出现的symbol %s: %v", s, got)
		}
	}
	for s := range want {
		if !seen[s] {
			t.Fatalf("缺少symbol %s: %v", s, got)
		}
	}
	if seen["ADAUSDT"] {
		t.Fatalf("volume=0的空仓ADAUSDT不该被计入: %v", got)
	}
}

func TestCollectRoundCloseSymbols_Empty(t *testing.T) {
	got := collectRoundCloseSymbols(nil, nil, nil)
	if len(got) != 0 {
		t.Fatalf("三个来源都是空的，结果应该是空集, 实际: %v", got)
	}
}
