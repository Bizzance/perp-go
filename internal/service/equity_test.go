package service

import (
	"testing"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

// 权益是全部属于用户的钱加浮动盈亏：每一块都要算进去，任何一块漏掉都会让权益凭空变少(或变多)
func TestEquity_SumsEveryPool(t *testing.T) {
	acc := &model.Account{
		Available:    d("100"),
		Credit:       d("20"),
		FrozenMargin: d("30"),
		FrozenCredit: d("5"),
	}
	got := Equity(acc, d("650"), d("-40"))
	if !got.Equal(d("765")) {
		t.Fatalf("100+20+30+5+650-40=765, got %s", got)
	}
}

// 开仓只是把钱从available挪进冻结/仓位保证金，权益不变
func TestEquity_UnchangedWhenMarginMovesFromAvailableToPosition(t *testing.T) {
	before := Equity(&model.Account{Available: d("1000")}, decimal.Zero, decimal.Zero)
	after := Equity(&model.Account{Available: d("350")}, d("650"), decimal.Zero)
	if !before.Equal(after) {
		t.Fatalf("保证金从available转进仓位，权益不应该变: before=%s after=%s", before, after)
	}
}

// available可以是负数(全仓下合法)，权益照常相加，不会被截成0
func TestEquity_NegativeAvailableIsCarried(t *testing.T) {
	got := Equity(&model.Account{Available: d("-100")}, d("300"), decimal.Zero)
	if !got.Equal(d("200")) {
		t.Fatalf("-100+300=200, got %s", got)
	}
}

func TestSumPositionMargin_IgnoresClosedPositions(t *testing.T) {
	positions := []model.Position{
		{Volume: d("0.1"), PositionMargin: d("650")},
		{Volume: d("1"), PositionMargin: d("300")},
		{Volume: decimal.Zero, PositionMargin: d("999")}, // 已经平掉的仓位，保证金不该再计
	}
	if got := sumPositionMargin(positions); !got.Equal(d("950")) {
		t.Fatalf("应该只算还有持仓量的仓位: got %s", got)
	}
}
