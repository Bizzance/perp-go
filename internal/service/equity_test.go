package service

import (
	"testing"

	"github.com/shopspring/decimal"

	"perp-go/internal/model"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

// 权益=balance+credit+浮动盈亏：balance/credit是不随冻结变化的总额，已经包含了挂单/仓位
// 锁定的那部分钱，不需要再单独加frozen_margin/frozen_credit/仓位保证金
func TestEquity_SumsBalanceCreditAndUnrealized(t *testing.T) {
	acc := &model.Account{
		Balance:      d("100"),
		Credit:       d("20"),
		FrozenMargin: d("30"),
		FrozenCredit: d("5"),
	}
	got := Equity(acc, d("-40"))
	if !got.Equal(d("80")) {
		t.Fatalf("100+20-40=80(frozen_margin/frozen_credit不重复计入), got %s", got)
	}
}

// 开仓只是把锁定额度从frozen_margin记多一点，balance本身不变，权益不变
func TestEquity_UnchangedWhenMarginGetsLocked(t *testing.T) {
	before := Equity(&model.Account{Balance: d("1000")}, decimal.Zero)
	after := Equity(&model.Account{Balance: d("1000"), FrozenMargin: d("650")}, decimal.Zero)
	if !before.Equal(after) {
		t.Fatalf("保证金被锁进frozen_margin，balance不变，权益不应该变: before=%s after=%s", before, after)
	}
}

// balance可以是负数(全仓下合法)，权益照常相加，不会被截成0
func TestEquity_NegativeBalanceIsCarried(t *testing.T) {
	got := Equity(&model.Account{Balance: d("-100")}, d("300"))
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
