//go:build integration

package service_test

import (
	"context"
	"errors"
	"testing"

	"perp-go/internal/service"
)

// 开仓买力：账户有浮亏时，买力=available+credit-浮亏，不够就拒绝，不管available本身够不够。
// 币安的可用余额=钱包余额-初始保证金+未实现盈亏，浮亏直接减少可用余额，见docs/account-and-margin.md。
// 统一的场景：a充值1000，多头0.1@65000(保证金650、开仓手续费3.25)，available=346.75；
// 再靠改标记价制造不同的浮亏(0.1 BTC，标记价每变动1，浮盈亏变动0.1)

func (e *engineEnv) buyingPowerAccount(t *testing.T, deposit string) uint64 {
	t.Helper()
	a := e.newAccount(t, 1, deposit)
	b := e.newAccount(t, 2, "10000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")
	return a
}

// 尝试冻结amount：成功就立刻退回(不影响下一次尝试)，返回是不是成功
func (e *engineEnv) canFreeze(t *testing.T, uid uint64, amount string) bool {
	t.Helper()
	ctx := context.Background()
	res, err := e.accounts.FreezeMargin(ctx, uid, decimalOf(t, amount))
	if err != nil {
		if !errors.Is(err, service.ErrInsufficientMargin) {
			t.Fatalf("冻结%s出现意外错误: %v", amount, err)
		}
		return false
	}
	if err := e.accounts.UnfreezeMargin(ctx, uid, res.FromAvailable, res.FromCredit); err != nil {
		t.Fatalf("退回冻结失败: %v", err)
	}
	return true
}

// 浮亏100：买力=346.75-100=246.75。240能冻结，250不能——尽管available(346.75)是够的
func TestBuyingPower_UnrealizedLossReducesIt(t *testing.T) {
	e := newEngineEnv(t)
	a := e.buyingPowerAccount(t, "1000")
	e.setMark(t, testSymbol, "64000") // 0.1*(64000-65000)=-100

	mustDec(t, e.account(t, a).Available, "346.75", "available本身是够的")
	if !e.canFreeze(t, a, "240") {
		t.Fatal("买力246.75，冻结240应该成功")
	}
	if !e.canFreeze(t, a, "246.75") {
		t.Fatal("买力刚好246.75，冻结246.75应该成功(边界)")
	}
	if e.canFreeze(t, a, "246.76") {
		t.Fatal("买力246.75，冻结246.76应该被拒绝，即使available有346.75")
	}
	if e.canFreeze(t, a, "300") {
		t.Fatal("冻结300应该被拒绝")
	}
}

// 已经满足强平条件的账户(权益21.75 <= 维持保证金22.1，浮亏975)：买力是负的，任何新开仓都被拒，
// 不需要单独判断"是不是在强平"。之前available还有346.75就能冻结340去开新仓
func TestBuyingPower_LiquidationEligibleAccountCannotOpenNewPositions(t *testing.T) {
	e := newEngineEnv(t)
	a := e.buyingPowerAccount(t, "1000")
	e.setMark(t, testSymbol, "55250")

	v, err := e.accounts.View(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	mustDec(t, v.Equity, "21.75", "权益已经低于维持保证金22.1")
	for _, amount := range []string{"1", "200", "340", "346.75"} {
		if e.canFreeze(t, a, amount) {
			t.Fatalf("满足强平条件的账户不能再冻结%s开新仓", amount)
		}
	}
}

// 浮盈的行为不变：买力不会因为预检被放宽或收紧。浮盈100时available(346.75)够的照常从available冻结，
// 不够的靠第3级(available+credit+浮盈=446.75)，再多就拒绝
func TestBuyingPower_UnrealizedProfitBehaviorUnchanged(t *testing.T) {
	e := newEngineEnv(t)
	a := e.buyingPowerAccount(t, "1000")
	e.setMark(t, testSymbol, "66000") // +100

	if !e.canFreeze(t, a, "346.75") {
		t.Fatal("available够，应该从available冻结")
	}
	if !e.canFreeze(t, a, "446.75") {
		t.Fatal("available+浮盈够，第3级应该允许")
	}
	if e.canFreeze(t, a, "446.76") {
		t.Fatal("超过available+浮盈应该拒绝")
	}
}

// 信用额度也是买力：浮亏100，available 346.75 + credit 200 - 100 = 446.75
func TestBuyingPower_CreditCountsAndLossStillDeducted(t *testing.T) {
	e := newEngineEnv(t)
	a := e.buyingPowerAccount(t, "1000")
	e.setCredit(t, a, "200", false)
	e.setMark(t, testSymbol, "64000") // -100

	if !e.canFreeze(t, a, "446.75") {
		t.Fatal("买力446.75，冻结446.75(含从信用额度冻结的部分)应该成功")
	}
	if e.canFreeze(t, a, "446.76") {
		t.Fatal("超过买力应该拒绝，浮亏要从available+credit里扣")
	}
}

// 没有仓位(没有浮盈亏)时行为不变：有多少余额就能冻结多少
func TestBuyingPower_NoPositionsBehaviorUnchanged(t *testing.T) {
	e := newEngineEnv(t)
	a := e.newAccount(t, 1, "1000")

	if !e.canFreeze(t, a, "1000") {
		t.Fatal("没有仓位时应该能冻结全部余额")
	}
	if e.canFreeze(t, a, "1000.01") {
		t.Fatal("超过余额应该拒绝")
	}
}

// 多个仓位的浮盈亏合计：一个仓位赚、一个仓位亏，按合计算。BTC多头浮亏100，ETH多头浮盈30，合计-70
func TestBuyingPower_UsesNetUnrealizedAcrossPositions(t *testing.T) {
	e := newEngineEnv(t)
	a := e.newAccount(t, 1, "2000")
	b := e.newAccount(t, 2, "50000")
	e.openLongAgainst(t, a, b, testSymbol, "65000", "0.1", "650")
	e.openLongAgainst(t, a, b, "ETHUSDT", "3000", "1", "300")
	e.setMark(t, testSymbol, "64000") // BTC -100
	e.setMark(t, "ETHUSDT", "3030")   // ETH +30

	// available=2000-650-3.25-300-1.5=1045.25；买力=1045.25-70=975.25
	mustDec(t, e.account(t, a).Available, "1045.25", "available")
	if !e.canFreeze(t, a, "975.25") {
		t.Fatal("买力975.25，冻结975.25应该成功")
	}
	if e.canFreeze(t, a, "975.26") {
		t.Fatal("合计浮亏70，超过975.25应该被拒绝")
	}
}
