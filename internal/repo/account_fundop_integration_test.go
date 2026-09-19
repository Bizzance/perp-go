//go:build integration

package repo_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/shopspring/decimal"

	"perp-go/internal/model"
	"perp-go/internal/repo"
	"perp-go/internal/testutil"
)

func fundOp(acc *model.Account, kind repo.FundOpKind, amount, requestID, hash string) repo.FundOp {
	txType := model.TxDeposit
	if kind == repo.FundOpGrantCredit {
		txType = model.TxCreditGrant
	}
	return repo.FundOp{AccountID: acc.ID, UID: acc.UID, Kind: kind, Amount: decimal.RequireFromString(amount),
		TxType: txType, RequestID: requestID, RequestHash: hash, Now: 1}
}

func newFundAccount(t *testing.T) (*repo.AccountRepo, *sqlx.DB, *model.Account) {
	t.Helper()
	conn := testutil.NewDB(t)
	accounts := repo.NewAccountRepo(conn)
	acc, _, err := accounts.CreateIfAbsent(context.Background(), testutil.UIDBase())
	if err != nil {
		t.Fatal(err)
	}
	return accounts, conn, acc
}

func ledgerCount(t *testing.T, conn *sqlx.DB, uid uint64) int {
	t.Helper()
	var n int
	if err := conn.Get(&n, `SELECT COUNT(*) FROM member_transactions WHERE uid = ?`, uid); err != nil {
		t.Fatal(err)
	}
	return n
}

func assertBalance(t *testing.T, accounts *repo.AccountRepo, uid uint64, available, credit string) {
	t.Helper()
	a, err := accounts.FindByUID(context.Background(), uid)
	if err != nil || a == nil {
		t.Fatalf("查账户: %v %v", a, err)
	}
	if !a.Available.Equal(decimal.RequireFromString(available)) || !a.Credit.Equal(decimal.RequireFromString(credit)) {
		t.Fatalf("余额不对: available=%s credit=%s, 期望 available=%s credit=%s", a.Available, a.Credit, available, credit)
	}
}

// 同一个requestId、同样的参数重复提交：只入账一次，后面的都是重放；参数不同是误用
func TestApplyFundOp_IdempotentReplayAndConflict(t *testing.T) {
	accounts, conn, acc := newFundAccount(t)
	ctx := context.Background()

	replayed, err := accounts.ApplyFundOp(ctx, fundOp(acc, repo.FundOpDeposit, "100", "dep-1", "h1"))
	if err != nil || replayed {
		t.Fatalf("第一次应该正常入账: replayed=%v err=%v", replayed, err)
	}
	replayed, err = accounts.ApplyFundOp(ctx, fundOp(acc, repo.FundOpDeposit, "100", "dep-1", "h1"))
	if err != nil || !replayed {
		t.Fatalf("同一个requestId同样的参数应该是重放: replayed=%v err=%v", replayed, err)
	}
	assertBalance(t, accounts, acc.UID, "100", "0")
	if n := ledgerCount(t, conn, acc.UID); n != 1 {
		t.Fatalf("重放不应该多记流水, got %d", n)
	}

	_, err = accounts.ApplyFundOp(ctx, fundOp(acc, repo.FundOpDeposit, "999", "dep-1", "h-other"))
	if !errors.Is(err, repo.ErrIdempotencyConflict) {
		t.Fatalf("同一个requestId参数不同应该是ErrIdempotencyConflict, got %v", err)
	}
	assertBalance(t, accounts, acc.UID, "100", "0")

	// 不同uid可以用同一个requestId：幂等键只在uid内唯一
	other, _, err := accounts.CreateIfAbsent(ctx, acc.UID+1)
	if err != nil {
		t.Fatal(err)
	}
	if replayed, err := accounts.ApplyFundOp(ctx, fundOp(other, repo.FundOpDeposit, "5", "dep-1", "h1")); err != nil || replayed {
		t.Fatalf("别的uid用同一个requestId应该正常入账: replayed=%v err=%v", replayed, err)
	}
}

// 余额不足的扣减不占用requestId：补足余额后用同一个requestId重试能成功
func TestApplyFundOp_InsufficientBalanceDoesNotConsumeRequestID(t *testing.T) {
	accounts, conn, acc := newFundAccount(t)
	ctx := context.Background()
	if _, err := accounts.ApplyFundOp(ctx, fundOp(acc, repo.FundOpDeposit, "50", "dep-1", "h")); err != nil {
		t.Fatal(err)
	}

	_, err := accounts.ApplyFundOp(ctx, fundOp(acc, repo.FundOpWithdraw, "80", "wd-1", "hw"))
	if !errors.Is(err, repo.ErrInsufficientBalance) {
		t.Fatalf("余额不足应该返回ErrInsufficientBalance, got %v", err)
	}
	assertBalance(t, accounts, acc.UID, "50", "0")
	if n := ledgerCount(t, conn, acc.UID); n != 1 {
		t.Fatalf("失败的扣减不应该留下流水, got %d", n)
	}

	if _, err := accounts.ApplyFundOp(ctx, fundOp(acc, repo.FundOpDeposit, "100", "dep-2", "h2")); err != nil {
		t.Fatal(err)
	}
	replayed, err := accounts.ApplyFundOp(ctx, fundOp(acc, repo.FundOpWithdraw, "80", "wd-1", "hw"))
	if err != nil || replayed {
		t.Fatalf("补足余额后同一个requestId应该能成功: replayed=%v err=%v", replayed, err)
	}
	assertBalance(t, accounts, acc.UID, "70", "0")
}

// 发信用额度是累加的，只动credit不动available，流水金额是正数
func TestApplyFundOp_GrantCreditOnlyTouchesCredit(t *testing.T) {
	accounts, conn, acc := newFundAccount(t)
	ctx := context.Background()
	for i, id := range []string{"c1", "c2"} {
		if _, err := accounts.ApplyFundOp(ctx, fundOp(acc, repo.FundOpGrantCredit, "300", id, "h"+id)); err != nil {
			t.Fatalf("第%d次发额度: %v", i+1, err)
		}
	}
	assertBalance(t, accounts, acc.UID, "0", "600")
	var sum decimal.Decimal
	if err := conn.Get(&sum, `SELECT COALESCE(SUM(amount), 0) FROM member_transactions WHERE uid = ? AND type = ?`, acc.UID, model.TxCreditGrant); err != nil {
		t.Fatal(err)
	}
	if !sum.Equal(decimal.NewFromInt(600)) {
		t.Fatalf("信用额度流水合计应该是600, got %s", sum)
	}
}

// 回归测试：同一个requestId并发提交、而先到的又因为余额不足回滚时，InnoDB会在唯一索引上判死锁，
// 原来约一半的请求会返回500。现在先锁账户行让它们串行，全部应该是确定的结果——没有任何一个
// 是死锁之类的意外错误，而且钱只扣一次
func TestApplyFundOp_ConcurrentSameRequestID(t *testing.T) {
	accounts, conn, acc := newFundAccount(t)
	ctx := context.Background()

	const workers = 16

	t.Run("余额不足", func(t *testing.T) {
		results := runConcurrent(workers, func() (bool, error) {
			return accounts.ApplyFundOp(ctx, fundOp(acc, repo.FundOpWithdraw, "10", "wd-poor", "hw"))
		})
		for _, r := range results {
			if !errors.Is(r.err, repo.ErrInsufficientBalance) {
				t.Fatalf("余额为0时并发扣减都应该是ErrInsufficientBalance(不能是死锁等意外错误), got %v", r.err)
			}
		}
		assertBalance(t, accounts, acc.UID, "0", "0")
	})

	t.Run("余额足够", func(t *testing.T) {
		if _, err := accounts.ApplyFundOp(ctx, fundOp(acc, repo.FundOpDeposit, "100", "dep-seed", "hd")); err != nil {
			t.Fatal(err)
		}
		results := runConcurrent(workers, func() (bool, error) {
			return accounts.ApplyFundOp(ctx, fundOp(acc, repo.FundOpWithdraw, "10", "wd-ok", "hw2"))
		})
		applied := 0
		for _, r := range results {
			if r.err != nil {
				t.Fatalf("余额足够时并发同一个requestId不应该有错误, got %v", r.err)
			}
			if !r.replayed {
				applied++
			}
		}
		if applied != 1 {
			t.Fatalf("应该恰好有一个请求真正扣了款, got %d", applied)
		}
		assertBalance(t, accounts, acc.UID, "90", "0")
		var n int
		if err := conn.Get(&n, `SELECT COUNT(*) FROM member_transactions WHERE uid = ? AND request_id = 'wd-ok'`, acc.UID); err != nil || n != 1 {
			t.Fatalf("同一个requestId应该只有一行流水, got %d err=%v", n, err)
		}
	})
}

// 并发的不同requestId扣款：总额不会超扣，不会出现负余额
func TestApplyFundOp_ConcurrentWithdrawsNeverOverdraw(t *testing.T) {
	accounts, _, acc := newFundAccount(t)
	ctx := context.Background()
	if _, err := accounts.ApplyFundOp(ctx, fundOp(acc, repo.FundOpDeposit, "100", "dep", "h")); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	ok, poor := 0, 0
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "wd-" + string(rune('a'+i))
			_, err := accounts.ApplyFundOp(ctx, fundOp(acc, repo.FundOpWithdraw, "30", id, "h"+id))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				ok++
			case errors.Is(err, repo.ErrInsufficientBalance):
				poor++
			default:
				t.Errorf("意外错误: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if ok != 3 || poor != 17 {
		t.Fatalf("100块每次扣30，应该恰好成功3次(共90)、17次余额不足, got ok=%d poor=%d", ok, poor)
	}
	assertBalance(t, accounts, acc.UID, "10", "0")
}

type concurrentResult struct {
	replayed bool
	err      error
}

func runConcurrent(n int, fn func() (bool, error)) []concurrentResult {
	results := make([]concurrentResult, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			replayed, err := fn()
			results[i] = concurrentResult{replayed, err}
		}(i)
	}
	close(start)
	wg.Wait()
	return results
}
