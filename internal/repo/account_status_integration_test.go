//go:build integration

package repo_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"perp-go/internal/model"
	"perp-go/internal/repo"
	"perp-go/internal/testutil"
)

type historyRow struct {
	UID        uint64 `db:"uid"`
	FromStatus string `db:"from_status"`
	ToStatus   string `db:"to_status"`
	Reason     string `db:"reason"`
	Operator   string `db:"operator"`
	CreateTime int64  `db:"create_time"`
}

func TestAccountRepo_SetStatus(t *testing.T) {
	conn := testutil.NewDB(t)
	accounts := repo.NewAccountRepo(conn)
	ctx := context.Background()
	uid := testutil.UIDBase()

	if _, _, err := accounts.SetStatus(ctx, uid, model.AccountStatusFrozen, "x", "op", 1); !errors.Is(err, repo.ErrAccountNotFound) {
		t.Fatalf("账户不存在应该返回ErrAccountNotFound, got %v", err)
	}
	if st, err := accounts.FindStatus(ctx, uid); err != nil || st != "" {
		t.Fatalf("账户不存在时FindStatus应该返回空串, got %q, %v", st, err)
	}

	acc, created, err := accounts.CreateIfAbsent(ctx, uid)
	if err != nil || !created {
		t.Fatalf("建账户失败: created=%v err=%v", created, err)
	}
	if acc.Status != model.AccountStatusActive || acc.StatusTime != 0 || acc.StatusReason != "" {
		t.Fatalf("新账户应该是active、没有状态变更记录: %+v", acc)
	}

	from, changed, err := accounts.SetStatus(ctx, uid, model.AccountStatusFrozen, "风控：异常交易", "partner-a", 1000)
	if err != nil || !changed || from != model.AccountStatusActive {
		t.Fatalf("active->frozen应该生效: from=%s changed=%v err=%v", from, changed, err)
	}
	acc, _ = accounts.FindByUID(ctx, uid)
	if acc.Status != model.AccountStatusFrozen || acc.StatusReason != "风控：异常交易" || acc.StatusTime != 1000 {
		t.Fatalf("冻结后账户字段不对: %+v", acc)
	}
	if st, _ := accounts.FindStatus(ctx, uid); st != model.AccountStatusFrozen {
		t.Fatalf("FindStatus应该返回frozen, got %q", st)
	}

	// 已经是目标状态：什么都不改，也不记历史，原因和时间保持第一次冻结时的
	from, changed, err = accounts.SetStatus(ctx, uid, model.AccountStatusFrozen, "另一个原因", "partner-b", 2000)
	if err != nil || changed || from != model.AccountStatusFrozen {
		t.Fatalf("重复冻结应该changed=false: from=%s changed=%v err=%v", from, changed, err)
	}
	acc, _ = accounts.FindByUID(ctx, uid)
	if acc.StatusReason != "风控：异常交易" || acc.StatusTime != 1000 {
		t.Fatalf("重复冻结不应该改原因/时间: %+v", acc)
	}

	if _, changed, err := accounts.SetStatus(ctx, uid, model.AccountStatusActive, "复核通过", "partner-a", 3000); err != nil || !changed {
		t.Fatalf("frozen->active应该生效: changed=%v err=%v", changed, err)
	}

	var rows []historyRow
	if err := conn.SelectContext(ctx, &rows,
		`SELECT uid, from_status, to_status, reason, operator, create_time FROM account_status_history WHERE uid = ? ORDER BY id`, uid); err != nil {
		t.Fatal(err)
	}
	want := []historyRow{
		{uid, "active", "frozen", "风控：异常交易", "partner-a", 1000},
		{uid, "frozen", "active", "复核通过", "partner-a", 3000},
	}
	if len(rows) != len(want) {
		t.Fatalf("历史应该只有真正发生变化的两行, got %+v", rows)
	}
	for i := range want {
		if rows[i] != want[i] {
			t.Fatalf("历史第%d行不对: got %+v want %+v", i, rows[i], want[i])
		}
	}
}

// 同一个账户并发设置成同一个目标状态：FOR UPDATE让它们串行，只有第一个真的改了状态、记一行历史，
// 其余都是changed=false
func TestAccountRepo_SetStatus_ConcurrentSameTarget(t *testing.T) {
	conn := testutil.NewDB(t)
	accounts := repo.NewAccountRepo(conn)
	ctx := context.Background()
	uid := testutil.UIDBase()
	if _, _, err := accounts.CreateIfAbsent(ctx, uid); err != nil {
		t.Fatal(err)
	}

	const workers = 12
	var wg sync.WaitGroup
	var mu sync.Mutex
	changedCount := 0
	errs := []error{}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, changed, err := accounts.SetStatus(ctx, uid, model.AccountStatusFrozen, "并发", "op", 1)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
			}
			if changed {
				changedCount++
			}
		}()
	}
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("并发设置状态出错: %v", errs)
	}
	if changedCount != 1 {
		t.Fatalf("并发设置成同一状态只应该有1个changed=true, got %d", changedCount)
	}
	var n int
	if err := conn.GetContext(ctx, &n, `SELECT COUNT(*) FROM account_status_history WHERE uid = ?`, uid); err != nil || n != 1 {
		t.Fatalf("历史应该只有1行, got %d err=%v", n, err)
	}
}
