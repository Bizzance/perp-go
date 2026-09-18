package repo

import (
	"context"

	"github.com/jmoiron/sqlx"
)

// RoundCloseProgressRepo 结束本轮(CloseRound)在engine分片部署下的跨分片完成度追踪，见
// docs/engine-sharding.md"结束本轮的异步化"一节。一个(uid,round)下每个涉及到的symbol
// 各占一行，负责那个symbol的分片实例做完自己那部分之后把done改成1，全部symbol都done了
// 才能做"清零credit+round前进"这个只能发生一次的最终结算
type RoundCloseProgressRepo struct{ db *sqlx.DB }

func NewRoundCloseProgressRepo(db *sqlx.DB) *RoundCloseProgressRepo {
	return &RoundCloseProgressRepo{db: db}
}

// Seed 给(uid,round)涉及到的symbol占一行坑(done=0)——用INSERT IGNORE，不是SELECT-then-INSERT：
// 多个分片实例收到同一个结束本轮事件(fan-out)时都会调用一次Seed，只有第一个真正插入成功，
// 后面几个都是无害的no-op，不需要调用方自己判断"我是不是第一个来的"
func (r *RoundCloseProgressRepo) Seed(ctx context.Context, uid, round uint64, symbol string, createTime int64) error {
	_, err := r.db.ExecContext(ctx,
		"INSERT IGNORE INTO round_close_progress (uid, round, symbol, done, create_time) VALUES (?, ?, ?, 0, ?)",
		uid, round, symbol, createTime)
	return err
}

// MarkDone 把某个symbol的这部分标记为完成——只有真正拥有这个symbol的分片实例才会调用，
// 见EngineService.closeRoundForSymbol
func (r *RoundCloseProgressRepo) MarkDone(ctx context.Context, uid, round uint64, symbol string) error {
	_, err := r.db.ExecContext(ctx,
		"UPDATE round_close_progress SET done = 1 WHERE uid = ? AND round = ? AND symbol = ?",
		uid, round, symbol)
	return err
}

// AllDone 这个(uid,round)涉及到的全部symbol是不是都完成了——一行都没seed过(uid在这个round
// 没有任何挂单/持仓/条件单)时COUNT(*)天然是0，也算全部完成，调用方可以直接进入最终结算
func (r *RoundCloseProgressRepo) AllDone(ctx context.Context, uid, round uint64) (bool, error) {
	var pending int
	err := r.db.GetContext(ctx, &pending,
		"SELECT COUNT(*) FROM round_close_progress WHERE uid = ? AND round = ? AND done = 0", uid, round)
	if err != nil {
		return false, err
	}
	return pending == 0, nil
}

// Delete 最终结算完成后清理掉这个(uid,round)的全部进度行，不然这张表会无限增长
func (r *RoundCloseProgressRepo) Delete(ctx context.Context, uid, round uint64) error {
	_, err := r.db.ExecContext(ctx, "DELETE FROM round_close_progress WHERE uid = ? AND round = ?", uid, round)
	return err
}
