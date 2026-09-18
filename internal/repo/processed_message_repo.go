package repo

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

// ProcessedMessageRepo 记录处理过的Kafka消息坐标(consumer_group+topic+partition+offset)，
// 用于防御at-least-once语义下的重复投递被业务handler处理第二遍——完整的消息级去重方案，见
// docs/message-dedup.md。consumer_group在key里是因为engine分片(docs/engine-sharding.md)
// 之后，同一条消息会被多个engine实例各自独立的consumer group各fan-out消费一次，去重必须
// 按"这个consumer group有没有处理过"分别判断，不能用一个全局共享的去重状态
type ProcessedMessageRepo struct{ db *sqlx.DB }

func NewProcessedMessageRepo(db *sqlx.DB) *ProcessedMessageRepo { return &ProcessedMessageRepo{db: db} }

// TryMark 尝试把这条消息标记为"已处理"。返回true表示这是第一次处理(标记成功，调用方应该
// 继续执行业务逻辑)，返回false表示这条消息之前已经处理过(重复投递，调用方应该跳过)。
// 用INSERT IGNORE+受影响行数判断，不是先SELECT再INSERT——避免"先查后插"之间的竞态窗口，
// 唯一约束本身就是并发安全的去重屏障
func (r *ProcessedMessageRepo) TryMark(ctx context.Context, consumerGroup, topic string, partition int, offset int64) (bool, error) {
	result, err := r.db.ExecContext(ctx,
		"INSERT IGNORE INTO processed_messages (consumer_group, topic, `partition`, `offset`) VALUES (?, ?, ?, ?)",
		consumerGroup, topic, partition, offset)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// DeleteOlderThan 清理create_time早于cutoff的去重记录，返回删除的行数——这张表按消息
// 坐标累积，不清理会无限增长。cutoff应该给一个明显超过"重复投递最长可能延迟"的余量
// (比如跟Kafka topic的retention周期对齐)，见cmd/contract-engine/main.go里的定时清理
func (r *ProcessedMessageRepo) DeleteOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	result, err := r.db.ExecContext(ctx, "DELETE FROM processed_messages WHERE create_time < ?", cutoff)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
