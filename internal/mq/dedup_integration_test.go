//go:build integration

package mq_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"perp-go/internal/mq"
	"perp-go/internal/repo"
	"perp-go/internal/testutil"
)

// 消息级去重：真实MySQL上的processed_messages表 + mq.WithDedup

func TestProcessedMessages_TryMarkSemantics(t *testing.T) {
	r := repo.NewProcessedMessageRepo(testutil.NewDB(t))
	ctx := context.Background()
	mark := func(group, topic string, partition int, offset int64) bool {
		ok, err := r.TryMark(ctx, group, topic, partition, offset)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}

	if !mark("g1", "t", 0, 100) {
		t.Fatal("第一次应该标记成功")
	}
	if mark("g1", "t", 0, 100) {
		t.Fatal("同一个消息坐标第二次应该返回false")
	}
	// 坐标里任何一项不同都是另一条消息
	if !mark("g1", "t", 0, 101) || !mark("g1", "t", 1, 100) || !mark("g1", "other", 0, 100) {
		t.Fatal("偏移、分区、topic不同都应该算新消息")
	}
	// engine分片下同一条消息被不同consumer group各自消费一次，去重状态要按group分开
	if !mark("g2", "t", 0, 100) {
		t.Fatal("不同consumer group对同一条消息应该各自能标记一次")
	}
}

// 并发标记同一个坐标：唯一约束保证只有一个赢
func TestProcessedMessages_ConcurrentTryMarkHasSingleWinner(t *testing.T) {
	r := repo.NewProcessedMessageRepo(testutil.NewDB(t))
	var winners atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ok, err := r.TryMark(context.Background(), "g", "t", 0, 7)
			if err != nil {
				t.Errorf("TryMark: %v", err)
			}
			if ok {
				winners.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("并发标记同一个坐标应该恰好1个成功, got %d", winners.Load())
	}
}

// 清理：早于cutoff的记录被删，删掉之后同一个坐标又能标记；cutoff在过去则什么都不删
func TestProcessedMessages_DeleteOlderThan(t *testing.T) {
	r := repo.NewProcessedMessageRepo(testutil.NewDB(t))
	ctx := context.Background()
	if ok, err := r.TryMark(ctx, "g", "t", 0, 1); err != nil || !ok {
		t.Fatalf("标记: %v %v", ok, err)
	}

	n, err := r.DeleteOlderThan(ctx, time.Now().Add(-time.Hour))
	if err != nil || n != 0 {
		t.Fatalf("cutoff在过去不应该删任何记录: n=%d err=%v", n, err)
	}
	if ok, _ := r.TryMark(ctx, "g", "t", 0, 1); ok {
		t.Fatal("记录还在，同一个坐标不能再标记")
	}

	n, err = r.DeleteOlderThan(ctx, time.Now().Add(time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("cutoff在未来应该删掉这1条: n=%d err=%v", n, err)
	}
	if ok, _ := r.TryMark(ctx, "g", "t", 0, 1); !ok {
		t.Fatal("清理之后同一个坐标应该又能标记")
	}
}

// WithDedup接上真实的去重表：重复投递只执行一次业务处理，偏移不同的消息各执行一次，
// 不同consumer group各自独立
func TestWithDedup_DuplicateDeliveryRunsHandlerOnce(t *testing.T) {
	r := repo.NewProcessedMessageRepo(testutil.NewDB(t))
	ctx := context.Background()
	var calls atomic.Int32
	handler := func(mq.Message) error { calls.Add(1); return nil }
	g1 := mq.WithDedup(ctx, "g1", r, handler)
	g2 := mq.WithDedup(ctx, "g2", r, handler)
	msg := mq.Message{Topic: "t", Partition: 0, Offset: 5}

	for i := 0; i < 3; i++ {
		if err := g1(msg); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("同一条消息投递3次应该只处理1次, got %d", calls.Load())
	}

	if err := g1(mq.Message{Topic: "t", Partition: 0, Offset: 6}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("偏移不同是新消息, got %d", calls.Load())
	}

	if err := g2(msg); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatalf("另一个consumer group对同一条消息要独立处理一次, got %d", calls.Load())
	}
}

// 并发重复投递同一条消息：业务处理只能执行一次
func TestWithDedup_ConcurrentDuplicatesRunHandlerOnce(t *testing.T) {
	r := repo.NewProcessedMessageRepo(testutil.NewDB(t))
	var calls atomic.Int32
	h := mq.WithDedup(context.Background(), "g", r, func(mq.Message) error { calls.Add(1); return nil })
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = h(mq.Message{Topic: "t", Offset: 9})
		}()
	}
	close(start)
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("并发重复投递只应该处理1次, got %d", calls.Load())
	}
}
