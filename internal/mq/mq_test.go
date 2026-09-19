package mq

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// 内存实现的DedupChecker，用来在不连真实MySQL的情况下测试WithDedup
// 自己的分支逻辑(标记成功/重复跳过/标记失败时的降级行为)
type fakeDedupChecker struct {
	marked  map[string]bool
	failErr error // 非nil时TryMark总是返回这个error，用来模拟DB抖动
}

func newFakeDedupChecker() *fakeDedupChecker {
	return &fakeDedupChecker{marked: make(map[string]bool)}
}

func (f *fakeDedupChecker) TryMark(_ context.Context, consumerGroup, topic string, partition int, offset int64) (bool, error) {
	if f.failErr != nil {
		return false, f.failErr
	}
	key := fmt.Sprintf("%s|%s|%d|%d", consumerGroup, topic, partition, offset)
	if f.marked[key] {
		return false, nil
	}
	f.marked[key] = true
	return true, nil
}

// 第一次见到的消息应该正常执行handler
func TestWithDedup_FirstTimeProcesses(t *testing.T) {
	checker := newFakeDedupChecker()
	called := false
	handler := WithDedup(context.Background(), "group-a", checker, func(msg Message) error {
		called = true
		return nil
	})
	if err := handler(Message{Topic: "t", Partition: 0, Offset: 1}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatalf("第一次处理这条消息，handler应该被调用")
	}
}

// 同一个消息坐标第二次来(模拟Kafka at-least-once重复投递)，
// handler不应该被再次调用——这是这一层存在的核心目的，见docs/message-dedup.md
func TestWithDedup_DuplicateSkips(t *testing.T) {
	checker := newFakeDedupChecker()
	callCount := 0
	handler := WithDedup(context.Background(), "group-a", checker, func(msg Message) error {
		callCount++
		return nil
	})
	msg := Message{Topic: "t", Partition: 0, Offset: 1}
	if err := handler(msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := handler(msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("重复投递的同一条消息，handler应该只被调用1次，实际%d次", callCount)
	}
}

// 同一个消息坐标，不同consumer group应该各自
// 独立处理一次——这是engine分片fan-out消费的正确性基础(docs/engine-sharding.md)，如果
// 去重状态是跨group共享的，先处理到的那个group会把其它group的处理机会顶掉
func TestWithDedup_DifferentGroupsIndependent(t *testing.T) {
	checker := newFakeDedupChecker()
	var groupACalled, groupBCalled bool
	handlerA := WithDedup(context.Background(), "group-a", checker, func(msg Message) error {
		groupACalled = true
		return nil
	})
	handlerB := WithDedup(context.Background(), "group-b", checker, func(msg Message) error {
		groupBCalled = true
		return nil
	})
	msg := Message{Topic: "t", Partition: 0, Offset: 1}
	if err := handlerA(msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := handlerB(msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !groupACalled || !groupBCalled {
		t.Fatalf("两个不同的consumer group应该各自独立处理一次这条消息, groupA=%v groupB=%v", groupACalled, groupBCalled)
	}
}

// 去重标记本身失败(比如DB抖动)时，应该
// 保守地继续执行handler而不是拒绝处理——去重层是锦上添花的正确性加固，不能变成新的单点
// 故障让消息完全消费不了，见WithDedup的函数注释
func TestWithDedup_MarkFailureFallsBackToProcessing(t *testing.T) {
	checker := newFakeDedupChecker()
	checker.failErr = errors.New("db连接失败")
	called := false
	handler := WithDedup(context.Background(), "group-a", checker, func(msg Message) error {
		called = true
		return nil
	})
	if err := handler(Message{Topic: "t", Partition: 0, Offset: 1}); err != nil {
		t.Fatalf("去重标记失败不应该导致handler返回error: %v", err)
	}
	if !called {
		t.Fatalf("去重标记失败时应该降级为直接执行handler")
	}
}
