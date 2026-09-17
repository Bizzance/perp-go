package service

import (
	"sync"
	"testing"
)

// 并发压测NextID：同一个node id高并发调用不能产生重复ID(验证序列号溢出忙等而不是
// 截断绕回)，不同node id即使时间戳完全重叠也不能撞上(验证node id位没有被覆盖/污染)
func TestNextID_NoCollisionUnderConcurrency(t *testing.T) {
	InitNodeID(1)
	const goroutines = 50
	const perGoroutine = 20000

	ids := make(chan uint64, goroutines*perGoroutine)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				ids <- NextID()
			}
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[uint64]bool, goroutines*perGoroutine)
	for id := range ids {
		if seen[id] {
			t.Fatalf("重复ID: %d", id)
		}
		seen[id] = true
	}
	if len(seen) != goroutines*perGoroutine {
		t.Fatalf("期望%d个唯一ID，实际%d个", goroutines*perGoroutine, len(seen))
	}
}

func TestNextID_DifferentNodesNeverCollide(t *testing.T) {
	InitNodeID(1)
	idsNode1 := make(map[uint64]bool, 1000)
	for i := 0; i < 1000; i++ {
		idsNode1[NextID()] = true
	}

	InitNodeID(2)
	for i := 0; i < 1000; i++ {
		id := NextID()
		if idsNode1[id] {
			t.Fatalf("node2生成的ID跟node1的历史ID撞了: %d", id)
		}
	}
}
