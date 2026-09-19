package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"time"

	"perp-go/internal/cache"
	"perp-go/internal/model"
)

var ErrLockBusy = errors.New("并发操作过多，请稍后重试")

const (
	lockTTL           = 3 * time.Second // 明显超过临界区正常耗时(几次DB往返)的上限，持锁方崩溃/异常也不会永久卡住
	lockRetryInterval = 20 * time.Millisecond
	lockMaxWait       = 500 * time.Millisecond // 超过这个等待时间说明是真的高并发冲突，直接失败比让请求一直挂着更好
)

// 基于Redis SETNX实现的简单分布式锁，用于序列化"必须原子执行、又分散在多个
// 进程/实例上"的临界区，两处调用方：①contract-api并发下单时的保证金分档校验+冻结保证金
// (见docs/risk-limit-tiers.md"并发下单的原子性"一节)；②contract-engine分片部署下结束
// 本轮的跨分片最终结算(见docs/engine-sharding.md)，两个进程各自持有自己的LockService
// 实例(共用同一个Redis)。不是完整意义上的Redlock(没有考虑多Redis节点的场景)，跟这个
// 项目现有的单Redis实例部署假设一致，够用，不是过度设计
type LockService struct {
	cache *cache.Cache
}

func NewLockService(cache *cache.Cache) *LockService {
	return &LockService{cache: cache}
}

// 同一个uid+symbol+side的开仓校验临界区(读现有仓位/挂单→算分档→冻结保证金)
// 用的锁key——不是锁整个uid，不同symbol/side之间不该互相阻塞，只有真正会读到同一份
// "existingOpenNotional"的并发请求才需要互斥
func OrderLockKey(uid uint64, symbol string, side model.Side) string {
	return fmt.Sprintf("perpgo:lock:order:%d:%s:%s", uid, symbol, side)
}

// 获取key对应的锁，成功后执行fn，无论fn成功失败都会释放锁。拿不到锁会在
// lockMaxWait内按lockRetryInterval短暂重试——为了让"两笔请求几乎同时到达但不是真的高频
// 冲突"这种正常场景不必因为差几毫秒就直接失败；超过lockMaxWait仍拿不到锁返回ErrLockBusy
func (l *LockService) WithLock(ctx context.Context, key string, fn func() error) error {
	token, err := randomLockToken()
	if err != nil {
		return err
	}
	deadline := time.Now().Add(lockMaxWait)
	for {
		ok, err := l.cache.AcquireLock(ctx, key, token, lockTTL)
		if err != nil {
			return err
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			return ErrLockBusy
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(lockRetryInterval):
		}
	}
	defer func() {
		// 用独立的context释放锁——调用方的ctx这时可能已经因为HTTP请求结束被取消，但锁必须
		// 释放，不能因为ctx取消就把锁一直攥到TTL到期，那会让下一个正常请求平白多等
		if err := l.cache.ReleaseLock(context.Background(), key, token); err != nil {
			log.Printf("[WARN] 释放锁失败(key=%s): %v", key, err)
		}
	}()
	return fn()
}

func randomLockToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
