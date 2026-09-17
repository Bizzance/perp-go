package service

import (
	"math/rand"
	"sync"
	"time"
)

var (
	idMu     sync.Mutex
	idSeq    uint64
	idLast   int64
	nodeSalt = uint64(rand.New(rand.NewSource(time.Now().UnixNano())).Intn(100))
)

func NextID() uint64 {
	idMu.Lock()
	defer idMu.Unlock()
	now := time.Now().UnixMilli()
	if now != idLast {
		idLast = now
		idSeq = 0
	} else {
		idSeq++
	}
	return uint64(now)*100000 + nodeSalt*1000 + idSeq%1000
}
