package service

import (
	"sync"
	"time"
)

var (
	idMu   sync.Mutex
	idSeq  uint64
	idLast int64
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
	return uint64(now)*1000 + idSeq%1000
}
