// 简单的分布式友好ID生成器：毫秒时间戳*1000+进程内自增序号，MVP单engine实例部署下足够唯一，
// 不是完整的雪花算法(不需要机器ID位，跟这个项目"先跑通再说"的取舍一致)。
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
