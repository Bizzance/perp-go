// 简单的分布式友好ID生成器：毫秒时间戳*1000+进程内自增序号，不是完整的雪花算法(没有严格
// 保证不冲突的机器ID位)。这不只是"以后要多实例部署才需要考虑"的假设性问题——contract-api
// 和contract-engine现在就是两个独立进程，都会调用这个函数往同一张orders表插入委托记录
// (contract-api的普通下单、contract-engine强平时挂的委托单)，各自的idSeq/idLast是各自
// 进程内的包级变量，互相看不到对方的状态。同一毫秒内两个进程都生成了同一个seq，就会撞出
// 同一个ID，导致其中一条INSERT因为主键冲突失败。
//
// nodeSalt是进程启动时生成的一个随机偏移，加进ID里降低这种跨进程碰撞的概率(不是消除，
// 只是把"同一毫秒里两个进程序号从0开始各自递增，几乎必然趟到相同值"这种确定性碰撞，
// 变成"两个进程凑巧抽到同一个随机偏移"这种低概率碰撞)。真正的根治需要引入机器ID位
// (完整雪花算法)或者改成从一个中心化服务申请ID，MVP阶段先用这个轻量方案顶着。
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
