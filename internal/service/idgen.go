package service

import (
	"log"
	"sync"
	"time"
)

// 标准雪花算法布局：41位毫秒时间戳(相对epochMillis) + 10位node id + 12位序列号，
// 一共63位，塞进uint64/int64都不会溢出符号位。node id区分不同进程/实例，序列号是
// 同一节点同一毫秒内的自增计数——旧实现(时间戳*100000+进程启动时随机抽的salt*1000+
// 序号取模1000)有两个问题：①随机salt不保证跨进程唯一，只是把"必然碰撞"降低成"运气不好
// 才碰撞"；②序号对1000取模会截断，单进程单毫秒内下单超过1000笔时序号绕回重复。这里换成
// 显式配置、要求全局唯一的node id，加上序列号用满就忙等下一毫秒而不是截断，从根上消除
// 碰撞可能，不是降低概率
const (
	nodeBits     = 10
	sequenceBits = 12
	maxNodeID    = (1 << nodeBits) - 1     // 1023
	maxSequence  = (1 << sequenceBits) - 1 // 4095
	nodeShift    = sequenceBits
	timeShift    = sequenceBits + nodeBits
	epochMillis  = 1735689600000 // 2025-01-01T00:00:00Z，自定义纪元，41位时间戳能用到2094年前后

	// 时钟回拨超过这个阈值就判定为系统时钟异常(不是正常NTP校时的量级)，直接快速失败，
	// 不能无限期忙等——忙等会占着下面的全局锁，让这个进程的全部下单/撮合请求卡死，
	// 而且没有任何日志线索，比直接崩溃更难排查
	maxClockBackwardMs = 5000
)

var (
	idMu      sync.Mutex
	idNode    uint64
	idLast    int64
	idSeq     uint64
	idNodeSet bool
)

// InitNodeID 进程启动时必须显式调用一次——不同进程/实例必须配不同的node id
// (PERP_NODE_ID环境变量，见config.Load)，配重了NextID在理论上可能生成重复ID
func InitNodeID(nodeID uint64) {
	if nodeID > maxNodeID {
		log.Fatalf("node id超出范围(必须在0-%d之间): %d", maxNodeID, nodeID)
	}
	idMu.Lock()
	idNode = nodeID
	idNodeSet = true
	idMu.Unlock()
}

func NextID() uint64 {
	idMu.Lock()
	defer idMu.Unlock()
	if !idNodeSet {
		log.Fatalf("NextID: node id还没初始化，必须在进程启动时调用service.InitNodeID")
	}

	now := time.Now().UnixMilli()
	if now < epochMillis {
		// 系统时钟早于自定义纪元——不止是"时钟回拨"，是系统时钟本身就没校准对(比如容器/
		// 虚拟机时钟没做过NTP同步、或者被设成了纪元之前的日期)。不能往下走：
		// uint64(now-epochMillis)这里会因为负数转uint64发生环绕，生成的ID会静默错乱
		// (可能覆盖高位、跟其它时间段/node的ID撞上)，而不是显式报错——必须在这里就
		// 快速失败，让部署问题在启动/首次调用时就暴露出来
		log.Fatalf("系统时钟异常：当前时间(%d)早于自定义纪元(%d)，请检查系统时钟是否已同步", now, epochMillis)
	}
	if now < idLast {
		backward := idLast - now
		if backward > maxClockBackwardMs {
			log.Fatalf("系统时钟回拨异常(%dms)，超过阈值(%dms)，怀疑系统时钟被大幅调整，需要人工介入", backward, int64(maxClockBackwardMs))
		}
		// 系统时钟被小幅往回调过(比如正常范围内的NTP校时)：不能沿用旧时间戳继续生成，
		// 可能撞上已经用过的(时间戳,序列号)组合，睡着等时钟追上来——带sleep而不是纯自旋，
		// 不会占着下面的全局锁把一个CPU核心跑到100%
		log.Printf("[WARN] NextID检测到系统时钟回拨%dms，等待时钟追上", backward)
		for now < idLast {
			time.Sleep(time.Millisecond)
			now = time.Now().UnixMilli()
		}
	}
	if now == idLast {
		idSeq = (idSeq + 1) & maxSequence
		if idSeq == 0 {
			// 这一毫秒内4096个序号用完了，等到下一毫秒——不截断/绕回，宁可短暂阻塞
			// 也不能生成重复ID。正常情况下这个等待在1ms内就结束
			for now <= idLast {
				time.Sleep(time.Millisecond)
				now = time.Now().UnixMilli()
			}
		}
	} else {
		idSeq = 0
	}
	idLast = now

	return uint64(now-epochMillis)<<timeShift | idNode<<nodeShift | idSeq
}
