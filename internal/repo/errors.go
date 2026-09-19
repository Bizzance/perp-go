package repo

import (
	"errors"

	"github.com/go-sql-driver/mysql"
)

// 判断err是不是MySQL唯一索引冲突(错误码1062)——合作方幂等键
// (uid+request_id)的并发重复提交靠数据库唯一索引兜底，调用方据此区分"撞了唯一索引"
// 和其它真正的写库失败
func IsDuplicateKey(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1062
}

// 判断err是不是MySQL死锁(错误码1213)。InnoDB检测到死锁会回滚其中一个事务(受害者)，
// 受害者应该整个事务重试
func IsDeadlock(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == 1213
}

var (
	// 合作方扣减余额时可用余额不足
	ErrInsufficientBalance = errors.New("可用余额不足，无法扣减")
	// 同一个requestId已经用于一笔参数不同的请求
	ErrIdempotencyConflict = errors.New("requestId已经用于一笔参数不同的请求")
)
