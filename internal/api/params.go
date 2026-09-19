package api

import (
	"regexp"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"
)

const (
	defaultPageLimit = 100
	maxPageLimit     = 500
)

// 历史类查询的分页大小：省略用默认100，最大500，超出上限直接拒绝而不是悄悄截断
// ——悄悄截断会让客户端以为"这一页没满就是到底了"，实际后面还有数据
func parseLimit(c *gin.Context) (int, string) {
	n, msg := parsePositiveIntQuery(c, "limit", defaultPageLimit)
	if msg != "" {
		return 0, msg
	}
	if n > maxPageLimit {
		return 0, "limit参数不能超过" + strconv.Itoa(maxPageLimit)
	}
	return n, ""
}

// 翻页游标：省略=0=从最新开始；非0时只返回id小于before的行。orderId/tradeId这些
// 雪花ID在JSON里是字符串，游标也按字符串/整数都能解析——客户端直接把上一页最后一条的id
// 原样传回来即可
func parseBefore(c *gin.Context) (uint64, string) {
	v := c.Query("before")
	if v == "" {
		return 0, ""
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil || n == 0 {
		return 0, "before参数不合法"
	}
	return n, ""
}

// requestIDPattern 幂等键只允许字母数字和_-，长度1~64：这个值会进唯一索引、也会出现在
// 日志和响应里，不放任意字符串进来
var requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// 空串=没传(不启用幂等)；传了就必须符合格式
func normalizeRequestID(v string) (string, string) {
	if v == "" {
		return "", ""
	}
	if !requestIDPattern.MatchString(v) {
		return "", "requestId只允许字母数字和_-，长度1到64"
	}
	return v, ""
}

// 资金类接口的requestId必填：空串直接拒绝，传了就必须符合格式
func requireRequestID(v string) (string, string) {
	if v == "" {
		return "", "requestId必填"
	}
	return normalizeRequestID(v)
}

// 可选的decimal指针转成指纹里的一段文本：没传和传了0要区分开(marginAmount没传跟传0语义不同)
func decPtrStr(p *decimal.Decimal) string {
	if p == nil {
		return "nil"
	}
	return p.String()
}

// 只有传了requestId才落库指纹——没传就没有"同一个键"可比，存了也用不上
func hashIfKeyed(requestID, requestHash string) *string {
	if requestID == "" {
		return nil
	}
	return &requestHash
}

// 同一个requestId再次提交时，落库的指纹跟这次请求的指纹不一致就是误用。落库指纹为空(没存过)
// 的历史数据不判冲突
func idempotencyConflict(stored *string, current string) bool {
	return stored != nil && *stored != "" && *stored != current
}
