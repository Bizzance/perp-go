package api

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"perp-go/internal/service"
)

// 机器可读的错误码(响应里的errCode字段)。code沿用HTTP语义的400/429/500这种粗粒度分类，
// message是给人看的中文描述、措辞可能调整，合作方程序应该按errCode做分支判断，不要去匹配
// message文本。新增错误码只增不改，已经发布的errCode含义不能变，完整清单见docs/api.md
const (
	ErrInvalidParam         = "invalid_param"          // 请求参数缺失/格式/取值不合法
	ErrSymbolNotFound       = "symbol_not_found"       // 合约不存在或已下架
	ErrOrderNotFound        = "order_not_found"        // 委托/条件单不存在(或不属于这个uid)
	ErrOrderNotCancelable   = "order_not_cancelable"   // 委托/条件单已经成交完、已撤销或已触发，不能再撤
	ErrPositionNotFound     = "position_not_found"     // 这个uid+symbol+side没有持仓
	ErrNoMarkPrice          = "no_mark_price"          // 这个合约还没有标记价格(从没成交过)
	ErrPriceOutOfRange      = "price_out_of_range"     // 委托价格偏离参考价超过价格保护带
	ErrPriceTickInvalid     = "price_tick_invalid"     // 价格不是最小变动单位的整数倍
	ErrVolumeOutOfRange     = "volume_out_of_range"    // 数量低于最小量/超过最大量/不是步长整数倍
	ErrTierNotConfigured    = "tier_not_configured"    // 这个合约没配保证金分档，不允许开仓
	ErrLeverageExceedsTier  = "leverage_exceeds_tier"  // 杠杆超出当前名义价值对应档位允许的上限
	ErrInsufficientMargin   = "insufficient_margin"    // 可用余额/信用额度/浮盈买力不够冻结保证金
	ErrInsufficientBalance  = "insufficient_balance"   // 扣减账户余额时可用余额不足
	ErrServerBusy           = "server_busy"            // 同一个uid+symbol+side的并发请求正在处理，稍后重试
	ErrDispatchFailed       = "dispatch_failed"        // 委托已落库但发往撮合引擎失败，带同一个requestId重试即可补发
	ErrAuthMissing          = "auth_missing"           // 缺少鉴权请求头，或者头的格式不对
	ErrAuthExpired          = "auth_expired"           // 时间戳不在允许的时间窗内(检查合作方服务器的时钟是否同步)
	ErrAuthReplayed         = "auth_replayed"          // 这个nonce已经用过，请求被拒绝(每个请求必须用新的nonce)
	ErrAuthInvalidSignature = "auth_invalid_signature" // 签名不对，或者API Key不存在(故意不区分，避免被用来探测有效的key)
	ErrForbidden            = "forbidden"              // 签名通过了，但这把密钥没有调用这个接口的权限范围
	ErrAccountNotFound      = "account_not_found"      // 账户不存在，先调 POST /account/create 创建
	ErrIdempotencyConflict  = "idempotency_conflict"   // 同一个requestId已经用于一笔参数不同的请求
	ErrRoundMismatch        = "round_mismatch"         // 结束本轮指定的round大于账户当前轮数
	ErrInternal             = "internal_error"         // 服务端内部错误
)

// 统一的失败响应：HTTP状态码固定200，错误信息在body里——code是粗粒度分类，errCode是
// 稳定的机器可读错误码，message是中文描述
func failC(c *gin.Context, code int, errCode, msg string) {
	c.JSON(http.StatusOK, gin.H{"code": code, "errCode": errCode, "message": msg})
}

// 没有专门错误码的失败：按code给一个兜底errCode(400=invalid_param，429=server_busy，
// 其它=internal_error)。有明确业务含义、合作方需要程序化区分的失败要用failC指定errCode
func fail(c *gin.Context, code int, msg string) {
	failC(c, code, defaultErrCode(code), msg)
}

func defaultErrCode(code int) string {
	switch code {
	case 400:
		return ErrInvalidParam
	case 429:
		return ErrServerBusy
	default:
		return ErrInternal
	}
}

// 携带这笔请求最终该返回给客户端的code/errCode/message，用在LockService.WithLock的
// 闭包内部——闭包内不能直接调fail()+return，那样只会终止闭包本身、外层handler会继续往下执行
// (插入订单、发Kafka事件)，等于绕过了刚刚在闭包里失败的校验，必须靠error传出闭包边界
type httpError struct {
	code    int
	errCode string
	msg     string
}

func newHTTPError(code int, errCode, msg string) *httpError {
	return &httpError{code: code, errCode: errCode, msg: msg}
}

func (e *httpError) Error() string { return e.msg }

// 把WithLock返回的error翻译成对应的HTTP失败响应：httpError按它自带的
// code/errCode/message处理，余额不足是400(业务上的正常拒绝，不是服务端故障)，ErrLockBusy
// 按429处理，其它一律当成500——下单/条件单/改杠杆几个handler共用
func respondLockErr(c *gin.Context, err error) {
	var he *httpError
	if errors.As(err, &he) {
		failC(c, he.code, he.errCode, he.msg)
		return
	}
	if errors.Is(err, service.ErrInsufficientMargin) {
		failC(c, 400, ErrInsufficientMargin, err.Error())
		return
	}
	if errors.Is(err, service.ErrLockBusy) {
		failC(c, 429, ErrServerBusy, err.Error())
		return
	}
	fail(c, 500, err.Error())
}
