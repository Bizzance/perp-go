package api

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"perp-go/internal/config"
)

// 权限范围。trade=交易和查询类接口，ops=运营类接口(加钱扣钱、发信用额度、设投保、喂指数价)
const (
	ScopeTrade = "trade"
	ScopeOps   = "ops"
)

const (
	// 时间戳允许的偏差：前后各30秒。合作方服务器的时钟偏差超过这个值请求会被拒绝，需要开NTP
	authWindow = 30 * time.Second
	// 请求体大小上限。鉴权要先读完整个请求体算哈希，这一步发生在确认调用方身份之前，所以上限要
	// 小：没有密钥的人每个并发请求最多让我们缓冲这么多。本系统的请求体都是几百字节的JSON，
	// 64KB已经绰绰有余
	maxAuthBodyBytes = 64 << 10

	ctxAPIKeyID = "apiKeyID"
)

var nonceRegexp = regexp.MustCompile(`^[A-Za-z0-9_-]{16,64}$`)

// 防重放用的一次性随机串存储
type NonceStore interface {
	ClaimNonce(ctx context.Context, key string, ttl time.Duration) (bool, error)
}

type authKey struct {
	secret []byte
	scopes map[string]bool
}

// API Key + HMAC-SHA256请求签名鉴权，方案和理由见docs/auth-design.md
type Auth struct {
	disabled bool
	keys     map[string]authKey
	// 每个路由要求的权限范围("METHOD /path" -> scope)，只能通过Route注册。没有在这里声明过的路由
	// 一律拒绝(默认拒绝)，所以以后新增接口忘了声明权限范围，结果是调不通而不是谁都能调
	routeScopes map[string]string
	nonces      NonceStore
	now         func() time.Time
}

// 关闭鉴权(disabled)只给本地开发用，会打醒目的警告；开启时必须至少配置一把密钥，
// 由调用方(main)在启动时检查，这里不会悄悄退化成没有鉴权
func NewAuth(disabled bool, keys []config.APIKey, nonces NonceStore) *Auth {
	a := &Auth{disabled: disabled, keys: make(map[string]authKey, len(keys)), routeScopes: map[string]string{},
		nonces: nonces, now: time.Now}
	for _, k := range keys {
		sc := make(map[string]bool, len(k.Scopes))
		for _, s := range k.Scopes {
			sc[s] = true
		}
		a.keys[k.ID] = authKey{secret: []byte(k.Secret), scopes: sc}
	}
	if disabled {
		log.Printf("[WARN] 接口鉴权已关闭(PERP_AUTH_DISABLED=true)，任何人都能调用全部接口，只能用于本地开发，绝对不能用在生产")
	}
	return a
}

// 算请求签名。待签名串=timestamp\nnonce\nMETHOD\npath\nrawQuery\nsha256hex(body)，
// 签名=HMAC-SHA256(secret, 待签名串)的十六进制小写。rawQuery是请求里实际发送的查询串，
// 不做排序或重新编码(不同语言的编码细节不一致，规范化了反而容易签错)；body是原始字节，
// 不要重新序列化JSON再算
func SignRequest(secret, timestamp, nonce, method, path, rawQuery string, body []byte) string {
	bodyHash := sha256.Sum256(body)
	msg := strings.Join([]string{timestamp, nonce, method, path, rawQuery, hex.EncodeToString(bodyHash[:])}, "\n")
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(msg))
	return hex.EncodeToString(mac.Sum(nil))
}

func authFail(c *gin.Context, code int, errCode, msg string) {
	failC(c, code, errCode, msg)
	c.Abort()
}

// 全局鉴权中间件：校验签名，通过后检查路由声明的权限范围(没声明的路由默认拒绝)。
// /health免鉴权(编排探针用)。校验顺序：头齐全 → 时间戳在窗口内 → 密钥存在且签名正确 →
// nonce没用过。nonce放在签名之后才记录：先记的话，没有密钥的人也能拿一堆假请求把nonce存储
// 塞满
func (a *Auth) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if a.disabled || c.Request.URL.Path == "/health" {
			c.Next()
			return
		}
		// 请求体声明的长度超限直接拒绝，连读都不读。这个判断跟密钥无关，不会泄露密钥是否存在
		if c.Request.ContentLength > maxAuthBodyBytes {
			authFail(c, 400, ErrInvalidParam, "请求体太大")
			return
		}
		keyID := c.GetHeader("X-Api-Key")
		tsStr := c.GetHeader("X-Timestamp")
		nonce := c.GetHeader("X-Nonce")
		sig := c.GetHeader("X-Signature")
		if keyID == "" || tsStr == "" || nonce == "" || sig == "" || !nonceRegexp.MatchString(nonce) {
			authFail(c, 401, ErrAuthMissing, "缺少鉴权请求头(X-Api-Key/X-Timestamp/X-Nonce/X-Signature)或格式不对")
			return
		}
		ts, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			authFail(c, 401, ErrAuthMissing, "X-Timestamp必须是毫秒时间戳")
			return
		}
		skew := a.now().Sub(time.UnixMilli(ts))
		if skew > authWindow || skew < -authWindow {
			authFail(c, 401, ErrAuthExpired, "时间戳不在允许的时间窗内(前后30秒)，请检查服务器时钟是否同步")
			return
		}

		body, err := readBody(c)
		if err != nil {
			authFail(c, 400, ErrInvalidParam, "请求体太大或读取失败")
			return
		}

		key, known := a.keys[keyID]
		secret := key.secret
		if !known {
			// 密钥不存在也要走一遍完整的HMAC计算再统一返回签名错误：不区分"key不存在"和"签名错"，
			// 耗时也尽量一致，避免被拿来枚举有效的key
			secret = []byte("unknown-key-dummy-secret")
		}
		expected := SignRequest(string(secret), tsStr, nonce, c.Request.Method, c.Request.URL.EscapedPath(), c.Request.URL.RawQuery, body)
		if !hmac.Equal([]byte(expected), []byte(strings.ToLower(sig))) || !known {
			authFail(c, 401, ErrAuthInvalidSignature, "签名不正确")
			return
		}

		// 只在签名通过之后才记录nonce，TTL覆盖时间戳的整个有效期(前后各一个窗口)
		fresh, err := a.nonces.ClaimNonce(c.Request.Context(), "perpgo:auth:nonce:"+keyID+":"+nonce, 2*authWindow+5*time.Second)
		if err != nil {
			// 存储不可用时拒绝请求(失败关闭)，不能因为防重放存储挂了就放行
			log.Printf("[ERROR] 鉴权记录nonce失败: %v", err)
			authFail(c, 500, ErrInternal, "鉴权服务暂时不可用")
			return
		}
		if !fresh {
			authFail(c, 401, ErrAuthReplayed, "nonce已经使用过，每个请求必须使用新的nonce")
			return
		}
		// 权限检查：路由必须声明过权限范围(默认拒绝)，并且这把密钥带这个范围
		scope, declared := a.routeScopes[c.Request.Method+" "+c.FullPath()]
		if !declared {
			log.Printf("[ERROR] 路由 %s %s 没有声明权限范围，默认拒绝(新增接口要用Auth.Route注册)", c.Request.Method, c.FullPath())
			authFail(c, 403, ErrForbidden, "该接口没有声明权限范围，默认拒绝")
			return
		}
		if !key.scopes[scope] {
			log.Printf("[WARN] 密钥%s没有%s权限，拒绝 %s %s", keyID, scope, c.Request.Method, c.FullPath())
			authFail(c, 403, ErrForbidden, "这把密钥没有调用该接口的权限")
			return
		}
		c.Set(ctxAPIKeyID, keyID)
		c.Next()
	}
}

// 注册一个路由并声明它要求的权限范围(ScopeTrade/ScopeOps)。所有需要鉴权的路由都必须通过这个方法
// 注册，直接用gin的r.GET/r.POST注册的路由(除了/health)会被默认拒绝。鉴权关闭时只是普通注册
func (a *Auth) Route(r gin.IRoutes, method, path, scope string, handler gin.HandlerFunc) {
	a.routeScopes[method+" "+path] = scope
	r.Handle(method, path, handler)
}

// 读完请求体并放回去，后面的ShouldBindJSON还要读
func readBody(c *gin.Context) ([]byte, error) {
	if c.Request.Body == nil || c.Request.Body == http.NoBody {
		return nil, nil
	}
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, maxAuthBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxAuthBodyBytes {
		return nil, io.ErrUnexpectedEOF
	}
	c.Request.Body = io.NopCloser(bytes.NewReader(body))
	return body, nil
}
