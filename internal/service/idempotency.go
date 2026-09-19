package service

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// 把一次请求的关键参数按固定顺序拼起来取SHA256，作为这次请求的"指纹"。同一个requestId再次
// 提交时比对指纹，就能识别"同一个幂等键却带了不同参数"这种误用——比如合作方两次充值用了同一个
// requestId、金额却不一样，只返回第一次的结果会让合作方误以为第二笔也成功了，应该明确报错。
// 用0x1f(单元分隔符)拼接而不是逗号，避免参数值里本身带分隔符导致不同参数拼出同一个串
func RequestFingerprint(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x1f")))
	return hex.EncodeToString(sum[:])
}
