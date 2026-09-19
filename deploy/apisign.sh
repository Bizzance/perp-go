#!/usr/bin/env bash
# 带签名调用接口的示例，也可以当作合作方实现签名时的对照参考。协议见docs/auth-design.md。
#
# 用法：
#   export PERP_API_KEY=partner-a PERP_API_SECRET=你的secret
#   deploy/apisign.sh GET  'http://localhost:7001/account/info?uid=10001'
#   deploy/apisign.sh POST 'http://localhost:7001/order/add' '{"uid":10001,...}'
#
# 依赖bash、curl、openssl。毫秒时间戳用了GNU date的%N，macOS的date不支持，
# 换成 python3 -c 'import time;print(int(time.time()*1000))'。
set -euo pipefail

method="${1:?用法: apisign.sh METHOD URL [BODY]}"
url="${2:?用法: apisign.sh METHOD URL [BODY]}"
body="${3:-}"
key="${PERP_API_KEY:?请设置PERP_API_KEY}"
secret="${PERP_API_SECRET:?请设置PERP_API_SECRET}"

# PERP_TS/PERP_NONCE只给测试用(比如故意重放同一个nonce)，正常调用不要设置
ts="${PERP_TS:-$(($(date +%s%N) / 1000000))}"
nonce="${PERP_NONCE:-$(openssl rand -hex 16)}"

# 从URL里取出路径和查询串(签名用的是请求里实际发送的原样字符串，不排序不重新编码)
rest="${url#*://}"
path_query="/${rest#*/}"
path="${path_query%%\?*}"
query=""
if [[ "$path_query" == *\?* ]]; then
  query="${path_query#*\?}"
fi

# 请求体哈希：对实际发送的原始字节算SHA256，没有请求体就是空串的哈希
body_hash="$(printf '%s' "$body" | openssl dgst -sha256 -hex | sed 's/^.* //')"

# 待签名串 = timestamp \n nonce \n METHOD \n path \n rawQuery \n sha256hex(body)
string_to_sign="$(printf '%s\n%s\n%s\n%s\n%s\n%s' "$ts" "$nonce" "$method" "$path" "$query" "$body_hash")"
signature="$(printf '%s' "$string_to_sign" | openssl dgst -sha256 -hmac "$secret" -hex | sed 's/^.* //')"

args=(-sS -X "$method" "$url"
  -H "X-Api-Key: $key" -H "X-Timestamp: $ts" -H "X-Nonce: $nonce" -H "X-Signature: $signature")
if [[ -n "$body" ]]; then
  args+=(-H 'Content-Type: application/json' --data-binary "$body")
fi
curl "${args[@]}"
echo
