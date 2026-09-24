#!/usr/bin/env bash
# 启动模拟客户端(内部测试用的交易页面)，连到deploy/.env描述的那套系统。
# 从deploy/.env里取第一把带ops权限的API密钥(页面上有充值、冻结这类运营操作)，后端端口取API_PORT/ENGINE_PORT。
#
# 用法：make sim-client                 # 先用 make compose-test-up 把系统拉起来
#       ENV_FILE=/path/to/.env make sim-client
# 页面地址：http://127.0.0.1:8088，详见docs/sim-client.md
set -euo pipefail
cd "$(dirname "$0")/.."

env_file="${ENV_FILE:-deploy/.env}"
if [ ! -f "$env_file" ]; then
  echo "找不到$env_file：先 cp deploy/.env.test.example deploy/.env 并把PERP_API_KEYS里的CHANGE_ME换成随机串，再 make compose-test-up" >&2
  exit 1
fi

# 变量没有设置时grep返回1，在set -e/pipefail下会让脚本静默退出，所以加|| true，取不到就是空串
get() { { grep -E "^$1=" "$env_file" || true; } | tail -1 | cut -d= -f2-; }
api_port="$(get API_PORT)"; engine_port="$(get ENGINE_PORT)"
auth_disabled="$(get PERP_AUTH_DISABLED)"

export SIM_API_URL="http://127.0.0.1:${api_port:-7001}"
export SIM_ENGINE_URL="http://127.0.0.1:${engine_port:-7002}"

if [ "$auth_disabled" != "true" ]; then
  # PERP_API_KEYS格式：id:secret:权限,id2:secret2:权限，权限用|分隔。取第一把带ops的
  key="$(get PERP_API_KEYS | tr ',' '\n' | awk -F: '$3 ~ /(^|\|)ops($|\|)/ {print; exit}')"
  if [ -z "$key" ]; then
    echo "$env_file 的 PERP_API_KEYS 里没有带ops权限的密钥" >&2
    exit 1
  fi
  export SIM_KEY_ID="${key%%:*}"
  rest="${key#*:}"
  export SIM_KEY_SECRET="${rest%%:*}"

  # 再取第一把只有trade权限(不带ops)的密钥：有的话页面的交易类请求用它、运营类(充值、冻结、设指数价)用上面的ops密钥，
  # 跟合作方的用法一致，接口权限范围分错了会直接返回forbidden。没有就所有请求都用上面那一把
  trade_key="$(get PERP_API_KEYS | tr ',' '\n' | awk -F: '$3 ~ /(^|\|)trade($|\|)/ && $3 !~ /(^|\|)ops($|\|)/ {print; exit}')"
  if [ -n "$trade_key" ]; then
    export SIM_TRADE_KEY_ID="${trade_key%%:*}"
    rest="${trade_key#*:}"
    export SIM_TRADE_KEY_SECRET="${rest%%:*}"
  fi
fi

# shellcheck disable=SC2086
exec go run ./tools/sim-client ${ARGS:-}
