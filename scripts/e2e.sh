#!/usr/bin/env bash
# 端到端冒烟测试：用docker compose拉起一整套一次性的系统(contract-api、contract-engine、MySQL、Redis、
# Kafka)，跑带e2e构建标签的测试，结束后不管成败都把环境删掉。发布前跑一次，不要放进日常测试——
# 要构建镜像、起Kafka，冷启动约2到3分钟。依赖docker(compose v2)、go、curl、openssl、python3。
#
# 用法：make test-e2e
#       E2E_KEEP=1 make test-e2e            # 跑完不拆环境，方便排查(自己用docker compose -p <项目名> down -v 清理)
#       make test-e2e ARGS='-run TestE2E_03 -v'   # 传给go test的额外参数
#
# 场景见e2e/e2e_test.go：签名鉴权、下单撮合结算、撤单、冻结、结束本轮、WebSocket推送、引擎重启恢复。
# 没有覆盖强平和资金费率：强平要把标记价格推到极端位置，资金费率周期8小时，这两块靠集成测试。
set -euo pipefail

cd "$(dirname "$0")/.."

project="perpgo-e2e-$$"
workdir="$(mktemp -d)"
envfile="$workdir/.env"
# 文件路径用绝对路径：测试进程的工作目录是e2e/，里面执行重启引擎的命令时相对路径就找不到了
root="$(pwd)"
compose=(docker compose -p "$project" --env-file "$envfile" -f "$root/deploy/docker-compose.yml" -f "$root/deploy/docker-compose.deps.yml")

cleanup() {
  if [ "${E2E_KEEP:-}" = "1" ]; then
    echo "E2E_KEEP=1，环境保留。清理命令："
    echo "  ${compose[*]} down -v"
    return
  fi
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$workdir"
}
trap cleanup EXIT

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}
api_port="$(free_port)"
engine_port="$(free_port)"

key_id="e2e-key"
key_secret="$(openssl rand -hex 24)"
mysql_root="$(openssl rand -hex 8)"
mysql_pass="$(openssl rand -hex 8)"
redis_pass="$(openssl rand -hex 8)"

# 基于测试环境模板生成一次性配置：随机密码、随机端口、一把trade+ops权限的密钥，只绑127.0.0.1
sed -e "s/change-me-root/$mysql_root/g" \
    -e "s/change-me-mysql/$mysql_pass/g" \
    -e "s/change-me-redis/$redis_pass/g" \
    -e "s/^API_PORT=.*/API_PORT=$api_port/" \
    -e "s/^ENGINE_PORT=.*/ENGINE_PORT=$engine_port/" \
    -e "s/^BIND_ADDR=.*/BIND_ADDR=127.0.0.1/" \
    -e "s/^PERP_API_KEYS=.*/PERP_API_KEYS=$key_id:$key_secret:trade|ops/" \
    deploy/.env.test.example > "$envfile"

echo "构建并启动整套系统(项目名 $project，api=127.0.0.1:$api_port engine=127.0.0.1:$engine_port)..."
"${compose[@]}" up -d --build

wait_healthy() {
  local url="$1" name="$2"
  for _ in $(seq 1 120); do
    if curl -fsS "$url/health" >/dev/null 2>&1; then return 0; fi
    sleep 2
  done
  echo "$name 4分钟内没有健康" >&2
  "${compose[@]}" logs --tail=60 >&2 || true
  return 1
}
wait_healthy "http://127.0.0.1:$engine_port" contract-engine
wait_healthy "http://127.0.0.1:$api_port" contract-api

export E2E_API_URL="http://127.0.0.1:$api_port"
export E2E_ENGINE_URL="http://127.0.0.1:$engine_port"
export E2E_KEY_ID="$key_id"
export E2E_KEY_SECRET="$key_secret"
export E2E_RESTART_ENGINE_CMD="${compose[*]} restart engine"

# -p 1/-count=1：测试之间有先后依赖(最后一个会重启引擎)，不缓存
# shellcheck disable=SC2086
if ! go test -tags=e2e -count=1 -p 1 ${ARGS:--v ./e2e/}; then
  echo "端到端测试失败，最近的服务日志：" >&2
  "${compose[@]}" logs --tail=80 api engine >&2 || true
  exit 1
fi
