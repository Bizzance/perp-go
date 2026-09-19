#!/usr/bin/env bash
# 集成测试：拉起一次性的MySQL和Redis容器(端口随机、只绑127.0.0.1)，跑带integration标签的测试，
# 结束后不管成功失败都把容器删掉。依赖docker和go。
#
# 用法：make test-integration
#       make test-integration ARGS='-run TestEngineFreeze -v ./internal/service/'   (传给go test的额外参数)
set -euo pipefail

cd "$(dirname "$0")/.."

suffix="$$"
mysql_name="perpgo-it-mysql-$suffix"
redis_name="perpgo-it-redis-$suffix"
mysql_pass="it-$(openssl rand -hex 8)"
redis_pass="it-$(openssl rand -hex 8)"

cleanup() {
  docker rm -f "$mysql_name" "$redis_name" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# --skip-log-bin/关闭fsync类的设置只为测试跑得快，生产配置不要照抄
docker run -d --name "$mysql_name" -e MYSQL_ROOT_PASSWORD="$mysql_pass" -e TZ=UTC \
  -p 127.0.0.1::3306 mysql:8.4 \
  --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci \
  --skip-log-bin --innodb-flush-log-at-trx-commit=0 --sync-binlog=0 >/dev/null
docker run -d --name "$redis_name" -p 127.0.0.1::6379 redis:7 \
  redis-server --requirepass "$redis_pass" --save "" --appendonly no >/dev/null

# 必须走TCP探测：mysql镜像首次启动时会先起一个只开unix socket的临时服务做初始化，
# 那个阶段mysqladmin ping走socket也会成功，但这时候宿主机连不上
echo "等待MySQL就绪..."
for i in $(seq 1 90); do
  if docker exec "$mysql_name" mysql -h127.0.0.1 --protocol=tcp -uroot -p"$mysql_pass" -e 'select 1' >/dev/null 2>&1; then
    break
  fi
  if [ "$i" = 90 ]; then
    echo "MySQL 90秒内没有就绪" >&2
    docker logs "$mysql_name" 2>&1 | tail -20 >&2
    exit 1
  fi
  sleep 1
done
for i in $(seq 1 30); do
  if docker exec "$redis_name" redis-cli -a "$redis_pass" --no-auth-warning ping 2>/dev/null | grep -q PONG; then
    break
  fi
  if [ "$i" = 30 ]; then echo "Redis 30秒内没有就绪" >&2; exit 1; fi
  sleep 1
done

mysql_port="$(docker port "$mysql_name" 3306/tcp | head -1 | sed 's/.*://')"
redis_port="$(docker port "$redis_name" 6379/tcp | head -1 | sed 's/.*://')"

export PERP_TEST_MYSQL_DSN="root:${mysql_pass}@tcp(127.0.0.1:${mysql_port})/?parseTime=true&loc=UTC&multiStatements=true"
export PERP_TEST_REDIS_ADDR="127.0.0.1:${redis_port}"
export PERP_TEST_REDIS_PASS="$redis_pass"

# -p 1：不同包的测试串行，因为Redis是共用的；-count=1：不用缓存，测试依赖外部状态
# shellcheck disable=SC2086
go test -tags=integration -count=1 -p 1 ${ARGS:-./...}
