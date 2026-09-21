BIN_DIR := bin

.PHONY: fmt vet race-check build build-api build-engine build-booksync test test-integration test-e2e sim-client test-race build-race run-api run-engine run-api-race run-engine-race clean docker-build compose-test-up compose-test-down compose-prod-up

fmt:
	gofmt -w .
	@command -v goimports >/dev/null 2>&1 && goimports -w . || true

vet:
	go vet ./...

# 只验证代码能用-race标志编译通过，不加-o、不产出/保留可执行文件——当作build的前置检查，
# 比单纯vet更进一步，但比真跑起来轻。这本身不会抓到运行时数据竞争(没有代码在跑就没有
# 竞争可抓)，真要抓运行时的数据竞争得用run-api-race/run-engine-race实际跑起来
race-check:
	go build -race ./...

build: vet race-check build-api build-engine build-booksync

build-api:
	go build -o $(BIN_DIR)/contract-api ./cmd/contract-api

build-engine:
	go build -o $(BIN_DIR)/contract-engine ./cmd/contract-engine

build-booksync:
	go build -o $(BIN_DIR)/orderbook-sync ./cmd/orderbook-sync

test:
	go test ./...

# 集成测试：真实的MySQL+Redis(scripts/test-integration.sh拉一次性容器，结束自动删)，测试文件带
# integration构建标签，所以普通的make test不会跑它们。ARGS可以传给go test，比如
#   make test-integration ARGS='-v -run TestEngineFreeze ./internal/service/'
test-integration:
	./scripts/test-integration.sh

# 端到端冒烟测试：docker compose拉起整套系统(含Kafka)，通过对外接口和WebSocket验证全链路，
# 结束自动拆掉。冷启动要2到3分钟，发布前跑，不放进日常测试。E2E_KEEP=1保留环境排查
test-e2e:
	./scripts/e2e.sh

# 模拟客户端：内部测试用的交易页面(http://127.0.0.1:8088)，扮演合作方的后端对接我们的系统，可以在页面上
# 交易、看盘口和K线、做运营操作，还能接币安行情做系统做市。先用compose-test-up把系统拉起来，详见docs/sim-client.md
sim-client:
	./scripts/sim-client.sh

# -race只在运行时实际经过的代码路径上抓数据竞争，跑测试用例覆盖不到撮合引擎/风控扫描/资金
# 费率结算这几个真正有并发的地方(都是go func启动的后台goroutine，没有单测覆盖)，所以另外
# 提供race版本的二进制，本地手动跑contract-engine/contract-api时开着更有意义
test-race:
	go test -race ./...

build-race:
	go build -race -o $(BIN_DIR)/contract-api-race ./cmd/contract-api
	go build -race -o $(BIN_DIR)/contract-engine-race ./cmd/contract-engine

# 本地开发的run目标显式关闭了鉴权(PERP_AUTH_DISABLED=true)，生产用容器部署、必须配置PERP_API_KEYS
run-api: build-api
	PERP_AUTH_DISABLED=true ./$(BIN_DIR)/contract-api

run-engine: build-engine
	PERP_AUTH_DISABLED=true ./$(BIN_DIR)/contract-engine

run-api-race: build-race
	PERP_AUTH_DISABLED=true ./$(BIN_DIR)/contract-api-race

run-engine-race: build-race
	PERP_AUTH_DISABLED=true ./$(BIN_DIR)/contract-engine-race

clean:
	rm -rf $(BIN_DIR)

# ---- 容器化部署，详见docs/deployment.md ----
# IMAGE_TAG默认latest，生产建议传具体版本号或提交哈希：make docker-build IMAGE_TAG=$(git rev-parse --short HEAD)
IMAGE_TAG ?= latest
COMPOSE_TEST = docker compose --env-file deploy/.env -f deploy/docker-compose.yml -f deploy/docker-compose.deps.yml

docker-build:
	docker build -f deploy/Dockerfile --target api    -t perp-go/api:$(IMAGE_TAG) .
	docker build -f deploy/Dockerfile --target engine -t perp-go/engine:$(IMAGE_TAG) .
	docker build -f deploy/Dockerfile --target booksync -t perp-go/booksync:$(IMAGE_TAG) .

# 测试/联调环境：依赖(MySQL/Redis/Kafka)一起拉起来，需要先 cp deploy/.env.test.example deploy/.env
compose-test-up:
	$(COMPOSE_TEST) up -d --build

# 连数据卷一起删掉，下次up会重新初始化数据库
compose-test-down:
	$(COMPOSE_TEST) down -v

# 生产环境：只起两个应用服务，依赖走托管服务，需要先 cp deploy/.env.prod.example deploy/.env 并填好
compose-prod-up:
	docker compose --env-file deploy/.env -f deploy/docker-compose.yml up -d
