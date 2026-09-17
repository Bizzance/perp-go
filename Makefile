BIN_DIR := bin

.PHONY: fmt vet race-check build build-api build-engine test test-race build-race run-api run-engine run-api-race run-engine-race clean

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

build: vet race-check build-api build-engine

build-api:
	go build -o $(BIN_DIR)/contract-api ./cmd/contract-api

build-engine:
	go build -o $(BIN_DIR)/contract-engine ./cmd/contract-engine

test:
	go test ./...

# -race只在运行时实际经过的代码路径上抓数据竞争，跑测试用例覆盖不到撮合引擎/风控扫描/资金
# 费率结算这几个真正有并发的地方(都是go func启动的后台goroutine，没有单测覆盖)，所以另外
# 提供race版本的二进制，本地手动跑contract-engine/contract-api时开着更有意义
test-race:
	go test -race ./...

build-race:
	go build -race -o $(BIN_DIR)/contract-api-race ./cmd/contract-api
	go build -race -o $(BIN_DIR)/contract-engine-race ./cmd/contract-engine

run-api: build-api
	./$(BIN_DIR)/contract-api

run-engine: build-engine
	./$(BIN_DIR)/contract-engine

run-api-race: build-race
	./$(BIN_DIR)/contract-api-race

run-engine-race: build-race
	./$(BIN_DIR)/contract-engine-race

clean:
	rm -rf $(BIN_DIR)
