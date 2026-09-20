# 三个进程共用一份多阶段构建：build阶段一次编译出三个二进制(共享依赖下载和编译缓存)，
# 最终镜像只有静态二进制，用api/engine/feeder三个target分别取用：
#   docker build -f deploy/Dockerfile --target api    -t perp-go/api    .
#   docker build -f deploy/Dockerfile --target engine -t perp-go/engine .
#   docker build -f deploy/Dockerfile --target feeder -t perp-go/feeder .
# 构建上下文是仓库根目录(不是deploy/)

FROM golang:1.27-alpine AS build
# Go模块代理，构建机访问官方代理慢或不通时可以换(比如 --build-arg GOPROXY=https://goproxy.cn,direct)
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY}
WORKDIR /src
# 先只复制依赖清单再下载依赖：代码变了但依赖没变时这一层命中缓存，不用每次重新下载
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# CGO_ENABLED=0 出静态二进制，不依赖运行镜像里的libc；-trimpath去掉构建机的路径，
# -s -w去掉调试符号缩小体积
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/contract-api ./cmd/contract-api \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/contract-engine ./cmd/contract-engine \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/index-feeder ./cmd/index-feeder

# 运行镜像：alpine自带busybox wget，供容器健康检查用；ca-certificates给以后可能的TLS连接用；
# tzdata让TZ环境变量生效。用非root用户运行
FROM alpine:3.20 AS runtime
RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -S app && adduser -S -G app app
USER app
WORKDIR /app

FROM runtime AS api
COPY --from=build /out/contract-api /app/contract-api
EXPOSE 7001
ENTRYPOINT ["/app/contract-api"]

FROM runtime AS engine
COPY --from=build /out/contract-engine /app/contract-engine
EXPOSE 7002
ENTRYPOINT ["/app/contract-engine"]

# 指数价喂价进程：从多家交易所取指数价、取中位数、推给contract-api。要访问外网，不监听端口
FROM runtime AS feeder
COPY --from=build /out/index-feeder /app/index-feeder
ENTRYPOINT ["/app/index-feeder"]
