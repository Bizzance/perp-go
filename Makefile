.PHONY: fmt build test

fmt:
	gofmt -w .

build:
	go build ./...

test:
	go test ./...
