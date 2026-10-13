.PHONY: test test-cover build
default: build test

test:
	go test ./...

build:
	cd cmd/node && \
		go build -v -o ../../bin/node ./

format:
	go fmt ./...

test-cover:
	go test -coverprofile=coverage.out ./... -coverpkg=./...

cover: test-cover
	go tool cover -html=coverage.out