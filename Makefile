.PHONY: proto test test-cover build
default: proto build test

proto:
	cd proto && \
		buf lint . && \
		rm -fr ./gen && \
		buf generate .

test:
	go test -v ./...

build:
	cd cmd/node && \
		go build -v -o ../../bin/node ./

format:
	go fmt ./...

test-cover:
	go test -coverprofile=coverage.out ./... -coverpkg=./...

cover: test-cover
	go tool cover -html=coverage.out