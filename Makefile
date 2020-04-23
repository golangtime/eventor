.PHONY: lint test

all: lint test

lint:
	golangci-lint run

test:
	go test -p 1 -v ./...

