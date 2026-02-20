APP=am-ops-observer

.PHONY: all tidy build run test clean

all: build

tidy:
	go mod tidy

build:
	go build -o $(APP) ./cmd/api

run:
	go run ./cmd/api

test:
	go test ./...

clean:
	rm -f $(APP)
