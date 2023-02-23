export GOPATH := $(shell go env GOPATH)

devsetup:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.61.0

test:
	go test ./
fasttest:
	go test -short ./

cover:
	go test -coverprofile=cover.out ./

lint:
	golangci-lint run -v

check: lint
	go test -short -cover -race ./