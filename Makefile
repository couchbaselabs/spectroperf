export GOPATH := $(shell go env GOPATH)

devsetup:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.61.0

test:
	go test ./...
fasttest:
	go test -short ./...

cover:
	go test -coverprofile=cover.out ./...

lint:
	golangci-lint run -v

check: lint
	go test -short -cover -race ./...

# Declared phony: the monitoring/ directory would otherwise satisfy these targets.
.PHONY: monitoring monitoring-down monitoring-logs monitoring-clean

monitoring:
	docker compose up -d
	@echo "Grafana:    http://localhost:3000 (the dashboard is the home page)"
	@echo "Prometheus: http://localhost:9090"

monitoring-down:
	docker compose down

monitoring-logs:
	docker compose logs -f

# Also discards the scraped metrics and any dashboard edits made in the UI.
monitoring-clean:
	docker compose down -v
