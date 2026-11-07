GO        ?= go
BIN       := bin/goflow
PKGS      := ./...
TEST_DB   ?= postgres://goflow:goflow@localhost:5432/postgres?sslmode=disable
COMPOSE   ?= docker compose

.PHONY: help build run fmt fmt-check vet test test-race test-integration cover bench check \
        up down logs ps migrate apikey demo clean

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

build: ## Build the goflow binary into ./bin
	CGO_ENABLED=0 $(GO) build -trimpath -o $(BIN) ./cmd/goflow

run: build ## Run API, scheduler and worker in one process (needs GOFLOW_DATABASE_URL)
	GOFLOW_AUTO_MIGRATE=true GOFLOW_EXAMPLE_HANDLERS=true GOFLOW_LOG_FORMAT=text $(BIN) all

fmt: ## Format sources
	gofmt -w .

fmt-check: ## Fail if sources are not gofmt-formatted
	@test -z "$$(gofmt -l .)" || (gofmt -l . && echo "run make fmt" && exit 1)

vet: ## Run go vet
	$(GO) vet $(PKGS)

test: ## Unit tests (integration tests skip without GOFLOW_TEST_DATABASE_URL)
	$(GO) test $(PKGS)

test-race: ## Unit tests with the race detector
	$(GO) test -race $(PKGS)

test-integration: ## All tests, including PostgreSQL integration tests, with the race detector
	GOFLOW_TEST_DATABASE_URL="$(TEST_DB)" $(GO) test -race -count=1 $(PKGS)

cover: ## Coverage report (integration tests included when the database is reachable)
	GOFLOW_TEST_DATABASE_URL="$(TEST_DB)" $(GO) test -count=1 -coverpkg=./internal/...,./pkg/... -coverprofile=coverage.out $(PKGS)
	$(GO) tool cover -func=coverage.out | tail -1

bench: ## End-to-end throughput benchmark against PostgreSQL
	GOFLOW_TEST_DATABASE_URL="$(TEST_DB)" $(GO) test -run '^$$' -bench . -benchtime 2000x ./test/integration/

check: fmt-check vet test-race ## What CI runs before integration tests

up: ## Start PostgreSQL, API, workers, Prometheus, Grafana and Jaeger
	$(COMPOSE) up -d --build

down: ## Stop the stack (keeps the database volume)
	$(COMPOSE) down

logs: ## Follow API and worker logs
	$(COMPOSE) logs -f api worker

ps: ## Show stack status
	$(COMPOSE) ps

migrate: ## Apply migrations using the compose database
	$(COMPOSE) run --rm migrate

apikey: ## Create an API key for tenant "demo" (printed once)
	$(COMPOSE) run --rm --no-deps api apikey create -tenant demo -name local

demo: ## Run the order-processing demo (needs GOFLOW_API_KEY)
	./scripts/demo.sh

clean: ## Remove build artifacts
	rm -rf bin coverage.out
