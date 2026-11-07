# Test report

This report records the results of the last validation run, done while the
project was being built (2026-11-06). Every result below comes from commands
that were actually executed.

## Environment

| Item | Value |
|---|---|
| OS | Linux (container), 2 vCPU |
| Go | `go1.27.3 linux/amd64` |
| PostgreSQL | 16.15 (local instance) |
| Docker | client only, **no daemon**: images and Compose stack not runnable |

## Commands and results

| Command | Result |
|---|---|
| `gofmt -l .` | no files listed (all formatted) |
| `go mod tidy` (diff against committed `go.mod`/`go.sum`) | no changes |
| `go vet ./...` | no findings |
| `go build ./...` | success |
| `go test ./...` (no database) | all packages `ok`. Integration tests skip themselves without `GOFLOW_TEST_DATABASE_URL` |
| `GOFLOW_TEST_DATABASE_URL=... go test -race -count=1 ./...` | **92 tests passed, 0 failed, 0 skipped**, no races detected |
| `go test -race -count=4 ./test/integration/ ./internal/application/worker/` | all passed 4/4 (flakiness check of the concurrency suites) |
| Coverage (`-coverpkg=./internal/...,./pkg/...`, with integration tests) | **84.8 %** of statements |
| `go test -bench WorkflowThroughput -benchtime 3000x` | 250.5 executions/s, 751.6 tasks/s |
| Layering check (`go list` imports) | `internal/domain` imports only the standard library; `internal/application` imports no adapter |
| `openapi-spec-validator api/openapi.yaml` | valid OpenAPI 3.1 |
| `TestRoutesMatchOpenAPI` | router and OpenAPI paths/methods are identical |
| Mermaid diagrams (`mmdc` render of all 11 diagrams) | all render |
| `docker compose config --quiet` | valid |
| YAML/JSON parse of Prometheus, Grafana provisioning, dashboard and CI files | valid |
| `bash -n scripts/demo.sh` and an actual run of it against `goflow all` | the success path (with one retry) completed, and the failure path ended COMPENSATED |
| Dashboard metric names checked against a live `/metrics` scrape | all 16 metrics exist |

## Coverage by package

| Package | Coverage |
|---|---|
| `internal/app` | 41.2% |
| `internal/application` | 83.3% |
| `internal/application/access` | 82.0% |
| `internal/application/definitions` | 81.1% |
| `internal/application/engine` | 89.9% |
| `internal/application/executions` | 75.7% |
| `internal/application/scheduler` | 86.5% |
| `internal/application/worker` | 93.1% |
| `internal/domain/execution` | 91.3% |
| `internal/domain/identity` | 96.2% |
| `internal/domain/retry` | 89.6% |
| `internal/domain/workflow` | 89.9% |
| `internal/examples/orders` | 81.7% |
| `internal/infrastructure/postgres` | 81.9% |
| `internal/interfaces/httpapi` | 88.0% |
| `internal/platform/config` | 71.2% |
| `internal/platform/telemetry` | 61.7% |
| `pkg/goflow` | 80.0% |
| `pkg/handler` | 97.0% |

`internal/app` is the composition root. Its untested lines are the HTTP
server lifecycle (`RunServer`), which is exercised manually through
`goflow all` but not by an automated test.

## What each suite covers

### Unit tests (no I/O)

* **State machines**: every pair of execution states is checked against the
  transition table, along with the task transitions
  (`TestExecutionStateMachine`, `TestTaskStateMachine`).
* **Workflow validation and dependency resolution**: cycles (with the steps
  involved), unknown, self and duplicate dependencies, limits, topological
  order, effective timeouts and retry policies, and the version lifecycle and
  immutability.
* **Retry policies and backoff**: exponential growth, the interval cap,
  jitter bounds, overflow and JSON round-trips.
* **Saga orchestration**: reverse completion order, waiting for running
  siblings, compensation retries, a compensation failing, and cancel with
  compensation.
* **Domain rules**: lease fencing, pause and resume, idempotent `Advance`,
  the parallelism limit, and recording every transition.
* **Idempotency**: response replay, rejection of a key reused with a
  different body, keys scoped per tenant, and key release after a 5xx.
* **Worker** (fake queue): concurrency never exceeds the limit or claims
  beyond free slots, failure classification (permanent, retryable, panic,
  timeout, ignored deadline, invalid JSON), a lost lease cancels the handler
  without reporting, the graceful drain, abort after the shutdown timeout,
  and backoff on claim errors.
* **HTTP**: rate limiter, error mapping, strict decoding, authentication,
  panic → 500, and the OpenAPI contract.

### Integration tests (PostgreSQL)

* **Persistence and API**: definition lifecycle and versions,
  executions pinned to their version, the database trigger on published
  versions, validation errors (422/400/413), pagination and filters,
  migrations applied twice.
* **Complete workflows**: DAG with parallel branches and dependency outputs,
  retry with measured backoff, timeout, panic, the dead letter queue, saga
  success and failure, scheduled execution activated by a fresh scheduler,
  pause and resume, cancelling a running handler, cancel with compensation,
  the shipped example workflow (stock and payment restored after
  compensation), and a worker embedded through the SDK.
* **Concurrency**: 4 workers × 60 executions × 4 steps, with every task run
  exactly once; 16 concurrent claimers with no overlap; 30 cancel-vs-complete
  races with consistent outcomes; 10-way fan-in with no lost updates; 10
  concurrent starts with the same Idempotency-Key producing one execution.
* **Failures**: a crashed worker (lease expiry → recovery → retry, with the
  late result fenced), an engine restart mid-flight (`WORKER_SHUTDOWN` →
  retried), every database connection killed three times during processing
  (all executions complete), redelivery keeping the idempotency key, tenant
  isolation on every route, and housekeeping (expired keys, stale workers).

## Not executed or blocked

| Item | Status | Reason |
|---|---|---|
| `docker build`, `docker compose up`, Grafana/Jaeger/Prometheus containers | **Blocked** | No Docker daemon or registry access in the build environment. Only the configuration files were validated. |
| GitHub Actions workflow | **Not executed** | Requires GitHub. The YAML was validated; the commands it runs (`make fmt-check vet test-integration build`) were run locally. |
| Messaging (RabbitMQ) failure tests | **Not applicable** | No broker in v1 (ADR-0002). |
| Redis tests | **Not applicable** | Redis is not used. |
| Testcontainers | **Not used** | Integration tests take a database URL, which the CI Postgres service and Docker Compose provide. The module could not be fetched here either. |
| Load test with an external tool (k6, vegeta) | **Not executed** | The end-to-end Go benchmark above measures engine throughput instead. |
