# GoFlow

GoFlow is a workflow orchestration engine written in Go. You register
workflows as versioned DAGs of steps; GoFlow runs them durably on a pool of
workers, retries failed steps with exponential backoff, compensates completed
steps (saga) when a later step fails, and recovers on its own from worker
crashes and restarts. PostgreSQL is the only infrastructure it needs.

It takes ideas from Temporal, Camunda, AWS Step Functions and Netflix
Conductor, with its own implementation. The design notes and ADRs explain
each decision and its trade-offs.

## What it solves

A business process such as *validate order → reserve stock ∥ charge payment →
issue invoice → notify* touches several systems and can fail anywhere. Doing
it inside a request handler loses work on crashes, retries blindly and leaves
half-done side effects. GoFlow gives every such process:

* **Durable state**: every transition is committed to PostgreSQL before it
  is acted upon. A restart loses nothing.
* **Dependency-aware execution**: independent steps run in parallel, and a
  step starts only when its dependencies have completed.
* **Retries that don't block**: backoff delays are stored as due times, so no
  goroutine sits waiting for a retry.
* **Saga compensation**: completed steps are undone in reverse order when the
  workflow fails, and failures during compensation are handled too.
* **Crash recovery**: tasks are held under leases. When a worker stops
  heartbeating, its tasks are retried elsewhere, and any late result it sends
  is rejected.
* **Full traceability**: the API exposes the history of every state
  transition and every attempt. Metrics, structured logs and one distributed
  trace per execution are also available.

## Features

| Area | Implemented |
|---|---|
| Definitions | Create, update draft, new versions, publish; DAG validation (cycles, unknown deps); immutable published versions; executions pinned to their version |
| Execution | Start now or at `scheduled_at`; get, list (filters, keyset pagination), history, tasks, attempts; cancel (optionally with compensation), pause, resume; formal state machines |
| Tasks | Per-step handler, params, timeout, retry policy, compensation; input envelope with dependency outputs; attempt history |
| Retry | Exponential backoff, cap, jitter, max attempts; retryable vs permanent vs timeout vs panic; dead letter queue |
| Saga | Reverse-completion-order compensation, one at a time, persisted and retried; stop on compensation failure |
| Workers | Bounded concurrency (semaphore/backpressure), leases + heartbeats, fencing tokens, panic recovery, per-task timeout, graceful drain, handler catalog, horizontal scaling |
| Recovery | Expired-lease recovery, persistent scheduling, restart-safe everything |
| API | REST v1, OpenAPI 3.1, RFC 9457 errors, validation, pagination, `Idempotency-Key`, API keys, permissions, tenant isolation, rate limiting |
| Observability | slog JSON with correlation/trace ids, Prometheus metrics, OpenTelemetry traces across API and workers, Grafana dashboard, health/readiness |
| Ops | Docker (distroless, non-root), Docker Compose stack, Makefile, GitHub Actions |

## Architecture in one picture

```mermaid
flowchart LR
    C[Client] -->|REST + API key| API[goflow serve<br/>API + scheduler]
    API --> PG[(PostgreSQL<br/>state + queue)]
    W1[goflow worker] -->|SKIP LOCKED claims,<br/>leases, results| PG
    W2[your app + pkg/goflow] --> PG
    W1 & W2 -->|idempotency key| EXT[External systems]
    API & W1 -.-> OBS[Prometheus / Grafana / Jaeger]
```

The codebase is a modular monolith with hexagonal layering. The domain is
pure Go (state machines, DAG progression, saga planning, backoff), the
application layer declares small ports, and PostgreSQL, HTTP and telemetry
are adapters. Read more:

* [Architecture overview and C4 diagrams](docs/architecture/overview.md)
* [Execution model: state machines, DAG, retries, saga, sequences](docs/architecture/execution-model.md)
* [Concurrency and resilience: guarantees, leases, fencing, failure scenarios](docs/architecture/concurrency-and-resilience.md)
* [Data model](docs/architecture/data-model.md) · [Security](docs/architecture/security.md) · [Observability](docs/architecture/observability.md)
* [ADRs](docs/adr/README.md) · [OpenAPI](api/openapi.yaml) · [Test report](docs/TESTING.md) · [Commit history](docs/COMMIT_HISTORY.md)

### Delivery guarantees (precisely)

Tasks are delivered **at least once**. The workflow state applies each
outcome **at most once** (fencing tokens). Handlers receive a stable
`IdempotencyKey` and must use it to make external side effects idempotent.
GoFlow does not promise exactly-once execution. See
[ADR-0003](docs/adr/0003-at-least-once-leases-fencing.md).

## Quick start (Docker Compose)

Requirements: Docker with Compose v2, `make`, `curl`, `python3` (demo script only).

```bash
make up                       # builds the image; starts postgres, migrate, api, worker, prometheus, grafana, jaeger
make apikey                   # prints an API key for tenant "demo" (shown once)
export GOFLOW_API_KEY=gf_...  # the printed key
make demo                     # runs the order example: one success with a retry, one compensated failure
```

| Service | URL |
|---|---|
| API | http://localhost:8080 (`/health`, `/ready`, `/metrics`, `/api/v1/...`) |
| Grafana (dashboard "GoFlow Overview") | http://localhost:3000 |
| Jaeger | http://localhost:16686 |
| Prometheus | http://localhost:9091 |

Scale workers with `docker compose up -d --scale worker=3`. Stop the stack with
`make down`.

## Running locally without Docker

Requirements: Go 1.27+, PostgreSQL 14+ (16 recommended).

```bash
export GOFLOW_DATABASE_URL="postgres://goflow:goflow@localhost:5432/goflow?sslmode=disable"
make build
./bin/goflow migrate
./bin/goflow apikey create -tenant demo -name local     # prints the key once
make run                                                # API + scheduler + worker with example handlers on :8080
```

Production-style processes: `goflow serve` (API + scheduler, any number of
replicas) and `goflow worker` (any number of replicas). Every background loop
is safe to run on many instances at once.

## Using the API

```bash
H="Authorization: Bearer $GOFLOW_API_KEY"

# 1. Create and publish a workflow (handlers must be registered by a running worker)
WF=$(curl -s -H "$H" -X POST localhost:8080/api/v1/workflows -d @examples/order-processing.json | jq -r .id)
curl -s -H "$H" -X POST localhost:8080/api/v1/workflows/$WF/publish

# 2. Start an execution (safe to retry thanks to Idempotency-Key)
EX=$(curl -s -H "$H" -H "Idempotency-Key: order-42" -X POST localhost:8080/api/v1/executions \
  -d "{\"workflow_id\":\"$WF\",\"input\":{\"order_id\":\"42\",\"customer_id\":\"c-1\",\"amount\":99.9,\"items\":[{\"sku\":\"SKU-1\",\"quantity\":1}]}}" | jq -r .id)

# 3. Observe
curl -s -H "$H" localhost:8080/api/v1/executions/$EX            # status, output, error
curl -s -H "$H" localhost:8080/api/v1/executions/$EX/history    # every transition
curl -s -H "$H" localhost:8080/api/v1/executions/$EX/tasks      # steps and compensations

# 4. Control
curl -s -H "$H" -X POST localhost:8080/api/v1/executions/$EX/pause
curl -s -H "$H" -X POST localhost:8080/api/v1/executions/$EX/resume
curl -s -H "$H" -X POST localhost:8080/api/v1/executions/$EX/cancel -d '{"reason":"fraud","compensate":true}'
```

Schedule for later with `"scheduled_at": "2026-12-01T09:00:00Z"`. See every
endpoint and schema in [`api/openapi.yaml`](api/openapi.yaml).

### Workflow definition format

```json
{
  "name": "order-processing",
  "spec": {
    "settings": { "max_parallelism": 4, "default_timeout": "30s",
                  "default_retry": { "max_attempts": 3, "initial_interval": "500ms", "max_interval": "10s", "multiplier": 2, "jitter": 0.2 } },
    "steps": [
      { "key": "validate", "handler": "orders.validate", "retry": { "max_attempts": 1 } },
      { "key": "reserve",  "handler": "inventory.reserve", "depends_on": ["validate"],
        "compensation": { "handler": "inventory.release" } },
      { "key": "charge",   "handler": "payment.charge", "depends_on": ["validate"], "timeout": "10s",
        "compensation": { "handler": "payment.refund" } },
      { "key": "invoice",  "handler": "invoice.issue", "depends_on": ["reserve", "charge"], "params": { "series": "A" } }
    ]
  }
}
```

The example handlers (`GOFLOW_EXAMPLE_HANDLERS=true`) simulate stock and
payments in memory. The workflow input can inject failures, for example
`"simulate": {"invoice": "transient:2" | "permanent" | "panic" | "slow:3s", "charge": "declined"}`.

## Writing your own handlers

Applications run their own workers with the public SDK. GoFlow never runs
code sent through the API.

```go
reg := handler.NewRegistry()
reg.MustRegister("billing.charge", handler.Func(func(ctx context.Context, req handler.Request) (json.RawMessage, error) {
    var in struct{ Amount int `json:"amount"` }
    if err := json.Unmarshal(req.WorkflowInput, &in); err != nil {
        return nil, handler.Permanent(err) // not retried
    }
    id, err := gateway.Charge(ctx, in.Amount, req.IdempotencyKey) // stable across retries
    if err != nil {
        return nil, err // retried with backoff
    }
    return json.Marshal(map[string]string{"charge_id": id})
}))
err := goflow.RunWorker(ctx, reg, goflow.WorkerOptions{DatabaseURL: os.Getenv("GOFLOW_DATABASE_URL"), Concurrency: 20})
```

`req.Dependencies` holds the outputs of the steps this one depends on. For a
compensation, `req.Compensation` is true and `req.CompensatedOutput` holds the
output of the step being undone.

## Configuration

All configuration comes from environment variables. See
[`configs/goflow.env.example`](configs/goflow.env.example).

| Variable | Default | Purpose |
|---|---|---|
| `GOFLOW_DATABASE_URL` | — (required) | PostgreSQL URL |
| `GOFLOW_DATABASE_MAX_CONNS` | 20 | Pool size |
| `GOFLOW_AUTO_MIGRATE` | false | Apply migrations at startup |
| `GOFLOW_HTTP_ADDR` | `:8080` | API listen address |
| `GOFLOW_SHUTDOWN_TIMEOUT` | 30s | HTTP shutdown and worker drain timeout |
| `GOFLOW_RATE_LIMIT_RPS` / `_BURST` | 50 / 100 | Per-tenant token bucket |
| `GOFLOW_AUTH_CACHE_TTL` | 30s | API key cache (max revocation delay) |
| `GOFLOW_IDEMPOTENCY_TTL` | 24h | Stored idempotent responses |
| `GOFLOW_MAX_REQUEST_BYTES` | 1048576 | Request body limit |
| `GOFLOW_WORKER_ID` | host-pid-random | Worker identity |
| `GOFLOW_WORKER_CONCURRENCY` | 10 | Tasks in flight per worker |
| `GOFLOW_WORKER_POLL_INTERVAL` | 500ms | Idle poll interval (jittered) |
| `GOFLOW_WORKER_LEASE_DURATION` | 30s | Task lease |
| `GOFLOW_WORKER_HEARTBEAT_INTERVAL` | 10s | Lease renewal (must be < lease) |
| `GOFLOW_METRICS_ADDR` | `:9090` | Worker `/metrics` and `/health` |
| `GOFLOW_EXAMPLE_HANDLERS` | false | Register the order example handlers in `goflow worker` |
| `GOFLOW_SCHEDULER_INTERVAL` / `_BATCH_SIZE` | 1s / 100 | Activation and recovery loop |
| `GOFLOW_LOG_LEVEL` / `GOFLOW_LOG_FORMAT` | info / json | Logging |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | — | OTLP/HTTP traces endpoint (export disabled when empty) |
| `GOFLOW_TRACE_SAMPLE_RATIO` | 1 | Parent-based sampling ratio |

## Tests

```bash
make check              # gofmt check, go vet, unit tests with -race
make test-integration   # + PostgreSQL integration/concurrency/failure tests (TEST_DB=postgres://...)
make cover              # coverage including integration tests
make bench              # end-to-end throughput benchmark
```

Integration tests create (and drop) their own database from
`GOFLOW_TEST_DATABASE_URL` and are skipped when it is not set. They cover
persistence, complete workflows, recovery after worker crashes and restarts,
lost database connections, competing workers, cancellation races, saga
failures, idempotency and tenant isolation. Results from the last run are in
[docs/TESTING.md](docs/TESTING.md).

## Project structure

```text
cmd/goflow/              CLI: serve | worker | all | migrate | tenant | apikey | probe | version
internal/domain/         workflow (spec, DAG, versions) · execution (Run aggregate, state machines, saga) · retry · identity
internal/application/    definitions · executions · engine · worker · scheduler · access · idempotency (ports)
internal/infrastructure/ postgres (repositories, task queue, migrator)
internal/interfaces/     httpapi (routes, middleware, DTOs, problem+json)
internal/platform/       config · telemetry (slog, Prometheus, OpenTelemetry)
internal/app/            composition root
internal/examples/       order-processing example handlers
pkg/handler, pkg/goflow  public handler contract and worker SDK
migrations/              embedded SQL migrations
api/openapi.yaml         API contract (kept in sync by a test)
deployments/monitoring/  Prometheus config, Grafana provisioning and dashboard
docs/                    architecture, ADRs, test report, commit history
examples/                example workflow definition
scripts/demo.sh          end-to-end demo against a running API
test/integration/        PostgreSQL integration, concurrency, failure tests and benchmark
```

## Known limitations

* **Throughput is bound by PostgreSQL.** About 250 executions/s (≈750
  tasks/s) were measured on a 2-vCPU container. Beyond that, the next steps
  would be partitioning or a broker behind the queue port
  ([ADR-0002](docs/adr/0002-postgresql-as-queue.md)).
* **Polling.** Idle workers poll every `GOFLOW_WORKER_POLL_INTERVAL`, which
  bounds pickup latency (`LISTEN/NOTIFY` is not implemented).
* **Workers are trusted.** They connect directly to PostgreSQL. There is no
  remote worker protocol (HTTP/gRPC) for untrusted or non-Go workers.
* **No dead-letter redrive endpoint.** Dead letters can be inspected but not
  replayed, and an execution stopped by a failed compensation needs manual
  intervention.
* **Rate limiting is per API instance**, and so is the API key cache: a
  revoked key works for up to `GOFLOW_AUTH_CACHE_TTL`.
* **API keys are managed with the CLI only.** There is no admin API and no
  OAuth/OIDC.
* **Payloads are stored in plain JSON.** There is no field-level encryption;
  use database encryption at rest for sensitive data.
* **Sequential compensation.** Compensations run one at a time, even for
  independent branches.
* **Retention.** History, attempts and finished executions are never purged.
* **Exactly-once is not provided** (by design, see above).
* **Validation of this delivery.** Docker images, the Compose stack, Grafana
  and Jaeger could not be run in the environment where GoFlow was built (no
  Docker daemon). Their configuration files were checked syntactically, and
  the same binary and configuration were run directly against PostgreSQL. See
  [docs/TESTING.md](docs/TESTING.md).

## License

MIT, see [LICENSE](LICENSE).
