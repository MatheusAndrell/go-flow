# Commit history

This history was written after the whole system had been implemented and
validated. The finished tree was then split into logical commits, ordered by
dependency. The process was:

1. Add the commit's files, copied from the validated final tree.
2. Run `go mod tidy`, so `go.mod` and `go.sum` hold only the dependencies used
   so far.
3. Check that `gofmt -l .` lists nothing, then run `go build ./...`,
   `go vet ./...` and `go test ./...`. From commit 22 onward, the PostgreSQL
   integration tests run too.
4. Commit.

After commit 26, the tree was byte-identical to the validated final tree
(`diff -r`).

Two files exist in intermediate versions:

* `go.mod` and `go.sum` grow with each commit (the output of `go mod tidy`).
* `docs/adr/0002-postgresql-as-queue.md` is first committed without
  benchmark numbers. Commit 25 adds the throughput measured by the benchmark
  from commit 22.

## How to reproduce

The repository already contains the real commits (`git log`). To rebuild the
same sequence elsewhere, use the patches delivered with the project:

```bash
mkdir goflow && cd goflow && git init -b main
git am /path/to/goflow-patches/*.patch
```

To regenerate the patches from the repository:

```bash
git format-patch --root -o goflow-patches
```

The `git add` and `git commit` commands listed for each commit are the ones
that created it. Running them in order, with each commit's files at their
state in that commit (`git show <sha>:<path>`), reproduces the same trees.

## Summary

| # | Commit | Message |
|---|---|---|
| 01 | `7d07457` | `chore: initialize Go module and repository conventions` |
| 02 | `0bb8ae6` | `docs: record architecture decisions and bounded contexts` |
| 03 | `b040296` | `feat(domain): implement retry policy with exponential backoff and jitter` |
| 04 | `1deea59` | `feat(domain): implement workflow definitions with DAG validation and versioning` |
| 05 | `aeadceb` | `feat(domain): define execution and task state machines` |
| 06 | `c06a2f6` | `feat(domain): implement execution aggregate with DAG progression and saga compensation` |
| 07 | `aceaf6f` | `feat(domain): add identity model with API keys and permissions` |
| 08 | `3b78d03` | `feat(handler): add public handler contract and registry` |
| 09 | `423e725` | `feat(application): implement workflow definition and execution use cases` |
| 10 | `bf98148` | `feat(engine): implement task outcome orchestration and lease recovery` |
| 11 | `fb517ed` | `feat(worker): implement concurrent worker with leases, heartbeats and graceful shutdown` |
| 12 | `07f3dc0` | `feat(scheduling): implement persistent scheduler and housekeeping loop` |
| 13 | `09a499a` | `feat(security): implement API key authentication and idempotency ports` |
| 14 | `336e724` | `feat(persistence): add PostgreSQL schema and embedded migrator` |
| 15 | `5d4e8cd` | `feat(persistence): implement PostgreSQL repositories and SKIP LOCKED task queue` |
| 16 | `b528a6c` | `feat(observability): add structured logging, Prometheus metrics and OpenTelemetry tracing` |
| 17 | `cd9c2fc` | `feat(config): load runtime configuration from environment variables` |
| 18 | `f0f9143` | `feat(api): implement REST API v1 with authentication, rate limiting, idempotency and OpenAPI contract` |
| 19 | `eaccdf1` | `feat(examples): add order-processing example handlers and workflow` |
| 20 | `c1fd372` | `feat(app): add composition root and goflow CLI` |
| 21 | `94ac7de` | `feat(sdk): add public SDK to run embedded workers` |
| 22 | `759bf62` | `test: add PostgreSQL integration, concurrency and failure tests` |
| 23 | `0978d14` | `chore: add Makefile, Docker image and Compose stack with monitoring` |
| 24 | `0bf4561` | `ci: add GitHub Actions pipeline` |
| 25 | `8aac6c0` | `docs: document architecture, execution model, concurrency, data model, security and observability` |
| 26 | `fc65bf3` | `docs: add README and test report` |
| 27 | `(this c` | `docs: add commit history` |

## Commit 01

`chore: initialize Go module and repository conventions` — `7d07457`

**Objective:** Create the Go module and the repository conventions (line endings, editor settings, ignored files, license).

**Implemented:** Go 1.27 module `github.com/MatheusAndrell/go-flow`.

**Technical justification:** Every later commit builds on the module path; LF line endings are enforced so the history is identical on Windows and Linux.

**Depends on:** none

**Files added:**

- `.editorconfig`
- `.gitattributes`
- `.gitignore`
- `LICENSE`
- `go.mod`

**Files modified:**

- none

**Commands:**

```bash
git add .editorconfig \
  .gitattributes \
  .gitignore \
  LICENSE \
  go.mod
git commit -m "chore: initialize Go module and repository conventions"
```

## Commit 02

`docs: record architecture decisions and bounded contexts` — `0bb8ae6`

**Objective:** Fix the architectural decisions before writing code: modular monolith, PostgreSQL as queue, delivery guarantees, aggregate boundaries, saga, versioning, handler model, auth and observability.

**Implemented:** Nine ADRs (context, decision, consequences).

**Technical justification:** The decisions constrain package boundaries and the database design; recording them first keeps the implementation consistent. ADR-0002 receives measured benchmark numbers in a later commit.

**Depends on:** commit 01

**Files added:**

- `docs/adr/0001-modular-monolith-hexagonal.md`
- `docs/adr/0002-postgresql-as-queue.md`
- `docs/adr/0003-at-least-once-leases-fencing.md`
- `docs/adr/0004-run-aggregate-single-write-path.md`
- `docs/adr/0005-orchestrated-saga.md`
- `docs/adr/0006-immutable-versioned-definitions.md`
- `docs/adr/0007-handlers-registered-in-code.md`
- `docs/adr/0008-api-keys-tenant-isolation.md`
- `docs/adr/0009-observability-stack.md`
- `docs/adr/README.md`

**Files modified:**

- none

**Commands:**

```bash
git add docs/adr/0001-modular-monolith-hexagonal.md \
  docs/adr/0002-postgresql-as-queue.md \
  docs/adr/0003-at-least-once-leases-fencing.md \
  docs/adr/0004-run-aggregate-single-write-path.md \
  docs/adr/0005-orchestrated-saga.md \
  docs/adr/0006-immutable-versioned-definitions.md \
  docs/adr/0007-handlers-registered-in-code.md \
  docs/adr/0008-api-keys-tenant-isolation.md \
  docs/adr/0009-observability-stack.md \
  docs/adr/README.md
git commit -m "docs: record architecture decisions and bounded contexts"
```

## Commit 03

`feat(domain): implement retry policy with exponential backoff and jitter` — `b040296`

**Objective:** Model retry behaviour as a pure value object.

**Implemented:** Max attempts, exponential backoff with cap, ± jitter, overflow protection, validation, JSON with duration strings.

**Technical justification:** Backoff is pure arithmetic; isolating it makes it exhaustively testable and reusable by steps and compensations.

**Depends on:** commit 01

**Files added:**

- `internal/domain/retry/policy.go`
- `internal/domain/retry/policy_test.go`

**Files modified:**

- none

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add internal/domain/retry/policy.go \
  internal/domain/retry/policy_test.go
git commit -m "feat(domain): implement retry policy with exponential backoff and jitter"
```

## Commit 04

`feat(domain): implement workflow definitions with DAG validation and versioning` — `1deea59`

**Objective:** Model the Workflow Definition context.

**Implemented:** Spec/Step/Settings, structural validation, cycle detection with Kahn's algorithm, topological order, effective timeout/retry resolution, Workflow aggregate with draft/publish/new version and change tracking.

**Technical justification:** Executions must bind to immutable versions; validation happens once at definition time so execution never meets an invalid graph.

**Depends on:** commit 03

**Files added:**

- `internal/domain/workflow/spec.go`
- `internal/domain/workflow/workflow.go`
- `internal/domain/workflow/workflow_test.go`

**Files modified:**

- none

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add internal/domain/workflow/spec.go \
  internal/domain/workflow/workflow.go \
  internal/domain/workflow/workflow_test.go
git commit -m "feat(domain): implement workflow definitions with DAG validation and versioning"
```

## Commit 05

`feat(domain): define execution and task state machines` — `aeadceb`

**Objective:** Formalize the allowed transitions of executions and tasks.

**Implemented:** Execution states (PENDING, RUNNING, WAITING, PAUSED, COMPLETED, FAILED, CANCELLED, COMPENSATING, COMPENSATED), task states, transition tables, entity types (Execution, Task, Attempt, Event, DeadLetter), error kinds and retryability.

**Technical justification:** A single transition table is the source of truth for both the engine and the API (409 on invalid transitions).

**Depends on:** commit 03

**Files added:**

- `internal/domain/execution/model.go`
- `internal/domain/execution/status.go`
- `internal/domain/execution/status_test.go`

**Files modified:**

- none

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add internal/domain/execution/model.go \
  internal/domain/execution/status.go \
  internal/domain/execution/status_test.go
git commit -m "feat(domain): define execution and task state machines"
```

## Commit 06

`feat(domain): implement execution aggregate with DAG progression and saga compensation` — `c06a2f6`

**Objective:** Implement the Run aggregate that decides what happens after every event.

**Implemented:** Start/Activate, RecordStarted/Success/Failure/Abandoned with lease fencing, retry scheduling and dead-lettering, idempotent Advance (dependency readiness, max parallelism, WAITING), saga with reverse completion order and sequential compensations, cancel (with optional compensation), pause/resume, change tracking for persistence.

**Technical justification:** Keeping the whole decision logic pure makes the hardest part of the engine testable without a database (ADR-0004, ADR-0005).

**Depends on:** commit 04, commit 05

**Files added:**

- `internal/domain/execution/run.go`
- `internal/domain/execution/run_test.go`
- `internal/domain/execution/sort.go`

**Files modified:**

- none

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add internal/domain/execution/run.go \
  internal/domain/execution/run_test.go \
  internal/domain/execution/sort.go
git commit -m "feat(domain): implement execution aggregate with DAG progression and saga compensation"
```

## Commit 07

`feat(domain): add identity model with API keys and permissions` — `aceaf6f`

**Objective:** Model the Identity and Access context.

**Implemented:** Tenants, principals, permissions, API key generation (256-bit), SHA-256 hashing and constant-time verification, revocation.

**Technical justification:** Authorization decisions belong to the domain so every use case can enforce them uniformly (ADR-0008).

**Depends on:** commit 01

**Files added:**

- `internal/domain/identity/identity.go`
- `internal/domain/identity/identity_test.go`

**Files modified:**

- none

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add internal/domain/identity/identity.go \
  internal/domain/identity/identity_test.go
git commit -m "feat(domain): add identity model with API keys and permissions"
```

## Commit 08

`feat(handler): add public handler contract and registry` — `3b78d03`

**Objective:** Define the public contract between GoFlow and step implementations.

**Implemented:** Handler interface, Func adapter, Request (idempotency key, dependency outputs, compensation data), Permanent error marking, concurrency-safe Registry.

**Technical justification:** Handlers are registered code, never user-supplied code (ADR-0007); the contract lives in pkg/ so applications can implement it.

**Depends on:** commit 01

**Files added:**

- `pkg/handler/handler.go`
- `pkg/handler/handler_test.go`

**Files modified:**

- none

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add pkg/handler/handler.go \
  pkg/handler/handler_test.go
git commit -m "feat(handler): add public handler contract and registry"
```

## Commit 09

`feat(application): implement workflow definition and execution use cases` — `423e725`

**Objective:** Coordinate domain objects through consumer-defined ports.

**Implemented:** Create/get/list/update/version/publish workflows (with handler catalog check); start (idempotent, scheduled, version pinning), query, history, tasks, attempts, dead letters, cancel, pause, resume; input validation and pagination.

**Technical justification:** Use cases depend only on small interfaces they declare, so persistence and HTTP stay replaceable adapters.

**Depends on:** commit 04, commit 06, commit 07

**Files added:**

- `internal/application/application.go`
- `internal/application/definitions/service.go`
- `internal/application/executions/service.go`

**Files modified:**

- none

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add internal/application/application.go \
  internal/application/definitions/service.go \
  internal/application/executions/service.go
git commit -m "feat(application): implement workflow definition and execution use cases"
```

## Commit 10

`feat(engine): implement task outcome orchestration and lease recovery` — `bf98148`

**Objective:** Translate worker reports into workflow progress and recover abandoned work.

**Implemented:** TaskStarted/CompleteTask/FailTask through the locked write path, recovery of expired leases, activation of due scheduled executions, business metrics hooks.

**Technical justification:** Workers and the scheduler share one engine so every outcome follows the same transactional rules (ADR-0003).

**Depends on:** commit 06

**Files added:**

- `internal/application/engine/engine.go`

**Files modified:**

- none

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add internal/application/engine/engine.go
git commit -m "feat(engine): implement task outcome orchestration and lease recovery"
```

## Commit 11

`feat(worker): implement concurrent worker with leases, heartbeats and graceful shutdown` — `fb517ed`

**Objective:** Execute handlers concurrently and safely.

**Implemented:** Semaphore-based concurrency with backpressure, batch claims, goroutine per task, per-task timeout, panic recovery, failure classification, heartbeat loop with lease-loss cancellation, graceful drain and abort, report retries, OpenTelemetry spans linked to the execution trace.

**Technical justification:** Worker correctness is the core concurrency problem of the engine; it is tested with fakes and the race detector.

**Depends on:** commit 08, commit 10

**Files added:**

- `go.sum`
- `internal/application/worker/worker.go`
- `internal/application/worker/worker_test.go`

**Files modified:**

- `go.mod`

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add go.mod \
  go.sum \
  internal/application/worker/worker.go \
  internal/application/worker/worker_test.go
git commit -m "feat(worker): implement concurrent worker with leases, heartbeats and graceful shutdown"
```

## Commit 12

`feat(scheduling): implement persistent scheduler and housekeeping loop` — `07f3dc0`

**Objective:** Run time-based work without in-memory timers.

**Implemented:** Periodic activation of scheduled executions and recovery of expired leases (draining full batches), purge of expired idempotency keys, marking silent workers as stopped, sampling of gauges.

**Technical justification:** All due times live in PostgreSQL, so restarts lose nothing and many instances can run the loop concurrently.

**Depends on:** commit 10

**Files added:**

- `internal/application/scheduler/scheduler.go`

**Files modified:**

- none

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add internal/application/scheduler/scheduler.go
git commit -m "feat(scheduling): implement persistent scheduler and housekeeping loop"
```

## Commit 13

`feat(security): implement API key authentication and idempotency ports` — `09a499a`

**Objective:** Provide authentication and request idempotency at the application level.

**Implemented:** Tenant/API key administration, authenticator with short TTL cache, idempotency record states and store port.

**Technical justification:** Authentication and duplicate-request protection are shared by every endpoint; ports keep them independent of HTTP and SQL.

**Depends on:** commit 07

**Files added:**

- `internal/application/access/access.go`
- `internal/application/access/access_test.go`
- `internal/application/idempotency/idempotency.go`

**Files modified:**

- none

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add internal/application/access/access.go \
  internal/application/access/access_test.go \
  internal/application/idempotency/idempotency.go
git commit -m "feat(security): implement API key authentication and idempotency ports"
```

## Commit 14

`feat(persistence): add PostgreSQL schema and embedded migrator` — `336e724`

**Objective:** Create the schema and the infrastructure to apply it.

**Implemented:** Tables for tenants, keys, definitions, versions, executions, tasks, attempts, events, dead letters, workers and idempotency keys; check constraints, partial indexes for claims/recovery/scheduling, immutability trigger; connection pool, transactional retry on serialization failures/deadlocks, error mapping, advisory-locked checksummed migrator.

**Technical justification:** The schema encodes invariants the application also checks (defense in depth) and the indexes that make PostgreSQL work as a queue (ADR-0002).

**Depends on:** commit 09

**Files added:**

- `internal/infrastructure/postgres/db.go`
- `internal/infrastructure/postgres/migrate.go`
- `migrations/0001_init.sql`
- `migrations/embed.go`

**Files modified:**

- `go.mod`
- `go.sum`

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add go.mod \
  go.sum \
  internal/infrastructure/postgres/db.go \
  internal/infrastructure/postgres/migrate.go \
  migrations/0001_init.sql \
  migrations/embed.go
git commit -m "feat(persistence): add PostgreSQL schema and embedded migrator"
```

## Commit 15

`feat(persistence): implement PostgreSQL repositories and SKIP LOCKED task queue` — `5d4e8cd`

**Objective:** Implement every persistence port.

**Implemented:** Workflow aggregate load/save with row locks, UpdateRun (execution then task locks, DB clock, batched persistence), atomic claims with FOR UPDATE SKIP LOCKED + lease + fencing token + attempt row, heartbeat with fencing, expired leases, due executions, keyset pagination, identity and idempotency stores, counts for gauges.

**Technical justification:** A single write path with fixed lock order gives serializable decisions per execution without deadlocks (ADR-0004).

**Depends on:** commit 10, commit 11, commit 12, commit 13, commit 14

**Files added:**

- `internal/infrastructure/postgres/cursor.go`
- `internal/infrastructure/postgres/executions.go`
- `internal/infrastructure/postgres/idempotency.go`
- `internal/infrastructure/postgres/identity.go`
- `internal/infrastructure/postgres/queue.go`
- `internal/infrastructure/postgres/workflows.go`

**Files modified:**

- none

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add internal/infrastructure/postgres/cursor.go \
  internal/infrastructure/postgres/executions.go \
  internal/infrastructure/postgres/idempotency.go \
  internal/infrastructure/postgres/identity.go \
  internal/infrastructure/postgres/queue.go \
  internal/infrastructure/postgres/workflows.go
git commit -m "feat(persistence): implement PostgreSQL repositories and SKIP LOCKED task queue"
```

## Commit 16

`feat(observability): add structured logging, Prometheus metrics and OpenTelemetry tracing` — `b528a6c`

**Objective:** Make executions observable end to end.

**Implemented:** slog handler adding correlation/trace/span ids, Prometheus registry with execution/task/worker/HTTP metrics implementing the recorder ports, DB-sampled gauges, OTLP/HTTP tracing with W3C propagation.

**Technical justification:** Metrics labels are bounded; trace context is propagated from the API to workers (ADR-0009).

**Depends on:** commit 06

**Files added:**

- `internal/platform/telemetry/logging.go`
- `internal/platform/telemetry/metrics.go`
- `internal/platform/telemetry/tracing.go`

**Files modified:**

- `go.mod`
- `go.sum`

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add go.mod \
  go.sum \
  internal/platform/telemetry/logging.go \
  internal/platform/telemetry/metrics.go \
  internal/platform/telemetry/tracing.go
git commit -m "feat(observability): add structured logging, Prometheus metrics and OpenTelemetry tracing"
```

## Commit 17

`feat(config): load runtime configuration from environment variables` — `cd9c2fc`

**Objective:** Centralize configuration.

**Implemented:** Typed environment loading with defaults, validation and database URL redaction.

**Technical justification:** Twelve-factor configuration keeps secrets out of the repository and images.

**Depends on:** commit 01

**Files added:**

- `internal/platform/config/config.go`
- `internal/platform/config/config_test.go`

**Files modified:**

- none

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add internal/platform/config/config.go \
  internal/platform/config/config_test.go
git commit -m "feat(config): load runtime configuration from environment variables"
```

## Commit 18

`feat(api): implement REST API v1 with authentication, rate limiting, idempotency and OpenAPI contract` — `f0f9143`

**Objective:** Expose the engine over HTTP.

**Implemented:** Routing table, workflow/execution/task/dead-letter endpoints, health/readiness/metrics, bearer auth, per-tenant token bucket, Idempotency-Key replay, correlation ids, tracing and RED metrics middleware, panic recovery, strict JSON decoding, RFC 9457 errors, OpenAPI 3.1 document with a test keeping it in sync with the router.

**Technical justification:** The interface layer only translates HTTP to use cases; security concerns are applied uniformly by middleware.

**Depends on:** commit 09, commit 13, commit 16

**Files added:**

- `api/openapi.yaml`
- `internal/interfaces/httpapi/contract_test.go`
- `internal/interfaces/httpapi/dto.go`
- `internal/interfaces/httpapi/errors.go`
- `internal/interfaces/httpapi/handlers.go`
- `internal/interfaces/httpapi/middleware.go`
- `internal/interfaces/httpapi/middleware_test.go`
- `internal/interfaces/httpapi/ratelimit.go`
- `internal/interfaces/httpapi/server.go`

**Files modified:**

- `go.sum`

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add api/openapi.yaml \
  go.sum \
  internal/interfaces/httpapi/contract_test.go \
  internal/interfaces/httpapi/dto.go \
  internal/interfaces/httpapi/errors.go \
  internal/interfaces/httpapi/handlers.go \
  internal/interfaces/httpapi/middleware.go \
  internal/interfaces/httpapi/middleware_test.go \
  internal/interfaces/httpapi/ratelimit.go \
  internal/interfaces/httpapi/server.go
git commit -m "feat(api): implement REST API v1 with authentication, rate limiting, idempotency and OpenAPI contract"
```

## Commit 19

`feat(examples): add order-processing example handlers and workflow` — `eaccdf1`

**Objective:** Provide a realistic, runnable example.

**Implemented:** Validate, reserve/release stock, charge/refund payment, issue invoice, notify; idempotent in-process simulators; failure injection (transient, permanent, panic, slow, declined).

**Technical justification:** Demonstrates parallel branches, retries and saga compensation with idempotent side effects; opt-in so it never runs in production by accident.

**Depends on:** commit 08

**Files added:**

- `examples/order-processing.json`
- `internal/examples/orders/orders.go`
- `internal/examples/orders/orders_test.go`

**Files modified:**

- none

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add examples/order-processing.json \
  internal/examples/orders/orders.go \
  internal/examples/orders/orders_test.go
git commit -m "feat(examples): add order-processing example handlers and workflow"
```

## Commit 20

`feat(app): add composition root and goflow CLI` — `c1fd372`

**Objective:** Wire adapters to use cases and provide the executable.

**Implemented:** App construction (tracing, pool, migrations, metrics, engine), HTTP server with graceful shutdown, scheduler, worker factory, worker metrics server; CLI commands serve, worker, all, migrate, tenant create, apikey create/revoke, probe, version.

**Technical justification:** Wiring in one place keeps every other package free of concrete dependencies.

**Depends on:** commit 11, commit 12, commit 15, commit 17, commit 18, commit 19

**Files added:**

- `cmd/goflow/main.go`
- `internal/app/app.go`

**Files modified:**

- `go.mod`

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add cmd/goflow/main.go \
  go.mod \
  internal/app/app.go
git commit -m "feat(app): add composition root and goflow CLI"
```

## Commit 21

`feat(sdk): add public SDK to run embedded workers` — `94ac7de`

**Objective:** Let applications run workers with their own handlers.

**Implemented:** goflow.RunWorker with WorkerOptions.

**Technical justification:** Integrating applications only depend on pkg/handler and pkg/goflow, never on engine internals.

**Depends on:** commit 20

**Files added:**

- `pkg/goflow/worker.go`

**Files modified:**

- none

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add pkg/goflow/worker.go
git commit -m "feat(sdk): add public SDK to run embedded workers"
```

## Commit 22

`test: add PostgreSQL integration, concurrency and failure tests` — `759bf62`

**Objective:** Verify the system end to end against a real PostgreSQL.

**Implemented:** Isolated database per run, per-test tenants and handler namespaces; lifecycle, versions, validation, tenant isolation, permissions, idempotency, pagination; DAG, retries, timeout, panic, DLQ, saga, scheduling, pause/resume, cancel; competing workers, claim contention, cancel races, fan-in; worker crash, restart, connection loss, redelivery; SDK; housekeeping; throughput benchmark.

**Technical justification:** Concurrency and recovery guarantees can only be trusted when exercised against the real database with the race detector.

**Depends on:** commit 20, commit 21

**Files added:**

- `test/integration/api_test.go`
- `test/integration/bench_test.go`
- `test/integration/concurrency_test.go`
- `test/integration/engine_test.go`
- `test/integration/example_test.go`
- `test/integration/failure_test.go`
- `test/integration/main_test.go`
- `test/integration/sdk_test.go`

**Files modified:**

- none

**Commands:**

```bash
go mod tidy && gofmt -l . && go build ./... && go vet ./... && go test ./...
git add test/integration/api_test.go \
  test/integration/bench_test.go \
  test/integration/concurrency_test.go \
  test/integration/engine_test.go \
  test/integration/example_test.go \
  test/integration/failure_test.go \
  test/integration/main_test.go \
  test/integration/sdk_test.go
git commit -m "test: add PostgreSQL integration, concurrency and failure tests"
```

## Commit 23

`chore: add Makefile, Docker image and Compose stack with monitoring` — `0978d14`

**Objective:** Make the project easy to build, run and observe locally.

**Implemented:** Make targets (build, run, check, test-integration, cover, bench, up/down, apikey, demo), multi-stage distroless non-root image, Compose with PostgreSQL, migrate, API, scalable workers, Prometheus (DNS discovery of workers), Grafana with provisioned dashboard, Jaeger; example environment file; demo script.

**Technical justification:** One command brings up the whole system with observability; no Kubernetes in v1.

**Depends on:** commit 20, commit 22

**Files added:**

- `.dockerignore`
- `Dockerfile`
- `Makefile`
- `configs/goflow.env.example`
- `deployments/monitoring/grafana/dashboards/goflow-overview.json`
- `deployments/monitoring/grafana/provisioning/dashboards/goflow.yml`
- `deployments/monitoring/grafana/provisioning/datasources/prometheus.yml`
- `deployments/monitoring/prometheus.yml`
- `docker-compose.yml`
- `scripts/demo.sh`

**Files modified:**

- none

**Commands:**

```bash
git add .dockerignore \
  Dockerfile \
  Makefile \
  configs/goflow.env.example \
  deployments/monitoring/grafana/dashboards/goflow-overview.json \
  deployments/monitoring/grafana/provisioning/dashboards/goflow.yml \
  deployments/monitoring/grafana/provisioning/datasources/prometheus.yml \
  deployments/monitoring/prometheus.yml \
  docker-compose.yml \
  scripts/demo.sh
git commit -m "chore: add Makefile, Docker image and Compose stack with monitoring"
```

## Commit 24

`ci: add GitHub Actions pipeline` — `0bf4561`

**Objective:** Run the quality gates on every push and pull request.

**Implemented:** gofmt check, go vet, unit + integration tests with race detector against a PostgreSQL service, build; Compose validation, image build and smoke test.

**Technical justification:** Keeps the guarantees verified by the test suite from regressing.

**Depends on:** commit 23

**Files added:**

- `.github/workflows/ci.yml`

**Files modified:**

- none

**Commands:**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add GitHub Actions pipeline"
```

## Commit 25

`docs: document architecture, execution model, concurrency, data model, security and observability` — `8aac6c0`

**Objective:** Explain how the implemented system works.

**Implemented:** C4 diagrams, domain model, state diagrams, sequence diagrams, guarantees, locking, failure scenarios, ER model, security model, metric catalogue; ADR-0002 updated with measured throughput.

**Technical justification:** Documentation written against the finished implementation so it matches actual behaviour.

**Depends on:** commit 02, commit 22, commit 23

**Files added:**

- `docs/architecture/concurrency-and-resilience.md`
- `docs/architecture/data-model.md`
- `docs/architecture/execution-model.md`
- `docs/architecture/observability.md`
- `docs/architecture/overview.md`
- `docs/architecture/security.md`

**Files modified:**

- `docs/adr/0002-postgresql-as-queue.md`

**Commands:**

```bash
git add docs/adr/0002-postgresql-as-queue.md \
  docs/architecture/concurrency-and-resilience.md \
  docs/architecture/data-model.md \
  docs/architecture/execution-model.md \
  docs/architecture/observability.md \
  docs/architecture/overview.md \
  docs/architecture/security.md
git commit -m "docs: document architecture, execution model, concurrency, data model, security and observability"
```

## Commit 26

`docs: add README and test report` — `fc65bf3`

**Objective:** Provide the entry point to the project and the validation evidence.

**Implemented:** Overview, features, quick start, local run, API usage, handler SDK, configuration, tests, structure, known limitations; executed commands, results, coverage, blocked items.

**Technical justification:** Reviewers need to know what exists, how to run it and what was actually verified.

**Depends on:** commit 25

**Files added:**

- `README.md`
- `docs/TESTING.md`

**Files modified:**

- none

**Commands:**

```bash
git add README.md \
  docs/TESTING.md
git commit -m "docs: add README and test report"
```

## Commit 27

`docs: add commit history`

**Objective:** Document how the repository history was built and how to reproduce it.

**Implemented:** This file.

**Technical justification:** Required deliverable; generated from the actual commits so it cannot drift from them.

**Depends on:** commit 01, commit 02, commit 03, commit 04, commit 05, commit 06, commit 07, commit 08, commit 09, commit 10, commit 11, commit 12, commit 13, commit 14, commit 15, commit 16, commit 17, commit 18, commit 19, commit 20, commit 21, commit 22, commit 23, commit 24, commit 25, commit 26

**Files added:**

- `docs/COMMIT_HISTORY.md`

**Files modified:**

- none

**Commands:**

```bash
git add docs/COMMIT_HISTORY.md
git commit -m "docs: add commit history"
```
