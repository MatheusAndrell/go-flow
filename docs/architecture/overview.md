# Architecture overview

GoFlow is a **modular monolith** built with hexagonal (ports and adapters)
layering. One binary runs in three roles — `serve` (REST API + scheduler),
`worker` and `all` — and every role coordinates exclusively through
PostgreSQL. There is no in-memory state that a crash could lose.

## C4 — Level 1: system context

```mermaid
flowchart LR
    client["Client application<br/>(starts and observes workflows)"]
    operator["Operator<br/>(dashboards, traces, CLI)"]
    workerApp["Application worker<br/>(embeds pkg/goflow with its own handlers)"]
    goflow["GoFlow<br/>workflow orchestration engine"]
    ext["External systems<br/>(payments, stock, e-mail...)"]

    client -- "REST/JSON, API key" --> goflow
    operator -- "Grafana / Jaeger / goflow CLI" --> goflow
    workerApp -- "claims tasks, reports results" --> goflow
    workerApp -- "side effects (idempotency key)" --> ext
    goflow -- "built-in/example handlers" --> ext
```

## C4 — Level 2: containers

```mermaid
flowchart TB
    subgraph runtime["GoFlow processes (same binary)"]
        api["goflow serve<br/>HTTP API + scheduler/recovery loop"]
        worker["goflow worker (N replicas)<br/>worker pool + heartbeats"]
        sdk["pkg/goflow worker<br/>embedded in an application"]
    end
    pg[("PostgreSQL 16<br/>system of record + task queue")]
    prom["Prometheus"]
    graf["Grafana"]
    jaeger["Jaeger (OTLP)"]

    api -- "SQL (pgx)" --> pg
    worker -- "SQL: SKIP LOCKED claims, leases" --> pg
    sdk -- "SQL" --> pg
    prom -- "scrape /metrics" --> api
    prom -- "scrape :9090/metrics" --> worker
    graf --> prom
    api -- "OTLP/HTTP spans" --> jaeger
    worker -- "OTLP/HTTP spans" --> jaeger
```

## C4 — Level 3: components and dependency rule

```mermaid
flowchart TB
    subgraph interfaces["interfaces"]
        http["httpapi<br/>routes, auth, rate limit,<br/>idempotency, problem+json"]
        cli["cmd/goflow<br/>CLI"]
    end
    subgraph application["application (use cases + ports)"]
        defs["definitions"]
        execs["executions"]
        eng["engine"]
        wrk["worker"]
        sched["scheduler"]
        acc["access"]
        idem["idempotency (port)"]
    end
    subgraph domain["domain (pure Go, no I/O)"]
        wf["workflow<br/>Spec, DAG, versions"]
        ex["execution<br/>Run aggregate, state machines, saga"]
        rt["retry<br/>backoff + jitter"]
        id["identity<br/>principals, API keys"]
    end
    subgraph infrastructure["infrastructure / platform"]
        pg["postgres<br/>repositories, queue, migrator"]
        tel["telemetry<br/>slog, Prometheus, OTel"]
        cfg["config"]
    end
    pub["pkg/handler + pkg/goflow<br/>public contract"]

    http --> defs & execs & acc & idem
    cli --> acc
    defs & execs & eng & wrk & sched & acc --> domain
    wrk --> pub
    pg -. implements ports .-> application
    tel -. implements recorders .-> application
```

Dependencies point inwards only. The domain imports nothing but the standard
library. Application packages declare the small interfaces they consume
(`definitions.Store`, `worker.Queue`, `engine.Recorder`...) and the
infrastructure satisfies them; `internal/app` is the only place where
concrete types meet.

## Bounded contexts

The eight contexts proposed initially were consolidated where splitting them
would only add indirection:

| Context | Package(s) | Notes |
|---|---|---|
| Workflow Definition | `domain/workflow`, `application/definitions` | Versioned DAG specs; published versions are immutable. |
| Workflow Execution + Task Execution + Compensation | `domain/execution`, `application/executions`, `application/engine` | One aggregate (`Run`): deciding what runs next, retries and saga compensation all need a consistent view of the execution **and all of its tasks**. Separate aggregates would require distributed coordination inside one database transaction. |
| Retry and Scheduling | `domain/retry`, `application/scheduler` | Backoff policy is a value object; scheduling is a due-time column (`run_at`, `scheduled_at`) polled from the database. |
| Worker Management | `application/worker`, `workers` table | Worker pool, leases, heartbeats, handler catalog. |
| Identity and Access | `domain/identity`, `application/access` | Tenants, API keys, permissions. |
| Observability | `platform/telemetry` | Cross-cutting; implements recorder ports. |

## Package layout

```text
cmd/goflow                 CLI and process entry point
internal/domain            entities, value objects, state machines (no I/O)
internal/application       use cases and the ports they need
internal/infrastructure    PostgreSQL adapters and migrator
internal/interfaces        HTTP API
internal/platform          config and telemetry
internal/app               composition root
internal/examples/orders   example handlers (opt-in)
pkg/handler                public handler contract (Handler, Registry, Permanent)
pkg/goflow                 public SDK to run a worker inside another application
migrations                 embedded SQL migrations
test/integration           PostgreSQL integration, concurrency and failure tests
```

## Process roles

| Command | Runs | Scales |
|---|---|---|
| `goflow serve` | HTTP API, scheduler (activation of scheduled executions, lease recovery, housekeeping, gauges) | Horizontally; every loop is safe to run on many instances. |
| `goflow worker` | Worker pool for the registered handlers, `/metrics` and `/health` on `:9090` | Horizontally; claims never overlap (`FOR UPDATE SKIP LOCKED`). |
| `goflow all` | Both, for local development | — |
| `pkg/goflow.RunWorker` | A worker with application-defined handlers | Like `goflow worker`. |

See also: [execution model](execution-model.md), [concurrency and
resilience](concurrency-and-resilience.md), [data model](data-model.md),
[security](security.md), [observability](observability.md) and the
[ADRs](../adr/).
