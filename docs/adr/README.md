# Architecture Decision Records

| # | Decision | Status |
|---|---|---|
| [0001](0001-modular-monolith-hexagonal.md) | Modular monolith with hexagonal layering | Accepted |
| [0002](0002-postgresql-as-queue.md) | PostgreSQL as system of record and task queue; no RabbitMQ/Redis in v1 | Accepted |
| [0003](0003-at-least-once-leases-fencing.md) | At-least-once execution with leases, fencing tokens and idempotency keys | Accepted |
| [0004](0004-run-aggregate-single-write-path.md) | Execution + tasks as one aggregate with a single locked write path | Accepted |
| [0005](0005-orchestrated-saga.md) | Orchestrated saga with sequential reverse-order compensation | Accepted |
| [0006](0006-immutable-versioned-definitions.md) | Versioned, immutable workflow definitions validated as DAGs | Accepted |
| [0007](0007-handlers-registered-in-code.md) | Steps run only handlers registered in code; no user-supplied code | Accepted |
| [0008](0008-api-keys-tenant-isolation.md) | API-key authentication with tenant-scoped data access | Accepted |
| [0009](0009-observability-stack.md) | slog + Prometheus + OpenTelemetry with persisted trace context | Accepted |

Format: context, decision, consequences (Michael Nygard).
