# ADR-0002: PostgreSQL as system of record and task queue

## Context
The engine must persist every state transition and also distribute tasks to
workers. The usual design adds a broker (RabbitMQ) and/or Redis. That creates
a dual write — "commit state, then publish message" — which needs a
transactional outbox and still leaves two systems that can disagree
(message lost, message for a rolled-back state, redelivery after ack loss).

## Decision
Use PostgreSQL for both. A task is runnable when its row is `READY` or
`RETRY_SCHEDULED` with `run_at <= now()`; workers claim with
`SELECT ... FOR UPDATE SKIP LOCKED` in a CTE that also sets the lease and
inserts the attempt row. Retries and scheduled executions are due-time
columns. No RabbitMQ and no Redis in v1. No outbox, because nothing is
published outside the database.

## Consequences
* State change and "enqueue" are the same transaction: no lost or phantom
  tasks, no outbox relay, one system to operate and back up.
* Retry scheduling and scheduled executions survive restarts by construction.
* Throughput is bounded by PostgreSQL write capacity. Measured locally:
  ~250 executions/s (≈750 tasks/s) on a 2-vCPU container with 4 workers
  (`make bench`). This is ample for orchestration workloads, where tasks are
  usually much slower than the engine.
* Idle workers poll (default 500 ms with jitter). Latency to pick up new work
  is bounded by the poll interval. `LISTEN/NOTIFY` could cut it but adds
  connection management; deferred.
* If external event publication is needed (webhooks, Kafka), add an
  `outbox` table written by the same transaction and a relay using the same
  `SKIP LOCKED` pattern. A broker can then be introduced behind the
  `worker.Queue` port without touching the domain.
