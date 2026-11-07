# Observability

## Logs

Structured JSON via `log/slog` (`GOFLOW_LOG_FORMAT=text` for development).
A context-aware handler adds to every record logged with a context:

* `correlation_id` — from the `X-Correlation-ID` request header or generated,
  echoed in the response and in problem+json bodies;
* `trace_id` / `span_id` — from the active OpenTelemetry span.

Worker logs carry `worker_id`, `task_id`, `execution_id`, `step`, `handler`,
`attempt` and the `trace_id` of the request that started the execution.

## Distributed tracing

OpenTelemetry SDK with W3C Trace Context. The HTTP middleware creates a
server span per request (named after the route). When an execution is
created, the current trace context is stored in `executions.trace_parent`;
every task span (`task <handler>`, kind consumer) is started as a child of
it, in whichever worker process runs it. One trace therefore shows the API
call and every task attempt of the execution, across processes and retries.

Export: set `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` (OTLP/HTTP, e.g.
`http://jaeger:4318/v1/traces`). Without it spans are still created (ids
appear in logs and propagate) but are not exported.
`GOFLOW_TRACE_SAMPLE_RATIO` sets parent-based ratio sampling.

## Metrics (Prometheus)

`/metrics` on the API port and on `:9090` for workers.

| Metric | Type | Labels | Answers |
|---|---|---|---|
| `goflow_executions_started_total` | counter | workflow | throughput |
| `goflow_executions_finished_total` | counter | workflow, status | success rate, failures |
| `goflow_execution_duration_seconds` | histogram | workflow, status | end-to-end latency |
| `goflow_executions_active` | gauge (sampled from DB) | status | active / waiting / paused workflows |
| `goflow_tasks_finished_total` | counter | handler, kind, outcome | task throughput and failures |
| `goflow_task_duration_seconds` | histogram | handler | processing time |
| `goflow_task_queue_wait_seconds` | histogram | handler | wait time between runnable and claimed |
| `goflow_task_retries_total` | counter | handler | retries |
| `goflow_tasks_retry_scheduled` | gauge (sampled) | — | tasks currently waiting for a retry |
| `goflow_dead_letters_total` | counter | handler | DLQ inflow |
| `goflow_task_leases_expired_total` | counter | — | crashed/stalled workers |
| `goflow_worker_in_flight_tasks`, `goflow_worker_capacity` | gauge | worker | utilisation / saturation |
| `goflow_http_requests_total`, `goflow_http_request_duration_seconds` | counter, histogram | method, route, code | API RED metrics |
| `goflow_http_rate_limited_total` | counter | — | throttling |

Labels are bounded (route patterns, handler and workflow names, statuses);
ids and tenants are never labels. Go runtime and process collectors are
included.

The provisioned Grafana dashboard
([`goflow-overview.json`](../../deployments/monitoring/grafana/dashboards/goflow-overview.json))
shows success rate, active executions, retries, DLQ, latencies (p50/p95),
queue wait, worker utilisation and HTTP traffic.

## Health

* `GET /health` — liveness (process up).
* `GET /ready` — readiness: database reachable and all embedded migrations
  applied. Containers use `goflow probe URL` as healthcheck (distroless image
  has no shell).

## Investigating an execution

1. `GET /api/v1/executions/{id}` — status and error.
2. `GET /api/v1/executions/{id}/history` — every transition with messages
   (retry times, error kinds, compensations).
3. `GET /api/v1/tasks/{task}/attempts` — per-attempt worker, duration, error.
4. Jaeger: search the `trace_id` from logs to see the API span and every
   task span.
