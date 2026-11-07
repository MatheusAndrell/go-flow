# ADR-0009: slog, Prometheus and OpenTelemetry with persisted trace context

## Context
Investigating an execution means following it from the API call through
tasks executed later, possibly by other processes and after retries.

## Decision
* `log/slog` JSON logs enriched with correlation id, trace id and span id.
* Prometheus metrics with bounded labels; DB-derived gauges (active
  executions, tasks awaiting retry) sampled by the scheduler.
* OpenTelemetry tracing with W3C propagation. The trace context of the
  starting request is stored in `executions.trace_parent`; task spans are
  children of it in whichever worker runs them. OTLP/HTTP export (Jaeger in
  Docker Compose) is optional.

## Consequences
* One trace per execution across processes and time; logs link to traces.
* Long-running executions produce long traces; sampling is parent-based so
  a sampled-out request produces no task spans either.
* Prometheus client and OTel tracing are separate libraries (OTel metrics
  were not adopted to keep the well-known Prometheus API).
