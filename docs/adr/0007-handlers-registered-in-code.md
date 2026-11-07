# ADR-0007: Steps run only handlers registered in code

## Context
Workflow engines that evaluate scripts or call arbitrary URLs from
definitions expose remote code execution and SSRF risks, especially in a
multi-tenant API.

## Decision
A step references a handler **name**. Handlers are Go implementations of
`pkg/handler.Handler` registered in a `Registry` by a worker binary — the
built-in `goflow worker` (example handlers, opt-in) or an application using
`pkg/goflow.RunWorker`. Workers advertise their handler names in the
`workers` table; publishing rejects unknown handlers; workers claim only
tasks whose handler they registered.

## Consequences
* No user-supplied code or URLs are executed.
* Integrating a new step type requires deploying a worker with that handler
  (the Temporal/Conductor model).
* Workers connect directly to PostgreSQL, so they are trusted components.
  A remote worker protocol (poll/complete over HTTP or gRPC) would allow
  untrusted or non-Go workers; it is future work.
