# ADR-0001: Modular monolith with hexagonal layering

## Context
GoFlow needs clear boundaries (definitions, executions, workers, identity)
and must be testable without infrastructure. The expected load of a first
version fits comfortably in one deployable unit; microservices would add
network failure modes, distributed transactions and operational cost without
a scaling need.

## Decision
One Go module and one binary with process roles (`serve`, `worker`, `all`).
Inside, packages follow ports and adapters: `domain` (pure), `application`
(use cases declaring consumer-side interfaces), `infrastructure`/`platform`
(adapters), `interfaces` (HTTP), wired only in `internal/app`. Execution,
task execution and compensation share one package because they share one
aggregate (ADR-0004); other contexts are separate packages.

## Consequences
* Domain logic (state machines, DAG progression, saga planning, backoff) is
  unit-tested without a database.
* API and workers scale independently already (separate processes), without
  separate codebases.
* Extracting a service later means moving an application package behind a
  network port; the interfaces it consumes already exist.
* Discipline is required to keep imports pointing inwards; Go's `internal`
  and the absence of import cycles enforce part of it.
