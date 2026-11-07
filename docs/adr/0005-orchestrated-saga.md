# ADR-0005: Orchestrated saga with sequential reverse-order compensation

## Context
Steps call independent systems (stock, payment, invoicing). A failure after
some succeeded must be undone by business operations — not a database
rollback. Compensations can fail too.

## Decision
* Orchestration (the engine decides), not choreography.
* A step may declare `compensation: {handler, timeout, retry}`. When an
  action fails permanently (or the execution is cancelled with
  `compensate: true`), the engine waits for running siblings, then creates
  one compensation task per completed compensable step and runs them **one at
  a time, most recently completed first**.
* Compensations receive the original output. They are persisted tasks with
  their own retries, leases and attempts, so they resume after crashes.
* If a compensation fails permanently, the saga stops: execution `FAILED`,
  remaining compensations stay `PENDING`, both errors are recorded.

## Consequences
* Correct reverse order even for DAGs (completion order is a valid
  topological order).
* Sequential compensation is slower than parallel, but avoids undoing B
  before A when A depends on B's effects. Parallel compensation of
  independent branches is a possible optimisation.
* Stopping on compensation failure requires operator action; GoFlow does not
  offer a "retry compensation" endpoint yet (known limitation).
