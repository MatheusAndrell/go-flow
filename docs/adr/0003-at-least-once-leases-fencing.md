# ADR-0003: At-least-once execution with leases, fencing and idempotency keys

## Context
Workers can crash, freeze (GC pause, network partition) or lose the
connection after running a handler but before storing its result.
Exactly-once execution of arbitrary side effects is impossible without the
cooperation of the external system.

## Decision
* A claim grants a **lease** (`lease_expires_at`, database clock) and a new
  **fencing token** (`lease_token + 1`). Workers heartbeat every lease/3.
* Outcomes are accepted only if the task is still `RUNNING` with the same
  token, checked under the execution row lock. Stale outcomes get
  `ErrLeaseLost` and are discarded.
* A recovery loop (in every API instance) fails tasks whose lease expired
  with `LEASE_EXPIRED` (retryable) and lets the retry policy decide.
* Handlers receive `IdempotencyKey = execution/kind/step`, identical across
  attempts, to deduplicate external side effects.
* The worker cancels a task's context when the heartbeat reports the lease
  lost, and cancels everything if heartbeats fail for longer than a lease.

## Consequences
* Guarantee: at-least-once delivery, at-most-once application of each
  outcome to the workflow state. Documented explicitly; never "exactly once".
* A handler may run twice concurrently in a narrow window (worker frozen
  past its lease, then resuming). Fencing protects GoFlow's state; handlers
  must use the idempotency key to protect external state.
* Lease duration trades recovery time against false expirations
  (default 30 s lease, 10 s heartbeat).
