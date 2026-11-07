# ADR-0004: Execution and tasks as one aggregate with a single write path

## Context
Deciding what happens after a task outcome (ready dependents, complete,
retry, skip siblings, start compensation, wait for in-flight siblings)
requires a consistent view of the execution and all its tasks. Parallel
branches finish concurrently; cancellations and recoveries race with them.

## Decision
`execution.Run` is the aggregate root (execution + tasks). Every command goes
through `Store.UpdateRun(executionID, fn)`, which locks the execution
(`FOR NO KEY UPDATE`) and then its tasks (`FOR UPDATE`, ordered), takes the
time from the database, runs the pure domain command, and writes all recorded
changes (tasks, events, attempts, dead letters) in one batch and one
transaction. Serialization failures and deadlocks are retried.

## Consequences
* No lost updates between parallel branches; cancel/complete races resolve
  to exactly one winner; every transition has a history event in the same
  transaction.
* Lock order execution → tasks prevents deadlocks between commands; claims
  use `SKIP LOCKED` and never wait on these locks.
* Commands on the same execution are serialized. Executions are independent,
  so throughput scales with the number of executions, not of steps per
  execution. Very wide fan-outs (hundreds of parallel steps in one
  execution) would contend; the 100-step limit keeps this bounded.
