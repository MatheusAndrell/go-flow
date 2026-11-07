# Data model

Schema: [`migrations/0001_init.sql`](../../migrations/0001_init.sql). Migrations
are embedded in the binary, applied in order under an advisory lock, one
transaction per file, and checksummed (an edited, already-applied migration
is an error). `goflow migrate` applies them; `GOFLOW_AUTO_MIGRATE=true` does
it at startup.

```mermaid
erDiagram
    tenants ||--o{ api_keys : owns
    tenants ||--o{ workflow_definitions : owns
    workflow_definitions ||--|{ workflow_versions : "has versions"
    workflow_versions ||--o{ executions : "bound to"
    executions ||--|{ tasks : contains
    tasks ||--o{ task_attempts : "attempted as"
    tasks |o--o| tasks : compensates
    executions ||--o{ execution_events : history
    tasks ||--o| dead_letters : "dead-lettered as"
    tenants ||--o{ idempotency_keys : scopes

    tenants {
        uuid id PK
        text name UK
    }
    api_keys {
        uuid id PK
        uuid tenant_id FK
        bytea key_hash UK
        text_array permissions
        timestamptz revoked_at
    }
    workflow_definitions {
        uuid id PK
        uuid tenant_id FK
        text name
        text description
    }
    workflow_versions {
        uuid id PK
        uuid definition_id FK
        int number
        text status
        jsonb spec
    }
    executions {
        uuid id PK
        uuid tenant_id FK
        uuid version_id FK
        text status
        jsonb input
        jsonb output
        text idempotency_key
        text trace_parent
        timestamptz scheduled_at
    }
    tasks {
        uuid id PK
        uuid execution_id FK
        text step_key
        text kind
        text status
        int attempts
        bigint lease_token
        timestamptz lease_expires_at
        timestamptz run_at
        jsonb retry_policy
    }
    task_attempts {
        uuid id PK
        uuid task_id FK
        int number
        text worker_id
        text status
        text error_kind
    }
    execution_events {
        bigserial id PK
        uuid execution_id FK
        text type
        text from_status
        text to_status
    }
    dead_letters {
        uuid id PK
        uuid task_id FK
        text error_kind
        int attempts
    }
    workers {
        text id PK
        text_array handlers
        text status
        timestamptz last_heartbeat_at
    }
    idempotency_keys {
        uuid tenant_id PK
        text key PK
        bytea fingerprint
        text state
        int response_status
    }
```

## Notable constraints and indexes

| Object | Purpose |
|---|---|
| `workflow_versions_single_draft_idx` (partial unique) | At most one draft per workflow |
| trigger `workflow_versions_immutable` | Rejects UPDATE/DELETE of a `PUBLISHED` version even if application code regresses |
| `UNIQUE (tenant_id, idempotency_key)` on executions | Deduplicates concurrent starts |
| `UNIQUE (execution_id, kind, step_key)` on tasks | One action and at most one compensation per step |
| `UNIQUE (task_id, number)` on attempts | Attempt history cannot fork |
| `UNIQUE task_id` on dead_letters | A task is dead-lettered once |
| `tasks_runnable_idx (run_at) WHERE status IN ('READY','RETRY_SCHEDULED')` | Hot path for claims and persistent retry scheduling |
| `tasks_lease_idx (lease_expires_at) WHERE status = 'RUNNING'` | Recovery of abandoned leases |
| `executions_due_idx (scheduled_at) WHERE status = 'PENDING'` | Scheduled executions |
| `(tenant_id, created_at DESC, id DESC)` indexes | Keyset pagination per tenant |
| `CHECK` constraints on every status column | The database rejects unknown states |

Every tenant-owned table carries `tenant_id`, and every API query filters by
it (see [security](security.md)).

## What is stored where

* **Execution input/output, task input/output** — `jsonb`. Task input is the
  envelope built when the task becomes ready (workflow input, params,
  dependency outputs), so a retry sees exactly the same input.
* **Retry policy and compensation spec** — copied onto each task at creation,
  so in-flight executions are unaffected by later definition versions.
* **Worker state** — `workers` keeps id, handlers, concurrency and last
  heartbeat. The `handlers` column is the catalog checked when publishing.
  Workers silent for 2 minutes are marked `STOPPED` by housekeeping.
* **Schedules** — `executions.scheduled_at` and `tasks.run_at`; no timers in
  memory.
