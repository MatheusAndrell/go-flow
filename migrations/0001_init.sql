-- Identity and access
CREATE TABLE tenants (
    id         uuid PRIMARY KEY,
    name       text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
    id          uuid PRIMARY KEY,
    tenant_id   uuid NOT NULL REFERENCES tenants (id),
    name        text NOT NULL,
    prefix      text NOT NULL,
    key_hash    bytea NOT NULL UNIQUE,
    permissions text[] NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    revoked_at  timestamptz
);
CREATE INDEX api_keys_tenant_idx ON api_keys (tenant_id);

-- Workflow definitions
CREATE TABLE workflow_definitions (
    id          uuid PRIMARY KEY,
    tenant_id   uuid NOT NULL REFERENCES tenants (id),
    name        text NOT NULL,
    description text NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL,
    UNIQUE (tenant_id, name)
);
CREATE INDEX workflow_definitions_page_idx ON workflow_definitions (tenant_id, created_at DESC, id DESC);

CREATE TABLE workflow_versions (
    id            uuid PRIMARY KEY,
    definition_id uuid NOT NULL REFERENCES workflow_definitions (id) ON DELETE CASCADE,
    tenant_id     uuid NOT NULL REFERENCES tenants (id),
    number        integer NOT NULL CHECK (number > 0),
    status        text NOT NULL CHECK (status IN ('DRAFT', 'PUBLISHED')),
    spec          jsonb NOT NULL,
    created_at    timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL,
    published_at  timestamptz,
    UNIQUE (definition_id, number)
);
CREATE UNIQUE INDEX workflow_versions_single_draft_idx ON workflow_versions (definition_id) WHERE status = 'DRAFT';

-- Defense in depth: a published version is immutable even if application code regresses.
CREATE FUNCTION goflow_forbid_published_version_change() RETURNS trigger AS $$
BEGIN
    IF OLD.status = 'PUBLISHED' THEN
        RAISE EXCEPTION 'workflow version % is published and immutable', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER workflow_versions_immutable
    BEFORE UPDATE OR DELETE ON workflow_versions
    FOR EACH ROW EXECUTE FUNCTION goflow_forbid_published_version_change();

-- Executions
CREATE TABLE executions (
    id              uuid PRIMARY KEY,
    tenant_id       uuid NOT NULL REFERENCES tenants (id),
    definition_id   uuid NOT NULL REFERENCES workflow_definitions (id),
    version_id      uuid NOT NULL REFERENCES workflow_versions (id),
    workflow_name   text NOT NULL,
    version_number  integer NOT NULL,
    status          text NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'WAITING', 'PAUSED', 'COMPLETED', 'FAILED', 'CANCELLED', 'COMPENSATING', 'COMPENSATED')),
    input           jsonb NOT NULL,
    output          jsonb,
    error           text NOT NULL DEFAULT '',
    idempotency_key text,
    max_parallelism integer NOT NULL DEFAULT 0,
    trace_parent    text NOT NULL DEFAULT '',
    scheduled_at    timestamptz NOT NULL,
    created_at      timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL,
    started_at      timestamptz,
    completed_at    timestamptz,
    UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX executions_page_idx ON executions (tenant_id, created_at DESC, id DESC);
CREATE INDEX executions_due_idx ON executions (scheduled_at) WHERE status = 'PENDING';
CREATE INDEX executions_status_idx ON executions (status);

CREATE TABLE tasks (
    id                  uuid PRIMARY KEY,
    execution_id        uuid NOT NULL REFERENCES executions (id) ON DELETE CASCADE,
    tenant_id           uuid NOT NULL REFERENCES tenants (id),
    step_key            text NOT NULL,
    name                text NOT NULL,
    handler             text NOT NULL,
    kind                text NOT NULL CHECK (kind IN ('ACTION', 'COMPENSATION')),
    position            integer NOT NULL,
    depends_on          text[] NOT NULL DEFAULT '{}',
    status              text NOT NULL CHECK (status IN ('PENDING', 'READY', 'RUNNING', 'RETRY_SCHEDULED', 'COMPLETED', 'FAILED', 'CANCELLED', 'SKIPPED', 'COMPENSATED')),
    params              jsonb,
    input               jsonb,
    output              jsonb,
    attempts            integer NOT NULL DEFAULT 0,
    retry_policy        jsonb NOT NULL,
    timeout_ms          bigint NOT NULL CHECK (timeout_ms > 0),
    compensation        jsonb,
    compensates_task_id uuid REFERENCES tasks (id),
    run_at              timestamptz,
    lease_owner         text NOT NULL DEFAULT '',
    lease_token         bigint NOT NULL DEFAULT 0,
    lease_expires_at    timestamptz,
    last_error          text NOT NULL DEFAULT '',
    last_error_kind     text NOT NULL DEFAULT '',
    created_at          timestamptz NOT NULL,
    updated_at          timestamptz NOT NULL,
    started_at          timestamptz,
    completed_at        timestamptz,
    UNIQUE (execution_id, kind, step_key)
);
CREATE INDEX tasks_execution_idx ON tasks (execution_id, kind, position);
-- Hot path: workers polling for runnable tasks.
CREATE INDEX tasks_runnable_idx ON tasks (run_at) WHERE status IN ('READY', 'RETRY_SCHEDULED');
-- Recovery: finding leases abandoned by dead workers.
CREATE INDEX tasks_lease_idx ON tasks (lease_expires_at) WHERE status = 'RUNNING';

CREATE TABLE task_attempts (
    id           uuid PRIMARY KEY,
    task_id      uuid NOT NULL REFERENCES tasks (id) ON DELETE CASCADE,
    execution_id uuid NOT NULL REFERENCES executions (id) ON DELETE CASCADE,
    tenant_id    uuid NOT NULL REFERENCES tenants (id),
    number       integer NOT NULL,
    worker_id    text NOT NULL,
    lease_token  bigint NOT NULL,
    status       text NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    error        text NOT NULL DEFAULT '',
    error_kind   text NOT NULL DEFAULT '',
    started_at   timestamptz NOT NULL,
    finished_at  timestamptz,
    UNIQUE (task_id, number)
);

CREATE TABLE execution_events (
    id           bigserial PRIMARY KEY,
    execution_id uuid NOT NULL REFERENCES executions (id) ON DELETE CASCADE,
    tenant_id    uuid NOT NULL REFERENCES tenants (id),
    task_id      uuid,
    step_key     text NOT NULL DEFAULT '',
    type         text NOT NULL,
    from_status  text NOT NULL DEFAULT '',
    to_status    text NOT NULL DEFAULT '',
    message      text NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL
);
CREATE INDEX execution_events_execution_idx ON execution_events (execution_id, id);

CREATE TABLE dead_letters (
    id           uuid PRIMARY KEY,
    tenant_id    uuid NOT NULL REFERENCES tenants (id),
    execution_id uuid NOT NULL REFERENCES executions (id) ON DELETE CASCADE,
    task_id      uuid NOT NULL UNIQUE REFERENCES tasks (id) ON DELETE CASCADE,
    step_key     text NOT NULL,
    handler      text NOT NULL,
    kind         text NOT NULL,
    attempts     integer NOT NULL,
    error        text NOT NULL,
    error_kind   text NOT NULL,
    input        jsonb,
    created_at   timestamptz NOT NULL
);
CREATE INDEX dead_letters_page_idx ON dead_letters (tenant_id, created_at DESC, id DESC);

-- Worker registry. handlers is the catalog consulted when publishing workflows.
CREATE TABLE workers (
    id                text PRIMARY KEY,
    hostname          text NOT NULL,
    handlers          text[] NOT NULL,
    concurrency       integer NOT NULL,
    status            text NOT NULL CHECK (status IN ('ACTIVE', 'STOPPED')),
    started_at        timestamptz NOT NULL,
    last_heartbeat_at timestamptz NOT NULL,
    stopped_at        timestamptz
);

-- HTTP idempotency keys: one stored response per (tenant, key).
CREATE TABLE idempotency_keys (
    tenant_id       uuid NOT NULL REFERENCES tenants (id),
    key             text NOT NULL,
    fingerprint     bytea NOT NULL,
    state           text NOT NULL CHECK (state IN ('PROCESSING', 'COMPLETED')),
    response_status integer,
    response_body   bytea,
    created_at      timestamptz NOT NULL DEFAULT now(),
    expires_at      timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, key)
);
CREATE INDEX idempotency_keys_expiry_idx ON idempotency_keys (expires_at);
