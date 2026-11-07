package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/MatheusAndrell/go-flow/internal/application/engine"
	"github.com/MatheusAndrell/go-flow/internal/application/worker"
	"github.com/MatheusAndrell/go-flow/internal/domain/execution"
)

func (s *Store) Register(ctx context.Context, r worker.Registration) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO workers (id, hostname, handlers, concurrency, status, started_at, last_heartbeat_at)
		VALUES ($1, $2, $3, $4, 'ACTIVE', now(), now())
		ON CONFLICT (id) DO UPDATE SET hostname = EXCLUDED.hostname, handlers = EXCLUDED.handlers,
			concurrency = EXCLUDED.concurrency, status = 'ACTIVE', started_at = now(), last_heartbeat_at = now(), stopped_at = NULL`,
		r.ID, r.Hostname, r.Handlers, r.Concurrency)
	return err
}

func (s *Store) Deregister(ctx context.Context, workerID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE workers SET status = 'STOPPED', stopped_at = now() WHERE id = $1`, workerID)
	return err
}

// claimSQL atomically moves runnable tasks to RUNNING. SKIP LOCKED lets many
// workers poll concurrently without blocking each other or double-claiming,
// and never waits on rows locked by UpdateRun. Each claim increments the
// fencing token and opens an attempt record.
const claimSQL = `
WITH candidates AS (
	SELECT t.id, t.run_at
	FROM tasks t
	JOIN executions e ON e.id = t.execution_id
	WHERE t.status IN ('READY', 'RETRY_SCHEDULED')
	  AND t.run_at <= now()
	  AND t.handler = ANY($2)
	  AND e.status IN ('RUNNING', 'WAITING', 'COMPENSATING')
	ORDER BY t.run_at, t.id
	LIMIT $3
	FOR UPDATE OF t SKIP LOCKED
), claimed AS (
	UPDATE tasks t
	SET status = 'RUNNING', attempts = t.attempts + 1, lease_owner = $1, lease_token = t.lease_token + 1,
	    lease_expires_at = now() + $4 * interval '1 millisecond', started_at = COALESCE(t.started_at, now()), updated_at = now()
	FROM candidates c
	WHERE t.id = c.id
	RETURNING t.id, t.execution_id, t.tenant_id, t.step_key, t.handler, t.kind, t.attempts, t.lease_token,
	          t.input, t.timeout_ms, c.run_at
), attempts AS (
	INSERT INTO task_attempts (id, task_id, execution_id, tenant_id, number, worker_id, lease_token, status, started_at)
	SELECT gen_random_uuid(), id, execution_id, tenant_id, attempts, $1, lease_token, 'RUNNING', now() FROM claimed
)
SELECT c.id, c.execution_id, c.tenant_id, c.step_key, c.handler, c.kind, c.attempts, c.lease_token, c.input,
       c.timeout_ms, c.run_at, now(), e.trace_parent
FROM claimed c JOIN executions e ON e.id = c.execution_id`

func (s *Store) Claim(ctx context.Context, workerID string, handlers []string, limit int, lease time.Duration) ([]engine.ClaimedTask, error) {
	rows, err := s.pool.Query(ctx, claimSQL, workerID, handlers, limit, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (engine.ClaimedTask, error) {
		var (
			t         engine.ClaimedTask
			input     []byte
			timeoutMS int64
			kind      string
		)
		err := r.Scan(&t.TaskID, &t.ExecutionID, &t.TenantID, &t.StepKey, &t.Handler, &kind, &t.Attempt, &t.Token,
			&input, &timeoutMS, &t.RunAt, &t.ClaimedAt, &t.TraceParent)
		t.Kind = execution.TaskKind(kind)
		t.Input = input
		t.Timeout = time.Duration(timeoutMS) * time.Millisecond
		return t, err
	})
}

// Heartbeat extends leases still held with the same fencing token and
// refreshes the worker's liveness.
func (s *Store) Heartbeat(ctx context.Context, workerID string, leases []engine.Lease, lease time.Duration) ([]string, error) {
	ids := make([]string, len(leases))
	tokens := make([]int64, len(leases))
	for i, l := range leases {
		ids[i], tokens[i] = l.TaskID, l.Token
	}
	var alive []string
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE workers SET last_heartbeat_at = now(), status = 'ACTIVE', stopped_at = NULL WHERE id = $1`, workerID); err != nil {
			return err
		}
		if len(leases) == 0 {
			alive = nil
			return nil
		}
		rows, err := tx.Query(ctx, `UPDATE tasks t SET lease_expires_at = now() + $4 * interval '1 millisecond'
			FROM unnest($2::uuid[], $3::bigint[]) AS l(id, token)
			WHERE t.id = l.id AND t.lease_token = l.token AND t.status = 'RUNNING' AND t.lease_owner = $1
			RETURNING t.id`, workerID, ids, tokens, lease.Milliseconds())
		if err != nil {
			return err
		}
		alive, err = pgx.CollectRows(rows, pgx.RowTo[string])
		return err
	})
	return alive, err
}

func (s *Store) ExpiredLeases(ctx context.Context, limit int) ([]engine.TaskRef, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, execution_id FROM tasks
		WHERE status = 'RUNNING' AND lease_expires_at < now() ORDER BY lease_expires_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (engine.TaskRef, error) {
		var ref engine.TaskRef
		err := r.Scan(&ref.TaskID, &ref.ExecutionID)
		return ref, err
	})
}

func (s *Store) DueExecutions(ctx context.Context, limit int) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT id FROM executions WHERE status = 'PENDING' AND scheduled_at <= now()
		ORDER BY scheduled_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func (s *Store) MarkStaleWorkersStopped(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE workers SET status = 'STOPPED', stopped_at = now()
		WHERE status = 'ACTIVE' AND last_heartbeat_at < now() - $1 * interval '1 millisecond'`, olderThan.Milliseconds())
	return tag.RowsAffected(), err
}

// Counts returns the number of executions per status and of tasks waiting for a retry.
func (s *Store) Counts(ctx context.Context) (map[execution.Status]int64, int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM executions
		WHERE status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'COMPENSATED') GROUP BY status`)
	if err != nil {
		return nil, 0, err
	}
	counts := map[execution.Status]int64{}
	for rows.Next() {
		var st string
		var n int64
		if err := rows.Scan(&st, &n); err != nil {
			rows.Close()
			return nil, 0, err
		}
		counts[execution.Status(st)] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	var retrying int64
	err = s.pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE status = 'RETRY_SCHEDULED'`).Scan(&retrying)
	return counts, retrying, err
}
