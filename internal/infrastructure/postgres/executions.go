package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/MatheusAndrell/go-flow/internal/application"
	"github.com/MatheusAndrell/go-flow/internal/application/executions"
	"github.com/MatheusAndrell/go-flow/internal/domain/execution"
	"github.com/MatheusAndrell/go-flow/internal/domain/retry"
)

const executionColumns = `id, tenant_id, definition_id, version_id, workflow_name, version_number, status, input, output,
	error, idempotency_key, max_parallelism, trace_parent, scheduled_at, created_at, updated_at, started_at, completed_at`

const taskColumns = `id, execution_id, tenant_id, step_key, name, handler, kind, position, depends_on, status, params,
	input, output, attempts, retry_policy, timeout_ms, compensation, compensates_task_id, run_at, lease_owner,
	lease_token, lease_expires_at, last_error, last_error_kind, created_at, updated_at, started_at, completed_at`

func (s *Store) CreateRun(ctx context.Context, run *execution.Run) error {
	return mapErr(s.inTx(ctx, func(tx pgx.Tx) error {
		e := run.Execution
		if _, err := tx.Exec(ctx, `INSERT INTO executions (`+executionColumns+`)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
			e.ID, e.TenantID, e.DefinitionID, e.VersionID, e.WorkflowName, e.VersionNumber, e.Status, []byte(e.Input),
			nullJSON(e.Output), e.Error, nullString(e.IdempotencyKey), e.MaxParallelism, e.TraceParent, e.ScheduledAt,
			e.CreatedAt, e.UpdatedAt, e.StartedAt, e.CompletedAt); err != nil {
			return err
		}
		return persistChanges(ctx, tx, run, false)
	}))
}

// UpdateRun is the single write path for executions. Locking order is always
// execution row, then its task rows, which prevents deadlocks between
// concurrent updates. Workers claiming tasks use SKIP LOCKED and never wait
// on these locks.
func (s *Store) UpdateRun(ctx context.Context, tenantID, executionID string, fn func(*execution.Run, time.Time) error) (*execution.Run, error) {
	var run *execution.Run
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		q := `SELECT ` + executionColumns + ` FROM executions WHERE id = $1`
		args := []any{executionID}
		if tenantID != "" {
			q += ` AND tenant_id = $2`
			args = append(args, tenantID)
		}
		exec, err := scanExecution(tx.QueryRow(ctx, q+` FOR NO KEY UPDATE`, args...))
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT `+taskColumns+` FROM tasks WHERE execution_id = $1 ORDER BY kind, position FOR UPDATE`, executionID)
		if err != nil {
			return err
		}
		tasks, err := pgx.CollectRows(rows, scanTaskRow)
		if err != nil {
			return err
		}
		var now time.Time
		if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return err
		}
		run = execution.Restore(exec, tasks, s.newID)
		if err := fn(run, now); err != nil {
			return err
		}
		return persistChanges(ctx, tx, run, true)
	})
	return run, mapErr(err)
}

func persistChanges(ctx context.Context, tx pgx.Tx, run *execution.Run, existing bool) error {
	ch := run.Changes()
	b := &pgx.Batch{}
	if existing && ch.ExecutionChanged {
		e := run.Execution
		b.Queue(`UPDATE executions SET status = $2, output = $3, error = $4, updated_at = $5, started_at = $6, completed_at = $7
			WHERE id = $1`, e.ID, e.Status, nullJSON(e.Output), e.Error, e.UpdatedAt, e.StartedAt, e.CompletedAt)
	}
	for _, t := range ch.NewTasks {
		retryJSON, comp, err := encodeTaskPolicies(t)
		if err != nil {
			return err
		}
		b.Queue(`INSERT INTO tasks (`+taskColumns+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28)`,
			t.ID, t.ExecutionID, t.TenantID, t.StepKey, t.Name, t.Handler, t.Kind, t.Position, nonNil(t.DependsOn), t.Status,
			nullJSON(t.Params), nullJSON(t.Input), nullJSON(t.Output), t.Attempts, retryJSON, t.Timeout.Milliseconds(), comp,
			nullString(t.CompensatesTaskID), t.RunAt, t.LeaseOwner, t.LeaseToken, t.LeaseExpiresAt, t.LastError,
			string(t.LastErrorKind), t.CreatedAt, t.UpdatedAt, t.StartedAt, t.CompletedAt)
	}
	for _, t := range ch.ChangedTasks {
		b.Queue(`UPDATE tasks SET status = $2, input = $3, output = $4, run_at = $5, lease_owner = $6, lease_expires_at = $7,
			last_error = $8, last_error_kind = $9, updated_at = $10, completed_at = $11 WHERE id = $1`,
			t.ID, t.Status, nullJSON(t.Input), nullJSON(t.Output), t.RunAt, t.LeaseOwner, t.LeaseExpiresAt,
			t.LastError, string(t.LastErrorKind), t.UpdatedAt, t.CompletedAt)
	}
	for _, a := range ch.Attempts {
		b.Queue(`UPDATE task_attempts SET status = $3, error = $4, error_kind = $5, finished_at = $6
			WHERE task_id = $1 AND number = $2`, a.TaskID, a.Number, a.Status, a.Error, string(a.ErrorKind), a.FinishedAt)
	}
	for _, d := range ch.DeadLetters {
		b.Queue(`INSERT INTO dead_letters (id, tenant_id, execution_id, task_id, step_key, handler, kind, attempts, error, error_kind, input, created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
			d.ID, d.TenantID, d.ExecutionID, d.TaskID, d.StepKey, d.Handler, d.Kind, d.Attempts, d.Error, string(d.ErrorKind),
			nullJSON(d.Input), d.CreatedAt)
	}
	for _, e := range ch.Events {
		b.Queue(`INSERT INTO execution_events (execution_id, tenant_id, task_id, step_key, type, from_status, to_status, message, created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			e.ExecutionID, e.TenantID, nullString(e.TaskID), e.StepKey, e.Type, e.From, e.To, e.Message, e.CreatedAt)
	}
	if b.Len() == 0 {
		return nil
	}
	return tx.SendBatch(ctx, b).Close()
}

func (s *Store) GetExecution(ctx context.Context, tenantID, id string) (*execution.Execution, error) {
	e, err := scanExecution(s.pool.QueryRow(ctx, `SELECT `+executionColumns+` FROM executions WHERE id = $1 AND tenant_id = $2`, id, tenantID))
	return e, mapErr(err)
}

func (s *Store) GetExecutionByIdempotencyKey(ctx context.Context, tenantID, key string) (*execution.Execution, error) {
	e, err := scanExecution(s.pool.QueryRow(ctx, `SELECT `+executionColumns+` FROM executions WHERE tenant_id = $1 AND idempotency_key = $2`, tenantID, key))
	return e, mapErr(err)
}

func (s *Store) ListExecutions(ctx context.Context, tenantID string, f executions.Filter, page application.Page) ([]*execution.Execution, string, error) {
	c, err := decodeCursor(page.Cursor)
	if err != nil {
		return nil, "", err
	}
	q := `SELECT ` + executionColumns + ` FROM executions WHERE tenant_id = $1`
	args := []any{tenantID, page.Limit + 1}
	if f.Status != "" {
		args = append(args, f.Status)
		q += ` AND status = $` + strconv.Itoa(len(args))
	}
	if f.DefinitionID != "" {
		args = append(args, f.DefinitionID)
		q += ` AND definition_id = $` + strconv.Itoa(len(args))
	}
	cond, extra := pageArgs(c, len(args)+1)
	rows, err := s.pool.Query(ctx, q+cond+` ORDER BY created_at DESC, id DESC LIMIT $2`, append(args, extra...)...)
	if err != nil {
		return nil, "", mapErr(err)
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (*execution.Execution, error) { return scanExecution(r) })
	if err != nil {
		return nil, "", mapErr(err)
	}
	next := ""
	if len(list) > page.Limit {
		list = list[:page.Limit]
		last := list[len(list)-1]
		next = cursor{CreatedAt: last.CreatedAt, ID: last.ID}.encode()
	}
	return list, next, nil
}

func (s *Store) ListTasks(ctx context.Context, tenantID, executionID string) ([]*execution.Task, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+taskColumns+` FROM tasks WHERE execution_id = $1 AND tenant_id = $2 ORDER BY kind, position`, executionID, tenantID)
	if err != nil {
		return nil, mapErr(err)
	}
	tasks, err := pgx.CollectRows(rows, scanTaskRow)
	return tasks, mapErr(err)
}

func (s *Store) GetTask(ctx context.Context, tenantID, taskID string) (*execution.Task, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+taskColumns+` FROM tasks WHERE id = $1 AND tenant_id = $2`, taskID, tenantID)
	if err != nil {
		return nil, mapErr(err)
	}
	t, err := pgx.CollectExactlyOneRow(rows, scanTaskRow)
	return t, mapErr(err)
}

func (s *Store) ListAttempts(ctx context.Context, tenantID, taskID string) ([]execution.Attempt, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, task_id, execution_id, tenant_id, number, worker_id, lease_token, status, error, error_kind, started_at, finished_at
		FROM task_attempts WHERE task_id = $1 AND tenant_id = $2 ORDER BY number`, taskID, tenantID)
	if err != nil {
		return nil, mapErr(err)
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (execution.Attempt, error) {
		var a execution.Attempt
		var kind string
		err := r.Scan(&a.ID, &a.TaskID, &a.ExecutionID, &a.TenantID, &a.Number, &a.WorkerID, &a.LeaseToken, &a.Status, &a.Error, &kind, &a.StartedAt, &a.FinishedAt)
		a.ErrorKind = execution.ErrorKind(kind)
		return a, err
	})
	return list, mapErr(err)
}

func (s *Store) ListEvents(ctx context.Context, tenantID, executionID string, afterID int64, limit int) ([]execution.Event, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, execution_id, tenant_id, task_id, step_key, type, from_status, to_status, message, created_at
		FROM execution_events WHERE execution_id = $1 AND tenant_id = $2 AND id > $3 ORDER BY id LIMIT $4`, executionID, tenantID, afterID, limit)
	if err != nil {
		return nil, mapErr(err)
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (execution.Event, error) {
		var e execution.Event
		var taskID *string
		err := r.Scan(&e.ID, &e.ExecutionID, &e.TenantID, &taskID, &e.StepKey, &e.Type, &e.From, &e.To, &e.Message, &e.CreatedAt)
		e.TaskID = strPtr(taskID)
		return e, err
	})
	return list, mapErr(err)
}

func (s *Store) ListDeadLetters(ctx context.Context, tenantID string, page application.Page) ([]execution.DeadLetter, string, error) {
	c, err := decodeCursor(page.Cursor)
	if err != nil {
		return nil, "", err
	}
	cond, args := pageArgs(c, 3)
	rows, err := s.pool.Query(ctx, `SELECT id, tenant_id, execution_id, task_id, step_key, handler, kind, attempts, error, error_kind, input, created_at
		FROM dead_letters WHERE tenant_id = $1`+cond+` ORDER BY created_at DESC, id DESC LIMIT $2`, append([]any{tenantID, page.Limit + 1}, args...)...)
	if err != nil {
		return nil, "", mapErr(err)
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (execution.DeadLetter, error) {
		var d execution.DeadLetter
		var kind string
		var input []byte
		err := r.Scan(&d.ID, &d.TenantID, &d.ExecutionID, &d.TaskID, &d.StepKey, &d.Handler, &d.Kind, &d.Attempts, &d.Error, &kind, &input, &d.CreatedAt)
		d.ErrorKind = execution.ErrorKind(kind)
		d.Input = input
		return d, err
	})
	if err != nil {
		return nil, "", mapErr(err)
	}
	next := ""
	if len(list) > page.Limit {
		list = list[:page.Limit]
		last := list[len(list)-1]
		next = cursor{CreatedAt: last.CreatedAt, ID: last.ID}.encode()
	}
	return list, next, nil
}

func scanExecution(row pgx.Row) (*execution.Execution, error) {
	var (
		e             execution.Execution
		input, output []byte
		idem          *string
	)
	err := row.Scan(&e.ID, &e.TenantID, &e.DefinitionID, &e.VersionID, &e.WorkflowName, &e.VersionNumber, &e.Status,
		&input, &output, &e.Error, &idem, &e.MaxParallelism, &e.TraceParent, &e.ScheduledAt, &e.CreatedAt, &e.UpdatedAt,
		&e.StartedAt, &e.CompletedAt)
	if err != nil {
		return nil, err
	}
	e.Input, e.Output, e.IdempotencyKey = input, output, strPtr(idem)
	return &e, nil
}

func scanTaskRow(r pgx.CollectableRow) (*execution.Task, error) {
	var (
		t                             execution.Task
		params, input, output, policy []byte
		comp                          []byte
		compensates                   *string
		timeoutMS                     int64
		errKind                       string
	)
	err := r.Scan(&t.ID, &t.ExecutionID, &t.TenantID, &t.StepKey, &t.Name, &t.Handler, &t.Kind, &t.Position, &t.DependsOn,
		&t.Status, &params, &input, &output, &t.Attempts, &policy, &timeoutMS, &comp, &compensates, &t.RunAt, &t.LeaseOwner,
		&t.LeaseToken, &t.LeaseExpiresAt, &t.LastError, &errKind, &t.CreatedAt, &t.UpdatedAt, &t.StartedAt, &t.CompletedAt)
	if err != nil {
		return nil, err
	}
	t.Params, t.Input, t.Output = params, input, output
	t.Timeout = time.Duration(timeoutMS) * time.Millisecond
	t.CompensatesTaskID = strPtr(compensates)
	t.LastErrorKind = execution.ErrorKind(errKind)
	if err := json.Unmarshal(policy, &t.Retry); err != nil {
		return nil, fmt.Errorf("decode retry policy of task %s: %w", t.ID, err)
	}
	if len(comp) > 0 {
		var c compensationJSON
		if err := json.Unmarshal(comp, &c); err != nil {
			return nil, fmt.Errorf("decode compensation of task %s: %w", t.ID, err)
		}
		t.Compensation = &execution.CompensationSpec{Handler: c.Handler, Timeout: time.Duration(c.TimeoutMS) * time.Millisecond, Retry: c.Retry}
	}
	return &t, nil
}

type compensationJSON struct {
	Handler   string       `json:"handler"`
	TimeoutMS int64        `json:"timeout_ms"`
	Retry     retry.Policy `json:"retry"`
}

func encodeTaskPolicies(t *execution.Task) ([]byte, any, error) {
	policy, err := json.Marshal(t.Retry)
	if err != nil {
		return nil, nil, err
	}
	if t.Compensation == nil {
		return policy, nil, nil
	}
	comp, err := json.Marshal(compensationJSON{Handler: t.Compensation.Handler, TimeoutMS: t.Compensation.Timeout.Milliseconds(), Retry: t.Compensation.Retry})
	return policy, comp, err
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
