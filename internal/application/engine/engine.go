// Package engine turns task outcomes reported by workers into workflow
// progress, and recovers work abandoned by crashed processes. Every decision
// is taken inside UpdateRun, which serializes all changes to one execution.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/domain/execution"
)

// Lease identifies one claim of a task by a worker. Token is a fencing token
// incremented on every claim; stale tokens are rejected.
type Lease struct {
	TaskID      string
	ExecutionID string
	TenantID    string
	Token       int64
	Handler     string
	Kind        execution.TaskKind
}

// ClaimedTask is what the task queue hands to a worker.
type ClaimedTask struct {
	Lease
	StepKey     string
	Attempt     int
	Input       json.RawMessage
	Timeout     time.Duration
	RunAt       time.Time
	ClaimedAt   time.Time
	TraceParent string
}

// TaskRef points at a task whose lease expired.
type TaskRef struct {
	TaskID      string
	ExecutionID string
}

type Store interface {
	UpdateRun(ctx context.Context, tenantID, executionID string, fn func(*execution.Run, time.Time) error) (*execution.Run, error)
	ExpiredLeases(ctx context.Context, limit int) ([]TaskRef, error)
	DueExecutions(ctx context.Context, limit int) ([]string, error)
}

// Recorder receives business metrics. Implementations must be cheap and non-blocking.
type Recorder interface {
	TaskFinished(handler string, kind execution.TaskKind, outcome string)
	TaskRetried(handler string)
	TaskDeadLettered(handler string)
	LeaseExpired()
	ExecutionFinished(workflow string, status execution.Status, duration time.Duration)
}

const (
	OutcomeSucceeded = "succeeded"
	OutcomeFailed    = "failed"
	OutcomeRetried   = "retried"
)

type Engine struct {
	store    Store
	recorder Recorder
	log      *slog.Logger
	rnd      func() float64
}

func New(store Store, recorder Recorder, log *slog.Logger) *Engine {
	return &Engine{store: store, recorder: recorder, log: log, rnd: rand.Float64}
}

// TaskStarted records the start of an attempt. It returns
// execution.ErrLeaseLost when the task was cancelled or reclaimed meanwhile.
func (e *Engine) TaskStarted(ctx context.Context, l Lease) error {
	_, err := e.store.UpdateRun(ctx, "", l.ExecutionID, func(r *execution.Run, now time.Time) error {
		return r.RecordStarted(l.TaskID, l.Token, now)
	})
	return err
}

func (e *Engine) CompleteTask(ctx context.Context, l Lease, output json.RawMessage) error {
	run, err := e.store.UpdateRun(ctx, "", l.ExecutionID, func(r *execution.Run, now time.Time) error {
		return r.RecordSuccess(l.TaskID, l.Token, output, now)
	})
	if err != nil {
		return err
	}
	e.recorder.TaskFinished(l.Handler, l.Kind, OutcomeSucceeded)
	e.observeRun(run)
	return nil
}

func (e *Engine) FailTask(ctx context.Context, l Lease, f execution.Failure) (execution.Outcome, error) {
	var out execution.Outcome
	run, err := e.store.UpdateRun(ctx, "", l.ExecutionID, func(r *execution.Run, now time.Time) error {
		var err error
		out, err = r.RecordFailure(l.TaskID, l.Token, f, now, e.rnd)
		return err
	})
	if err != nil {
		return out, err
	}
	e.recordFailure(l.Handler, l.Kind, out)
	e.observeRun(run)
	return out, nil
}

// RecoverAbandoned fails (and usually retries) tasks whose worker stopped
// heartbeating. Safe to run concurrently from many processes.
func (e *Engine) RecoverAbandoned(ctx context.Context, limit int) (int, error) {
	refs, err := e.store.ExpiredLeases(ctx, limit)
	if err != nil {
		return 0, err
	}
	recovered := 0
	for _, ref := range refs {
		var (
			out     execution.Outcome
			handler string
			kind    execution.TaskKind
		)
		run, err := e.store.UpdateRun(ctx, "", ref.ExecutionID, func(r *execution.Run, now time.Time) error {
			var err error
			out, err = r.RecordAbandoned(ref.TaskID, now, e.rnd)
			for _, t := range r.Tasks {
				if t.ID == ref.TaskID {
					handler, kind = t.Handler, t.Kind
				}
			}
			return err
		})
		switch {
		case errors.Is(err, execution.ErrLeaseLost), errors.Is(err, execution.ErrTaskNotFound):
			continue // another process recovered it, or the worker reported just in time
		case err != nil:
			return recovered, err
		}
		recovered++
		e.recorder.LeaseExpired()
		e.recordFailure(handler, kind, out)
		e.observeRun(run)
		e.log.WarnContext(ctx, "recovered abandoned task",
			"task_id", ref.TaskID, "execution_id", ref.ExecutionID, "retried", out.Retried)
	}
	return recovered, nil
}

// ActivateDue starts scheduled executions whose time has come.
func (e *Engine) ActivateDue(ctx context.Context, limit int) (int, error) {
	ids, err := e.store.DueExecutions(ctx, limit)
	if err != nil {
		return 0, err
	}
	activated := 0
	for _, id := range ids {
		_, err := e.store.UpdateRun(ctx, "", id, func(r *execution.Run, now time.Time) error {
			return r.Activate(now)
		})
		switch {
		case errors.Is(err, execution.ErrInvalidTransition), errors.Is(err, execution.ErrNotDue):
			continue // cancelled or activated by another process meanwhile
		case err != nil:
			return activated, err
		}
		activated++
	}
	return activated, nil
}

func (e *Engine) recordFailure(handler string, kind execution.TaskKind, out execution.Outcome) {
	if out.Retried {
		e.recorder.TaskRetried(handler)
		e.recorder.TaskFinished(handler, kind, OutcomeRetried)
		return
	}
	e.recorder.TaskFinished(handler, kind, OutcomeFailed)
	if out.DeadLettered {
		e.recorder.TaskDeadLettered(handler)
	}
}

func (e *Engine) observeRun(run *execution.Run) {
	ex := run.Execution
	if !ex.Status.Terminal() || !run.Changes().ExecutionChanged || ex.CompletedAt == nil {
		return
	}
	start := ex.CreatedAt
	if ex.StartedAt != nil {
		start = *ex.StartedAt
	}
	e.recorder.ExecutionFinished(ex.WorkflowName, ex.Status, ex.CompletedAt.Sub(start))
}
