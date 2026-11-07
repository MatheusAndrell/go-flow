// Package executions implements the Workflow Execution and Task Execution
// query and command use cases exposed to API clients.
package executions

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/application"
	"github.com/MatheusAndrell/go-flow/internal/domain/execution"
	"github.com/MatheusAndrell/go-flow/internal/domain/identity"
	"github.com/MatheusAndrell/go-flow/internal/domain/workflow"
)

const (
	MaxInputBytes       = 256 << 10
	MaxScheduleHorizon  = 366 * 24 * time.Hour
	maxIdempotencyKey   = 255
	defaultHistoryLimit = 100
	maxHistoryLimit     = 1000
)

type Filter struct {
	Status       execution.Status
	DefinitionID string
}

type Store interface {
	CreateRun(ctx context.Context, run *execution.Run) error
	// UpdateRun locks the execution and its tasks, applies fn with the database
	// clock and persists all recorded changes in one transaction.
	UpdateRun(ctx context.Context, tenantID, executionID string, fn func(*execution.Run, time.Time) error) (*execution.Run, error)
	GetExecution(ctx context.Context, tenantID, id string) (*execution.Execution, error)
	GetExecutionByIdempotencyKey(ctx context.Context, tenantID, key string) (*execution.Execution, error)
	ListExecutions(ctx context.Context, tenantID string, f Filter, page application.Page) ([]*execution.Execution, string, error)
	ListTasks(ctx context.Context, tenantID, executionID string) ([]*execution.Task, error)
	GetTask(ctx context.Context, tenantID, taskID string) (*execution.Task, error)
	ListAttempts(ctx context.Context, tenantID, taskID string) ([]execution.Attempt, error)
	ListEvents(ctx context.Context, tenantID, executionID string, afterID int64, limit int) ([]execution.Event, error)
	ListDeadLetters(ctx context.Context, tenantID string, page application.Page) ([]execution.DeadLetter, string, error)
}

type WorkflowReader interface {
	GetWorkflow(ctx context.Context, tenantID, id string) (*workflow.Workflow, error)
}

// Observer is notified about executions created by clients.
type Observer interface {
	ExecutionStarted(workflow string)
	ExecutionFinished(workflow string, status execution.Status, duration time.Duration)
}

type Service struct {
	store     Store
	workflows WorkflowReader
	observer  Observer
	newID     func() string
	now       func() time.Time
}

func NewService(store Store, workflows WorkflowReader, observer Observer, newID func() string, now func() time.Time) *Service {
	return &Service{store: store, workflows: workflows, observer: observer, newID: newID, now: now}
}

type StartInput struct {
	WorkflowID     string
	Version        *int
	Input          json.RawMessage
	ScheduledAt    *time.Time
	IdempotencyKey string
	TraceParent    string
}

// Start creates an execution bound to a published version. Starting twice with
// the same idempotency key returns the original execution and created=false.
func (s *Service) Start(ctx context.Context, p identity.Principal, in StartInput) (exec *execution.Execution, created bool, err error) {
	if err := p.Require(identity.PermExecutionsWrite); err != nil {
		return nil, false, err
	}
	if trimmed := bytes.TrimSpace(in.Input); len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		in.Input = nil
	}
	if err := validateStart(in, s.now()); err != nil {
		return nil, false, err
	}
	if in.IdempotencyKey != "" {
		existing, err := s.store.GetExecutionByIdempotencyKey(ctx, p.TenantID, in.IdempotencyKey)
		if err == nil {
			return existing, false, nil
		}
		if !errors.Is(err, application.ErrNotFound) {
			return nil, false, err
		}
	}
	wf, err := s.workflows.GetWorkflow(ctx, p.TenantID, in.WorkflowID)
	if err != nil {
		return nil, false, err
	}
	var version *workflow.Version
	if in.Version != nil {
		version, err = wf.PublishedVersion(*in.Version)
	} else {
		version, err = wf.LatestPublished()
	}
	if err != nil {
		return nil, false, application.Invalid("%s", err.Error())
	}
	run, err := execution.Start(execution.StartParams{
		ExecutionID: s.newID(), TenantID: p.TenantID, WorkflowName: wf.Definition.Name, Version: version,
		Input: in.Input, IdempotencyKey: in.IdempotencyKey, ScheduledAt: in.ScheduledAt, TraceParent: in.TraceParent,
	}, s.newID, s.now())
	if err != nil {
		return nil, false, err
	}
	if err := s.store.CreateRun(ctx, run); err != nil {
		if errors.Is(err, application.ErrConflict) && in.IdempotencyKey != "" {
			// Lost a race with a concurrent request using the same key.
			existing, getErr := s.store.GetExecutionByIdempotencyKey(ctx, p.TenantID, in.IdempotencyKey)
			if getErr == nil {
				return existing, false, nil
			}
		}
		return nil, false, err
	}
	s.observer.ExecutionStarted(wf.Definition.Name)
	return run.Execution, true, nil
}

func validateStart(in StartInput, now time.Time) error {
	if in.WorkflowID == "" {
		return application.Invalid("workflow_id is required")
	}
	if len(in.Input) > MaxInputBytes {
		return application.Invalid("input exceeds %d bytes", MaxInputBytes)
	}
	if len(in.Input) > 0 {
		trimmed := bytes.TrimSpace(in.Input)
		if len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(trimmed) {
			return application.Invalid("input must be a JSON object")
		}
	}
	if in.ScheduledAt != nil && in.ScheduledAt.After(now.Add(MaxScheduleHorizon)) {
		return application.Invalid("scheduled_at must be within %s", MaxScheduleHorizon)
	}
	if len(in.IdempotencyKey) > maxIdempotencyKey {
		return application.Invalid("idempotency key must not exceed %d characters", maxIdempotencyKey)
	}
	return nil
}

func (s *Service) Get(ctx context.Context, p identity.Principal, id string) (*execution.Execution, error) {
	if err := p.Require(identity.PermExecutionsRead); err != nil {
		return nil, err
	}
	return s.store.GetExecution(ctx, p.TenantID, id)
}

func (s *Service) List(ctx context.Context, p identity.Principal, f Filter, page application.Page) ([]*execution.Execution, string, error) {
	if err := p.Require(identity.PermExecutionsRead); err != nil {
		return nil, "", err
	}
	if f.Status != "" && !f.Status.Valid() {
		return nil, "", application.Invalid("unknown status %q", f.Status)
	}
	page, err := page.Normalize()
	if err != nil {
		return nil, "", err
	}
	return s.store.ListExecutions(ctx, p.TenantID, f, page)
}

func (s *Service) Tasks(ctx context.Context, p identity.Principal, executionID string) ([]*execution.Task, error) {
	if err := p.Require(identity.PermExecutionsRead); err != nil {
		return nil, err
	}
	if _, err := s.store.GetExecution(ctx, p.TenantID, executionID); err != nil {
		return nil, err
	}
	return s.store.ListTasks(ctx, p.TenantID, executionID)
}

func (s *Service) Task(ctx context.Context, p identity.Principal, taskID string) (*execution.Task, error) {
	if err := p.Require(identity.PermExecutionsRead); err != nil {
		return nil, err
	}
	return s.store.GetTask(ctx, p.TenantID, taskID)
}

func (s *Service) Attempts(ctx context.Context, p identity.Principal, taskID string) ([]execution.Attempt, error) {
	if err := p.Require(identity.PermExecutionsRead); err != nil {
		return nil, err
	}
	if _, err := s.store.GetTask(ctx, p.TenantID, taskID); err != nil {
		return nil, err
	}
	return s.store.ListAttempts(ctx, p.TenantID, taskID)
}

// History returns events after the given event id, oldest first.
func (s *Service) History(ctx context.Context, p identity.Principal, executionID string, afterID int64, limit int) ([]execution.Event, error) {
	if err := p.Require(identity.PermExecutionsRead); err != nil {
		return nil, err
	}
	switch {
	case limit == 0:
		limit = defaultHistoryLimit
	case limit < 0 || limit > maxHistoryLimit:
		return nil, application.Invalid("limit must be between 1 and %d", maxHistoryLimit)
	}
	if _, err := s.store.GetExecution(ctx, p.TenantID, executionID); err != nil {
		return nil, err
	}
	return s.store.ListEvents(ctx, p.TenantID, executionID, afterID, limit)
}

func (s *Service) DeadLetters(ctx context.Context, p identity.Principal, page application.Page) ([]execution.DeadLetter, string, error) {
	if err := p.Require(identity.PermExecutionsRead); err != nil {
		return nil, "", err
	}
	page, err := page.Normalize()
	if err != nil {
		return nil, "", err
	}
	return s.store.ListDeadLetters(ctx, p.TenantID, page)
}

func (s *Service) Cancel(ctx context.Context, p identity.Principal, id, reason string, compensate bool) (*execution.Execution, error) {
	if len(reason) > 500 {
		return nil, application.Invalid("reason must not exceed 500 characters")
	}
	return s.command(ctx, p, id, func(r *execution.Run, now time.Time) error {
		return r.Cancel(reason, compensate, now)
	})
}

func (s *Service) Pause(ctx context.Context, p identity.Principal, id string) (*execution.Execution, error) {
	return s.command(ctx, p, id, func(r *execution.Run, now time.Time) error { return r.Pause(now) })
}

func (s *Service) Resume(ctx context.Context, p identity.Principal, id string) (*execution.Execution, error) {
	return s.command(ctx, p, id, func(r *execution.Run, now time.Time) error { return r.Resume(now) })
}

func (s *Service) command(ctx context.Context, p identity.Principal, id string, fn func(*execution.Run, time.Time) error) (*execution.Execution, error) {
	if err := p.Require(identity.PermExecutionsWrite); err != nil {
		return nil, err
	}
	run, err := s.store.UpdateRun(ctx, p.TenantID, id, fn)
	if err != nil {
		return nil, err
	}
	if ex := run.Execution; ex.Status.Terminal() && ex.CompletedAt != nil {
		start := ex.CreatedAt
		if ex.StartedAt != nil {
			start = *ex.StartedAt
		}
		s.observer.ExecutionFinished(ex.WorkflowName, ex.Status, ex.CompletedAt.Sub(start))
	}
	return run.Execution, nil
}
