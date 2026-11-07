package httpapi

import (
	"encoding/json"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/domain/execution"
	"github.com/MatheusAndrell/go-flow/internal/domain/retry"
	"github.com/MatheusAndrell/go-flow/internal/domain/workflow"
)

type listResponse[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type workflowRequest struct {
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Spec        workflow.Spec `json:"spec"`
}

type updateWorkflowRequest struct {
	Description *string        `json:"description"`
	Spec        *workflow.Spec `json:"spec"`
}

type versionRequest struct {
	Spec *workflow.Spec `json:"spec"`
}

type versionResponse struct {
	ID          string        `json:"id"`
	Number      int           `json:"number"`
	Status      string        `json:"status"`
	Spec        workflow.Spec `json:"spec"`
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
	PublishedAt *time.Time    `json:"published_at,omitempty"`
}

type workflowResponse struct {
	ID                     string            `json:"id"`
	Name                   string            `json:"name"`
	Description            string            `json:"description"`
	LatestVersion          int               `json:"latest_version"`
	LatestPublishedVersion *int              `json:"latest_published_version,omitempty"`
	Versions               []versionResponse `json:"versions,omitempty"`
	CreatedAt              time.Time         `json:"created_at"`
	UpdatedAt              time.Time         `json:"updated_at"`
}

type definitionSummary struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func toVersion(v *workflow.Version) versionResponse {
	return versionResponse{ID: v.ID, Number: v.Number, Status: string(v.Status), Spec: v.Spec,
		CreatedAt: v.CreatedAt, UpdatedAt: v.UpdatedAt, PublishedAt: v.PublishedAt}
}

func toWorkflow(w *workflow.Workflow) workflowResponse {
	d := w.Definition
	out := workflowResponse{ID: d.ID, Name: d.Name, Description: d.Description, CreatedAt: d.CreatedAt, UpdatedAt: d.UpdatedAt}
	if latest := w.Latest(); latest != nil {
		out.LatestVersion = latest.Number
	}
	if p, err := w.LatestPublished(); err == nil {
		n := p.Number
		out.LatestPublishedVersion = &n
	}
	for _, v := range w.Versions {
		out.Versions = append(out.Versions, toVersion(v))
	}
	return out
}

type startRequest struct {
	WorkflowID  string          `json:"workflow_id"`
	Version     *int            `json:"version"`
	Input       json.RawMessage `json:"input"`
	ScheduledAt *time.Time      `json:"scheduled_at"`
}

type cancelRequest struct {
	Reason     string `json:"reason"`
	Compensate bool   `json:"compensate"`
}

type executionResponse struct {
	ID           string          `json:"id"`
	WorkflowID   string          `json:"workflow_id"`
	WorkflowName string          `json:"workflow_name"`
	Version      int             `json:"version"`
	Status       string          `json:"status"`
	Input        json.RawMessage `json:"input"`
	Output       json.RawMessage `json:"output,omitempty"`
	Error        string          `json:"error,omitempty"`
	ScheduledAt  time.Time       `json:"scheduled_at"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
	StartedAt    *time.Time      `json:"started_at,omitempty"`
	CompletedAt  *time.Time      `json:"completed_at,omitempty"`
}

func toExecution(e *execution.Execution) executionResponse {
	return executionResponse{
		ID: e.ID, WorkflowID: e.DefinitionID, WorkflowName: e.WorkflowName, Version: e.VersionNumber,
		Status: string(e.Status), Input: e.Input, Output: e.Output, Error: e.Error, ScheduledAt: e.ScheduledAt,
		CreatedAt: e.CreatedAt, UpdatedAt: e.UpdatedAt, StartedAt: e.StartedAt, CompletedAt: e.CompletedAt,
	}
}

type compensationResponse struct {
	Handler string       `json:"handler"`
	Timeout string       `json:"timeout"`
	Retry   retry.Policy `json:"retry"`
}

type taskResponse struct {
	ID                string                `json:"id"`
	ExecutionID       string                `json:"execution_id"`
	StepKey           string                `json:"step_key"`
	Name              string                `json:"name"`
	Handler           string                `json:"handler"`
	Kind              string                `json:"kind"`
	Status            string                `json:"status"`
	DependsOn         []string              `json:"depends_on"`
	Input             json.RawMessage       `json:"input,omitempty"`
	Output            json.RawMessage       `json:"output,omitempty"`
	Attempts          int                   `json:"attempts"`
	Retry             retry.Policy          `json:"retry"`
	Timeout           string                `json:"timeout"`
	Compensation      *compensationResponse `json:"compensation,omitempty"`
	CompensatesTaskID string                `json:"compensates_task_id,omitempty"`
	NextRunAt         *time.Time            `json:"next_run_at,omitempty"`
	LastError         string                `json:"last_error,omitempty"`
	LastErrorKind     string                `json:"last_error_kind,omitempty"`
	CreatedAt         time.Time             `json:"created_at"`
	UpdatedAt         time.Time             `json:"updated_at"`
	StartedAt         *time.Time            `json:"started_at,omitempty"`
	CompletedAt       *time.Time            `json:"completed_at,omitempty"`
}

func toTask(t *execution.Task) taskResponse {
	out := taskResponse{
		ID: t.ID, ExecutionID: t.ExecutionID, StepKey: t.StepKey, Name: t.Name, Handler: t.Handler, Kind: string(t.Kind),
		Status: string(t.Status), DependsOn: nonNilStrings(t.DependsOn), Input: t.Input, Output: t.Output, Attempts: t.Attempts,
		Retry: t.Retry, Timeout: t.Timeout.String(), CompensatesTaskID: t.CompensatesTaskID, LastError: t.LastError,
		LastErrorKind: string(t.LastErrorKind), CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt, StartedAt: t.StartedAt,
		CompletedAt: t.CompletedAt,
	}
	if t.Status == execution.TaskReady || t.Status == execution.TaskRetryScheduled {
		out.NextRunAt = t.RunAt
	}
	if c := t.Compensation; c != nil {
		out.Compensation = &compensationResponse{Handler: c.Handler, Timeout: c.Timeout.String(), Retry: c.Retry}
	}
	return out
}

type attemptResponse struct {
	Number     int        `json:"number"`
	WorkerID   string     `json:"worker_id"`
	Status     string     `json:"status"`
	Error      string     `json:"error,omitempty"`
	ErrorKind  string     `json:"error_kind,omitempty"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	DurationMS *int64     `json:"duration_ms,omitempty"`
}

func toAttempt(a execution.Attempt) attemptResponse {
	out := attemptResponse{Number: a.Number, WorkerID: a.WorkerID, Status: string(a.Status), Error: a.Error,
		ErrorKind: string(a.ErrorKind), StartedAt: a.StartedAt, FinishedAt: a.FinishedAt}
	if a.FinishedAt != nil {
		ms := a.FinishedAt.Sub(a.StartedAt).Milliseconds()
		out.DurationMS = &ms
	}
	return out
}

type eventResponse struct {
	ID        int64     `json:"id"`
	Type      string    `json:"type"`
	TaskID    string    `json:"task_id,omitempty"`
	StepKey   string    `json:"step_key,omitempty"`
	From      string    `json:"from,omitempty"`
	To        string    `json:"to,omitempty"`
	Message   string    `json:"message,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

func toEvent(e execution.Event) eventResponse {
	return eventResponse{ID: e.ID, Type: string(e.Type), TaskID: e.TaskID, StepKey: e.StepKey, From: e.From, To: e.To,
		Message: e.Message, CreatedAt: e.CreatedAt}
}

type deadLetterResponse struct {
	ID          string    `json:"id"`
	ExecutionID string    `json:"execution_id"`
	TaskID      string    `json:"task_id"`
	StepKey     string    `json:"step_key"`
	Handler     string    `json:"handler"`
	Kind        string    `json:"kind"`
	Attempts    int       `json:"attempts"`
	Error       string    `json:"error"`
	ErrorKind   string    `json:"error_kind"`
	CreatedAt   time.Time `json:"created_at"`
}

func toDeadLetter(d execution.DeadLetter) deadLetterResponse {
	return deadLetterResponse{ID: d.ID, ExecutionID: d.ExecutionID, TaskID: d.TaskID, StepKey: d.StepKey, Handler: d.Handler,
		Kind: string(d.Kind), Attempts: d.Attempts, Error: d.Error, ErrorKind: string(d.ErrorKind), CreatedAt: d.CreatedAt}
}

func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func mapSlice[T, U any](in []T, f func(T) U) []U {
	out := make([]U, 0, len(in))
	for _, v := range in {
		out = append(out, f(v))
	}
	return out
}
