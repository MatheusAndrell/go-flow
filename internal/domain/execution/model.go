package execution

import (
	"encoding/json"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/domain/retry"
)

type Execution struct {
	ID             string
	TenantID       string
	DefinitionID   string
	VersionID      string
	WorkflowName   string
	VersionNumber  int
	Status         Status
	Input          json.RawMessage
	Output         json.RawMessage
	Error          string
	IdempotencyKey string
	MaxParallelism int
	// TraceParent is the W3C trace context of the request that started the
	// execution, so worker spans join the same distributed trace.
	TraceParent string
	ScheduledAt time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
}

type TaskKind string

const (
	KindAction       TaskKind = "ACTION"
	KindCompensation TaskKind = "COMPENSATION"
)

// CompensationSpec is copied onto action tasks at creation, so compensation
// uses the definition version the execution started with.
type CompensationSpec struct {
	Handler string        `json:"handler"`
	Timeout time.Duration `json:"timeout"`
	Retry   retry.Policy  `json:"retry"`
}

type Task struct {
	ID                string
	ExecutionID       string
	TenantID          string
	StepKey           string
	Name              string
	Handler           string
	Kind              TaskKind
	DependsOn         []string
	Status            TaskStatus
	Params            json.RawMessage
	Input             json.RawMessage
	Output            json.RawMessage
	Attempts          int
	Retry             retry.Policy
	Timeout           time.Duration
	Compensation      *CompensationSpec
	CompensatesTaskID string
	// Position orders actions topologically and compensations by execution order.
	Position       int
	RunAt          *time.Time
	LeaseOwner     string
	LeaseToken     int64
	LeaseExpiresAt *time.Time
	LastError      string
	LastErrorKind  ErrorKind
	CreatedAt      time.Time
	UpdatedAt      time.Time
	StartedAt      *time.Time
	CompletedAt    *time.Time
}

// TaskInput is the envelope handed to handlers. Dependencies carries the
// outputs of the steps this step depends on.
type TaskInput struct {
	WorkflowInput     json.RawMessage            `json:"workflow_input"`
	Params            json.RawMessage            `json:"params,omitempty"`
	Dependencies      map[string]json.RawMessage `json:"dependencies,omitempty"`
	CompensatedOutput json.RawMessage            `json:"compensated_output,omitempty"`
}

type AttemptStatus string

const (
	AttemptRunning   AttemptStatus = "RUNNING"
	AttemptSucceeded AttemptStatus = "SUCCEEDED"
	AttemptFailed    AttemptStatus = "FAILED"
	AttemptCancelled AttemptStatus = "CANCELLED"
)

type Attempt struct {
	ID          string
	TaskID      string
	ExecutionID string
	TenantID    string
	Number      int
	WorkerID    string
	LeaseToken  int64
	Status      AttemptStatus
	Error       string
	ErrorKind   ErrorKind
	StartedAt   time.Time
	FinishedAt  *time.Time
}

// AttemptResult finishes the attempt row created when the task was claimed.
type AttemptResult struct {
	TaskID     string
	Number     int
	Status     AttemptStatus
	Error      string
	ErrorKind  ErrorKind
	FinishedAt time.Time
}

type EventType string

const (
	EventExecutionCreated       EventType = "execution.created"
	EventExecutionStatusChanged EventType = "execution.status_changed"
	EventTaskStatusChanged      EventType = "task.status_changed"
	EventTaskStarted            EventType = "task.started"
	EventTaskDeadLettered       EventType = "task.dead_lettered"
)

// Event is an immutable history record. Every status change of the execution
// or of one of its tasks produces exactly one event.
type Event struct {
	ID          int64
	ExecutionID string
	TenantID    string
	TaskID      string
	StepKey     string
	Type        EventType
	From        string
	To          string
	Message     string
	CreatedAt   time.Time
}

// DeadLetter records a task that exhausted its retries or failed permanently.
type DeadLetter struct {
	ID          string
	TenantID    string
	ExecutionID string
	TaskID      string
	StepKey     string
	Handler     string
	Kind        TaskKind
	Attempts    int
	Error       string
	ErrorKind   ErrorKind
	Input       json.RawMessage
	CreatedAt   time.Time
}

// ErrorKind classifies failures so the engine can decide whether to retry.
type ErrorKind string

const (
	ErrorRetryable    ErrorKind = "RETRYABLE"
	ErrorPermanent    ErrorKind = "PERMANENT"
	ErrorTimeout      ErrorKind = "TIMEOUT"
	ErrorPanic        ErrorKind = "PANIC"
	ErrorLeaseExpired ErrorKind = "LEASE_EXPIRED"
	ErrorShutdown     ErrorKind = "WORKER_SHUTDOWN"
)

// Retryable reports whether a failure of this kind may succeed on a later attempt.
// Panics are treated as programming errors and are not retried.
func (k ErrorKind) Retryable() bool {
	switch k {
	case ErrorRetryable, ErrorTimeout, ErrorLeaseExpired, ErrorShutdown:
		return true
	}
	return false
}

type Failure struct {
	Kind    ErrorKind
	Message string
}

const maxErrorLength = 4000

func (f Failure) truncated() string {
	if len(f.Message) > maxErrorLength {
		return f.Message[:maxErrorLength]
	}
	return f.Message
}
