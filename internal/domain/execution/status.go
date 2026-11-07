// Package execution holds the Workflow Execution, Task Execution and
// Compensation bounded contexts. An execution and its tasks form a single
// aggregate (Run) because every decision about what runs next must see a
// consistent view of all of them.
package execution

import (
	"errors"
	"fmt"
	"slices"
)

// ErrInvalidTransition is returned for any transition outside the state machines below.
var ErrInvalidTransition = errors.New("invalid state transition")

type Status string

const (
	StatusPending      Status = "PENDING"
	StatusRunning      Status = "RUNNING"
	StatusWaiting      Status = "WAITING"
	StatusPaused       Status = "PAUSED"
	StatusCompleted    Status = "COMPLETED"
	StatusFailed       Status = "FAILED"
	StatusCancelled    Status = "CANCELLED"
	StatusCompensating Status = "COMPENSATING"
	StatusCompensated  Status = "COMPENSATED"
)

// executionTransitions is the formal execution state machine.
//
//	PENDING      -> RUNNING | CANCELLED
//	RUNNING      -> WAITING | PAUSED | COMPLETED | FAILED | CANCELLED | COMPENSATING
//	WAITING      -> RUNNING | PAUSED | FAILED | CANCELLED | COMPENSATING
//	PAUSED       -> RUNNING | CANCELLED | COMPENSATING
//	COMPENSATING -> COMPENSATED | FAILED
//
// WAITING means nothing is runnable now and the remaining work is scheduled
// for later (retry backoff). COMPLETED, FAILED, CANCELLED and COMPENSATED are terminal.
var executionTransitions = map[Status][]Status{
	StatusPending:      {StatusRunning, StatusCancelled},
	StatusRunning:      {StatusWaiting, StatusPaused, StatusCompleted, StatusFailed, StatusCancelled, StatusCompensating},
	StatusWaiting:      {StatusRunning, StatusPaused, StatusFailed, StatusCancelled, StatusCompensating},
	StatusPaused:       {StatusRunning, StatusCancelled, StatusCompensating},
	StatusCompensating: {StatusCompensated, StatusFailed},
}

var AllStatuses = []Status{
	StatusPending, StatusRunning, StatusWaiting, StatusPaused, StatusCompleted,
	StatusFailed, StatusCancelled, StatusCompensating, StatusCompensated,
}

func (s Status) Valid() bool { return slices.Contains(AllStatuses, s) }

func (s Status) CanTransitionTo(to Status) bool {
	return slices.Contains(executionTransitions[s], to)
}

func (s Status) Terminal() bool {
	return s.Valid() && len(executionTransitions[s]) == 0
}

// Active reports whether workers may claim tasks of an execution in this status.
func (s Status) Active() bool {
	return s == StatusRunning || s == StatusWaiting || s == StatusCompensating
}

type TaskStatus string

const (
	TaskPending        TaskStatus = "PENDING"
	TaskReady          TaskStatus = "READY"
	TaskRunning        TaskStatus = "RUNNING"
	TaskRetryScheduled TaskStatus = "RETRY_SCHEDULED"
	TaskCompleted      TaskStatus = "COMPLETED"
	TaskFailed         TaskStatus = "FAILED"
	TaskCancelled      TaskStatus = "CANCELLED"
	TaskSkipped        TaskStatus = "SKIPPED"
	TaskCompensated    TaskStatus = "COMPENSATED"
)

// taskTransitions is the formal task state machine.
//
//	PENDING         -> READY | SKIPPED | CANCELLED
//	READY           -> RUNNING | SKIPPED | CANCELLED
//	RUNNING         -> COMPLETED | FAILED | RETRY_SCHEDULED | CANCELLED
//	RETRY_SCHEDULED -> RUNNING | SKIPPED | CANCELLED
//	COMPLETED       -> COMPENSATED
//
// READY/RETRY_SCHEDULED -> RUNNING is performed atomically by the task queue
// when a worker claims the task.
var taskTransitions = map[TaskStatus][]TaskStatus{
	TaskPending:        {TaskReady, TaskSkipped, TaskCancelled},
	TaskReady:          {TaskRunning, TaskSkipped, TaskCancelled},
	TaskRunning:        {TaskCompleted, TaskFailed, TaskRetryScheduled, TaskCancelled},
	TaskRetryScheduled: {TaskRunning, TaskSkipped, TaskCancelled},
	TaskCompleted:      {TaskCompensated},
}

func (s TaskStatus) CanTransitionTo(to TaskStatus) bool {
	return slices.Contains(taskTransitions[s], to)
}

// Finished reports whether the task will not run again.
func (s TaskStatus) Finished() bool {
	switch s {
	case TaskCompleted, TaskFailed, TaskCancelled, TaskSkipped, TaskCompensated:
		return true
	}
	return false
}

// InFlight reports whether the task is running or will run without further decisions.
func (s TaskStatus) InFlight() bool {
	return s == TaskReady || s == TaskRunning || s == TaskRetryScheduled
}

func transitionError(kind string, from, to any) error {
	return fmt.Errorf("%w: %s %v -> %v", ErrInvalidTransition, kind, from, to)
}
