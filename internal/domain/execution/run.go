package execution

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/domain/workflow"
)

var (
	// ErrLeaseLost means the caller no longer owns the task: the lease expired,
	// another worker claimed it, or the execution was cancelled. The caller's
	// result must be discarded.
	ErrLeaseLost    = errors.New("task lease lost")
	ErrTaskNotFound = errors.New("task not found")
	ErrNotDue       = errors.New("execution is not due yet")
)

// IDGenerator creates identifiers for new entities.
type IDGenerator func() string

// Run is the execution aggregate: the execution plus all of its tasks.
// Methods mutate state through the state machines and record every change
// so a repository can persist them atomically.
type Run struct {
	Execution *Execution
	Tasks     []*Task

	ids          IDGenerator
	execChanged  bool
	changedTasks map[string]bool
	newTasks     []*Task
	events       []Event
	attempts     []AttemptResult
	deadLetters  []DeadLetter
}

// Restore rebuilds a run loaded from storage. Tasks must be ordered by kind and position.
func Restore(exec *Execution, tasks []*Task, ids IDGenerator) *Run {
	return &Run{Execution: exec, Tasks: tasks, ids: ids, changedTasks: map[string]bool{}}
}

type StartParams struct {
	ExecutionID    string
	TenantID       string
	WorkflowName   string
	Version        *workflow.Version
	Input          json.RawMessage
	IdempotencyKey string
	ScheduledAt    *time.Time
	TraceParent    string
}

// Start creates a run bound to an immutable published version. If it is not
// scheduled for the future it is activated immediately.
func Start(p StartParams, ids IDGenerator, now time.Time) (*Run, error) {
	if p.Version == nil || p.Version.Status != workflow.VersionPublished {
		return nil, workflow.ErrNoPublishedVersion
	}
	input := p.Input
	if len(input) == 0 {
		input = json.RawMessage(`{}`)
	}
	scheduled := now
	if p.ScheduledAt != nil && p.ScheduledAt.After(now) {
		scheduled = *p.ScheduledAt
	}
	spec := p.Version.Spec
	exec := &Execution{
		ID: p.ExecutionID, TenantID: p.TenantID, DefinitionID: p.Version.DefinitionID,
		VersionID: p.Version.ID, WorkflowName: p.WorkflowName, VersionNumber: p.Version.Number,
		Status: StatusPending, Input: input, IdempotencyKey: p.IdempotencyKey,
		MaxParallelism: spec.Settings.MaxParallelism, TraceParent: p.TraceParent,
		ScheduledAt: scheduled, CreatedAt: now, UpdatedAt: now,
	}
	order, err := spec.TopologicalOrder()
	if err != nil {
		return nil, err
	}
	byKey := make(map[string]workflow.Step, len(spec.Steps))
	for _, s := range spec.Steps {
		byKey[s.Key] = s
	}
	r := &Run{Execution: exec, ids: ids, changedTasks: map[string]bool{}, execChanged: true}
	for i, key := range order {
		step := byKey[key]
		name := step.Name
		if name == "" {
			name = step.Key
		}
		t := &Task{
			ID: ids(), ExecutionID: exec.ID, TenantID: exec.TenantID, StepKey: step.Key, Name: name,
			Handler: step.Handler, Kind: KindAction, DependsOn: step.DependsOn, Status: TaskPending,
			Params: step.Params, Retry: spec.RetryFor(step), Timeout: spec.TimeoutFor(step),
			Position: i, CreatedAt: now, UpdatedAt: now,
		}
		if c := step.Compensation; c != nil {
			cs := &CompensationSpec{Handler: c.Handler, Timeout: c.Timeout.Std(), Retry: spec.RetryFor(workflow.Step{Retry: c.Retry})}
			if cs.Timeout == 0 {
				cs.Timeout = t.Timeout
			}
			t.Compensation = cs
		}
		r.Tasks = append(r.Tasks, t)
		r.newTasks = append(r.newTasks, t)
	}
	r.record(Event{Type: EventExecutionCreated, To: string(StatusPending),
		Message: fmt.Sprintf("workflow %q version %d", p.WorkflowName, p.Version.Number)}, now)
	if !scheduled.After(now) {
		if err := r.Activate(now); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Activate starts a pending execution once its scheduled time has arrived.
func (r *Run) Activate(now time.Time) error {
	if r.Execution.Status != StatusPending {
		return transitionError("execution", r.Execution.Status, StatusRunning)
	}
	if r.Execution.ScheduledAt.After(now) {
		return ErrNotDue
	}
	if err := r.setStatus(StatusRunning, "", now); err != nil {
		return err
	}
	return r.Advance(now)
}

func (r *Run) Pause(now time.Time) error {
	return r.setStatus(StatusPaused, "paused by request", now)
}

// Resume continues a paused execution and applies any decision that was
// deferred while paused (readying dependents, finishing, compensating).
func (r *Run) Resume(now time.Time) error {
	if r.Execution.Status != StatusPaused {
		return transitionError("execution", r.Execution.Status, StatusRunning)
	}
	if err := r.setStatus(StatusRunning, "resumed by request", now); err != nil {
		return err
	}
	return r.Advance(now)
}

// Cancel stops an execution. Running tasks are fenced off: their results will
// be rejected because the task is no longer RUNNING. With compensate=true the
// completed steps are compensated before the execution ends.
func (r *Run) Cancel(reason string, compensate bool, now time.Time) error {
	st := r.Execution.Status
	if !st.CanTransitionTo(StatusCancelled) {
		return transitionError("execution", st, StatusCancelled)
	}
	msg := "cancelled"
	if reason != "" {
		msg += ": " + reason
	}
	for _, t := range r.actions() {
		if t.Status.Finished() {
			continue
		}
		if t.Status == TaskRunning {
			r.finishAttempt(t, AttemptCancelled, Failure{Message: msg}, now)
			t.clearLease()
		}
		if err := r.setTaskStatus(t, TaskCancelled, msg, now); err != nil {
			return err
		}
	}
	if compensate && st.CanTransitionTo(StatusCompensating) && len(r.compensable()) > 0 {
		return r.beginCompensation(msg, now)
	}
	r.Execution.Error = msg
	return r.setStatus(StatusCancelled, msg, now)
}

// RecordStarted is called by the worker right after claiming a task.
func (r *Run) RecordStarted(taskID string, leaseToken int64, now time.Time) error {
	t, err := r.ownedTask(taskID, leaseToken)
	if err != nil {
		return err
	}
	r.record(Event{Type: EventTaskStarted, TaskID: t.ID, StepKey: t.StepKey, To: string(TaskRunning),
		Message: fmt.Sprintf("attempt %d by %s", t.Attempts, t.LeaseOwner)}, now)
	if r.Execution.Status == StatusWaiting {
		return r.setStatus(StatusRunning, "", now)
	}
	return nil
}

// RecordSuccess stores a task result and advances the workflow.
func (r *Run) RecordSuccess(taskID string, leaseToken int64, output json.RawMessage, now time.Time) error {
	t, err := r.ownedTask(taskID, leaseToken)
	if err != nil {
		return err
	}
	if len(output) == 0 {
		output = json.RawMessage(`null`)
	}
	r.finishAttempt(t, AttemptSucceeded, Failure{}, now)
	t.clearLease()
	t.Output = output
	if err := r.setTaskStatus(t, TaskCompleted, "", now); err != nil {
		return err
	}
	return r.Advance(now)
}

// Outcome tells the caller what happened to a failed task.
type Outcome struct {
	Retried       bool
	NextAttemptAt time.Time
	DeadLettered  bool
}

// RecordFailure schedules a retry when the failure is retryable and attempts
// remain; otherwise the task fails, is dead-lettered and the workflow advances
// (which may start compensation). rnd feeds backoff jitter.
func (r *Run) RecordFailure(taskID string, leaseToken int64, f Failure, now time.Time, rnd func() float64) (Outcome, error) {
	t, err := r.ownedTask(taskID, leaseToken)
	if err != nil {
		return Outcome{}, err
	}
	if f.Kind == "" {
		f.Kind = ErrorRetryable
	}
	r.finishAttempt(t, AttemptFailed, f, now)
	t.clearLease()
	t.LastError = f.truncated()
	t.LastErrorKind = f.Kind

	var out Outcome
	if f.Kind.Retryable() && t.Retry.CanRetry(t.Attempts) {
		next := now.Add(t.Retry.Backoff(t.Attempts, rnd))
		t.RunAt = &next
		msg := fmt.Sprintf("attempt %d failed (%s): %s; retry at %s", t.Attempts, f.Kind, t.LastError, next.UTC().Format(time.RFC3339Nano))
		if err := r.setTaskStatus(t, TaskRetryScheduled, msg, now); err != nil {
			return Outcome{}, err
		}
		out = Outcome{Retried: true, NextAttemptAt: next}
	} else {
		msg := fmt.Sprintf("attempt %d failed (%s): %s", t.Attempts, f.Kind, t.LastError)
		if err := r.setTaskStatus(t, TaskFailed, msg, now); err != nil {
			return Outcome{}, err
		}
		r.deadLetter(t, f, now)
		out = Outcome{DeadLettered: true}
	}
	return out, r.Advance(now)
}

// RecordAbandoned handles a task whose worker stopped heartbeating.
func (r *Run) RecordAbandoned(taskID string, now time.Time, rnd func() float64) (Outcome, error) {
	t := r.task(taskID)
	if t == nil {
		return Outcome{}, ErrTaskNotFound
	}
	if t.Status != TaskRunning || t.LeaseExpiresAt == nil || t.LeaseExpiresAt.After(now) {
		return Outcome{}, ErrLeaseLost
	}
	f := Failure{Kind: ErrorLeaseExpired, Message: fmt.Sprintf("lease held by worker %s expired", t.LeaseOwner)}
	return r.RecordFailure(t.ID, t.LeaseToken, f, now, rnd)
}

// Advance applies every decision implied by the current task states. It is
// idempotent: calling it twice without other changes does nothing.
func (r *Run) Advance(now time.Time) error {
	switch r.Execution.Status {
	case StatusRunning, StatusWaiting:
		return r.advanceActions(now)
	case StatusCompensating:
		return r.advanceCompensation(now)
	}
	return nil
}

func (r *Run) advanceActions(now time.Time) error {
	actions := r.actions()
	if failed := firstWithStatus(actions, TaskFailed); failed != nil {
		reason := fmt.Sprintf("step %q failed: %s", failed.StepKey, failed.LastError)
		running := false
		for _, t := range actions {
			switch t.Status {
			case TaskPending, TaskReady, TaskRetryScheduled:
				if err := r.setTaskStatus(t, TaskSkipped, fmt.Sprintf("skipped because step %q failed", failed.StepKey), now); err != nil {
					return err
				}
			case TaskRunning:
				running = true
			}
		}
		if running {
			// Wait for in-flight siblings: their outcome decides what must be compensated.
			return nil
		}
		return r.beginCompensation(reason, now)
	}

	completed := make(map[string]*Task, len(actions))
	inFlight := 0
	for _, t := range actions {
		if t.Status == TaskCompleted {
			completed[t.StepKey] = t
		}
		if t.Status.InFlight() {
			inFlight++
		}
	}
	limit := r.Execution.MaxParallelism
	for _, t := range actions {
		if t.Status != TaskPending || !dependenciesMet(t, completed) {
			continue
		}
		if limit > 0 && inFlight >= limit {
			break
		}
		if err := r.ready(t, r.actionInput(t, completed), now); err != nil {
			return err
		}
		inFlight++
	}

	if len(completed) == len(actions) {
		outputs := make(map[string]json.RawMessage, len(actions))
		for _, t := range actions {
			outputs[t.StepKey] = t.Output
		}
		out, err := json.Marshal(outputs)
		if err != nil {
			return err
		}
		r.Execution.Output = out
		return r.setStatus(StatusCompleted, "", now)
	}
	return r.updateWaiting(actions, now)
}

func (r *Run) updateWaiting(tasks []*Task, now time.Time) error {
	runnableNow, scheduled := false, false
	for _, t := range tasks {
		switch t.Status {
		case TaskReady, TaskRunning:
			runnableNow = true
		case TaskRetryScheduled:
			scheduled = true
		}
	}
	switch {
	case r.Execution.Status == StatusRunning && !runnableNow && scheduled:
		return r.setStatus(StatusWaiting, "waiting for scheduled retries", now)
	case r.Execution.Status == StatusWaiting && runnableNow:
		return r.setStatus(StatusRunning, "", now)
	}
	return nil
}

func dependenciesMet(t *Task, completed map[string]*Task) bool {
	for _, d := range t.DependsOn {
		if completed[d] == nil {
			return false
		}
	}
	return true
}

func (r *Run) actionInput(t *Task, completed map[string]*Task) TaskInput {
	in := TaskInput{WorkflowInput: r.Execution.Input, Params: t.Params}
	if len(t.DependsOn) > 0 {
		in.Dependencies = make(map[string]json.RawMessage, len(t.DependsOn))
		for _, d := range t.DependsOn {
			in.Dependencies[d] = completed[d].Output
		}
	}
	return in
}

func (r *Run) ready(t *Task, in TaskInput, now time.Time) error {
	data, err := json.Marshal(in)
	if err != nil {
		return err
	}
	t.Input = data
	t.RunAt = &now
	return r.setTaskStatus(t, TaskReady, "", now)
}

// compensable returns completed steps that declare a compensation, most
// recently completed first: the order in which they must be undone.
func (r *Run) compensable() []*Task {
	var out []*Task
	for _, t := range r.actions() {
		if t.Status == TaskCompleted && t.Compensation != nil {
			out = append(out, t)
		}
	}
	sortByCompletionDesc(out)
	return out
}

func (r *Run) beginCompensation(reason string, now time.Time) error {
	r.Execution.Error = reason
	targets := r.compensable()
	if len(targets) == 0 {
		return r.setStatus(StatusFailed, reason, now)
	}
	for i, orig := range targets {
		c := &Task{
			ID: r.ids(), ExecutionID: r.Execution.ID, TenantID: r.Execution.TenantID,
			StepKey: orig.StepKey, Name: "compensate " + orig.Name, Handler: orig.Compensation.Handler,
			Kind: KindCompensation, Status: TaskPending, Params: orig.Params,
			Retry: orig.Compensation.Retry, Timeout: orig.Compensation.Timeout,
			CompensatesTaskID: orig.ID, Position: i, CreatedAt: now, UpdatedAt: now,
		}
		r.Tasks = append(r.Tasks, c)
		r.newTasks = append(r.newTasks, c)
		r.record(Event{Type: EventTaskStatusChanged, TaskID: c.ID, StepKey: c.StepKey, To: string(TaskPending),
			Message: "compensation scheduled"}, now)
	}
	if err := r.setStatus(StatusCompensating, reason, now); err != nil {
		return err
	}
	return r.advanceCompensation(now)
}

// advanceCompensation runs compensations strictly one at a time, in reverse
// completion order. A permanently failed compensation stops the saga and
// fails the execution, leaving it for manual intervention.
func (r *Run) advanceCompensation(now time.Time) error {
	for _, c := range r.compensations() {
		switch c.Status {
		case TaskCompleted:
			if orig := r.task(c.CompensatesTaskID); orig != nil && orig.Status == TaskCompleted {
				if err := r.setTaskStatus(orig, TaskCompensated, "compensated by task "+c.ID, now); err != nil {
					return err
				}
			}
		case TaskFailed:
			msg := fmt.Sprintf("%s; compensation of step %q failed: %s", r.Execution.Error, c.StepKey, c.LastError)
			r.Execution.Error = msg
			return r.setStatus(StatusFailed, msg, now)
		case TaskPending:
			orig := r.task(c.CompensatesTaskID)
			in := TaskInput{WorkflowInput: r.Execution.Input, Params: c.Params}
			if orig != nil {
				in.CompensatedOutput = orig.Output
			}
			return r.ready(c, in, now)
		default:
			return nil
		}
	}
	return r.setStatus(StatusCompensated, r.Execution.Error, now)
}

func (r *Run) ownedTask(taskID string, leaseToken int64) (*Task, error) {
	t := r.task(taskID)
	if t == nil {
		return nil, ErrTaskNotFound
	}
	if t.Status != TaskRunning || t.LeaseToken != leaseToken {
		return nil, ErrLeaseLost
	}
	return t, nil
}

func (r *Run) finishAttempt(t *Task, status AttemptStatus, f Failure, now time.Time) {
	r.attempts = append(r.attempts, AttemptResult{
		TaskID: t.ID, Number: t.Attempts, Status: status,
		Error: f.truncated(), ErrorKind: f.Kind, FinishedAt: now,
	})
}

func (r *Run) deadLetter(t *Task, f Failure, now time.Time) {
	r.deadLetters = append(r.deadLetters, DeadLetter{
		ID: r.ids(), TenantID: t.TenantID, ExecutionID: t.ExecutionID, TaskID: t.ID,
		StepKey: t.StepKey, Handler: t.Handler, Kind: t.Kind, Attempts: t.Attempts,
		Error: f.truncated(), ErrorKind: f.Kind, Input: t.Input, CreatedAt: now,
	})
	r.record(Event{Type: EventTaskDeadLettered, TaskID: t.ID, StepKey: t.StepKey, Message: string(f.Kind)}, now)
}

func (t *Task) clearLease() {
	t.LeaseOwner = ""
	t.LeaseExpiresAt = nil
}

func (r *Run) setStatus(to Status, msg string, now time.Time) error {
	from := r.Execution.Status
	if !from.CanTransitionTo(to) {
		return transitionError("execution", from, to)
	}
	e := r.Execution
	e.Status = to
	e.UpdatedAt = now
	if to == StatusRunning && e.StartedAt == nil {
		e.StartedAt = &now
	}
	if to.Terminal() {
		e.CompletedAt = &now
	}
	r.execChanged = true
	r.record(Event{Type: EventExecutionStatusChanged, From: string(from), To: string(to), Message: msg}, now)
	return nil
}

func (r *Run) setTaskStatus(t *Task, to TaskStatus, msg string, now time.Time) error {
	from := t.Status
	if !from.CanTransitionTo(to) {
		return transitionError("task "+t.StepKey, from, to)
	}
	t.Status = to
	t.UpdatedAt = now
	if to == TaskCompleted || to == TaskFailed || to == TaskCancelled || to == TaskSkipped {
		t.CompletedAt = &now
	}
	r.changedTasks[t.ID] = true
	r.record(Event{Type: EventTaskStatusChanged, TaskID: t.ID, StepKey: t.StepKey, From: string(from), To: string(to), Message: msg}, now)
	return nil
}

func (r *Run) record(e Event, now time.Time) {
	e.ExecutionID = r.Execution.ID
	e.TenantID = r.Execution.TenantID
	e.CreatedAt = now
	r.events = append(r.events, e)
}

func (r *Run) task(id string) *Task {
	for _, t := range r.Tasks {
		if t.ID == id {
			return t
		}
	}
	return nil
}

func (r *Run) actions() []*Task       { return r.byKind(KindAction) }
func (r *Run) compensations() []*Task { return r.byKind(KindCompensation) }

func (r *Run) byKind(k TaskKind) []*Task {
	var out []*Task
	for _, t := range r.Tasks {
		if t.Kind == k {
			out = append(out, t)
		}
	}
	sortByPosition(out)
	return out
}

func firstWithStatus(tasks []*Task, s TaskStatus) *Task {
	for _, t := range tasks {
		if t.Status == s {
			return t
		}
	}
	return nil
}

// Changes exposes what must be persisted. New tasks are not reported as changed.
type Changes struct {
	ExecutionChanged bool
	NewTasks         []*Task
	ChangedTasks     []*Task
	Events           []Event
	Attempts         []AttemptResult
	DeadLetters      []DeadLetter
}

func (r *Run) Changes() Changes {
	isNew := make(map[string]bool, len(r.newTasks))
	for _, t := range r.newTasks {
		isNew[t.ID] = true
	}
	var changed []*Task
	for _, t := range r.Tasks {
		if r.changedTasks[t.ID] && !isNew[t.ID] {
			changed = append(changed, t)
		}
	}
	return Changes{
		ExecutionChanged: r.execChanged, NewTasks: r.newTasks, ChangedTasks: changed,
		Events: r.events, Attempts: r.attempts, DeadLetters: r.deadLetters,
	}
}
