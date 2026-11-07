package execution

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/domain/retry"
	"github.com/MatheusAndrell/go-flow/internal/domain/workflow"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func idGen() IDGenerator {
	n := 0
	return func() string { n++; return fmt.Sprintf("id-%d", n) }
}

func published(spec workflow.Spec) *workflow.Version {
	return &workflow.Version{ID: "ver", DefinitionID: "def", Number: 1, Status: workflow.VersionPublished, Spec: spec}
}

// orderSpec: validate -> (reserve, charge) -> invoice
func orderSpec() workflow.Spec {
	noRetry := retry.NoRetry()
	return workflow.Spec{Steps: []workflow.Step{
		{Key: "validate", Handler: "orders.validate"},
		{Key: "reserve", Handler: "inventory.reserve", DependsOn: []string{"validate"}, Compensation: &workflow.Compensation{Handler: "inventory.release"}},
		{Key: "charge", Handler: "payment.charge", DependsOn: []string{"validate"}, Compensation: &workflow.Compensation{Handler: "payment.refund", Retry: &noRetry}},
		{Key: "invoice", Handler: "invoice.issue", DependsOn: []string{"reserve", "charge"}, Retry: &noRetry},
	}}
}

func startRun(t *testing.T, spec workflow.Spec) *Run {
	t.Helper()
	r, err := Start(StartParams{ExecutionID: "exec", TenantID: "tenant", WorkflowName: "orders", Version: published(spec), Input: json.RawMessage(`{"order":1}`)}, idGen(), t0)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// claim simulates the task queue's atomic claim.
func claim(t *testing.T, r *Run, kind TaskKind, key string, now time.Time) *Task {
	t.Helper()
	task := find(r, kind, key)
	if task.Status != TaskReady && task.Status != TaskRetryScheduled {
		t.Fatalf("task %s is %s, not claimable", key, task.Status)
	}
	task.Status = TaskRunning
	task.Attempts++
	task.LeaseToken++
	task.LeaseOwner = "worker-1"
	exp := now.Add(30 * time.Second)
	task.LeaseExpiresAt = &exp
	if err := r.RecordStarted(task.ID, task.LeaseToken, now); err != nil {
		t.Fatal(err)
	}
	return task
}

func succeed(t *testing.T, r *Run, key string, now time.Time) {
	t.Helper()
	task := claim(t, r, KindAction, key, now)
	if err := r.RecordSuccess(task.ID, task.LeaseToken, json.RawMessage(fmt.Sprintf(`{"step":%q}`, key)), now); err != nil {
		t.Fatal(err)
	}
}

func find(r *Run, kind TaskKind, key string) *Task {
	for _, t := range r.Tasks {
		if t.Kind == kind && t.StepKey == key {
			return t
		}
	}
	return nil
}

func statuses(r *Run, kind TaskKind) map[string]TaskStatus {
	out := map[string]TaskStatus{}
	for _, t := range r.Tasks {
		if t.Kind == kind {
			out[t.StepKey] = t.Status
		}
	}
	return out
}

func TestStartActivatesRootsOnly(t *testing.T) {
	r := startRun(t, orderSpec())
	if r.Execution.Status != StatusRunning || r.Execution.StartedAt == nil {
		t.Fatalf("status %s", r.Execution.Status)
	}
	want := map[string]TaskStatus{"validate": TaskReady, "reserve": TaskPending, "charge": TaskPending, "invoice": TaskPending}
	if got := statuses(r, KindAction); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if len(r.Changes().NewTasks) != 4 {
		t.Fatal("tasks not reported as new")
	}
}

func TestScheduledExecutionStaysPendingUntilDue(t *testing.T) {
	later := t0.Add(time.Hour)
	r, err := Start(StartParams{ExecutionID: "e", TenantID: "t", Version: published(orderSpec()), ScheduledAt: &later}, idGen(), t0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Execution.Status != StatusPending {
		t.Fatalf("status %s", r.Execution.Status)
	}
	if err := r.Activate(t0.Add(time.Minute)); !errors.Is(err, ErrNotDue) {
		t.Fatalf("expected ErrNotDue, got %v", err)
	}
	if err := r.Activate(later); err != nil || r.Execution.Status != StatusRunning {
		t.Fatalf("activation failed: %v %s", err, r.Execution.Status)
	}
}

func TestStartRequiresPublishedVersion(t *testing.T) {
	v := published(orderSpec())
	v.Status = workflow.VersionDraft
	if _, err := Start(StartParams{Version: v}, idGen(), t0); err == nil {
		t.Fatal("draft versions must not be executable")
	}
}

func TestHappyPathRespectsDAGAndPassesDependencyOutputs(t *testing.T) {
	r := startRun(t, orderSpec())
	succeed(t, r, "validate", t0)
	if s := statuses(r, KindAction); s["reserve"] != TaskReady || s["charge"] != TaskReady || s["invoice"] != TaskPending {
		t.Fatalf("parallel branches not readied: %v", s)
	}
	succeed(t, r, "reserve", t0.Add(time.Second))
	if find(r, KindAction, "invoice").Status != TaskPending {
		t.Fatal("invoice must wait for both dependencies")
	}
	succeed(t, r, "charge", t0.Add(2*time.Second))
	invoice := find(r, KindAction, "invoice")
	var in TaskInput
	if err := json.Unmarshal(invoice.Input, &in); err != nil {
		t.Fatal(err)
	}
	if string(in.Dependencies["charge"]) != `{"step":"charge"}` || string(in.WorkflowInput) != `{"order":1}` {
		t.Fatalf("unexpected input: %s", invoice.Input)
	}
	succeed(t, r, "invoice", t0.Add(3*time.Second))
	if r.Execution.Status != StatusCompleted || r.Execution.CompletedAt == nil {
		t.Fatalf("status %s", r.Execution.Status)
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(r.Execution.Output, &out); err != nil || len(out) != 4 {
		t.Fatalf("bad output %s", r.Execution.Output)
	}
}

func TestMaxParallelismLimitsReadyTasks(t *testing.T) {
	spec := workflow.Spec{Settings: workflow.Settings{MaxParallelism: 2}, Steps: []workflow.Step{
		{Key: "a", Handler: "h"}, {Key: "b", Handler: "h"}, {Key: "c", Handler: "h"},
	}}
	r := startRun(t, spec)
	if s := statuses(r, KindAction); s["a"] != TaskReady || s["b"] != TaskReady || s["c"] != TaskPending {
		t.Fatalf("parallelism not enforced: %v", s)
	}
	succeed(t, r, "a", t0)
	if find(r, KindAction, "c").Status != TaskReady {
		t.Fatal("slot freed by a must ready c")
	}
}

func TestRetryableFailureSchedulesBackoffAndWaits(t *testing.T) {
	spec := workflow.Spec{Steps: []workflow.Step{{Key: "a", Handler: "h", Retry: &retry.Policy{MaxAttempts: 3, InitialInterval: 10 * time.Second, MaxInterval: time.Minute, Multiplier: 2}}}}
	r := startRun(t, spec)
	task := claim(t, r, KindAction, "a", t0)
	out, err := r.RecordFailure(task.ID, task.LeaseToken, Failure{Kind: ErrorRetryable, Message: "boom"}, t0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !out.Retried || !out.NextAttemptAt.Equal(t0.Add(10*time.Second)) || task.Status != TaskRetryScheduled {
		t.Fatalf("unexpected outcome %+v status %s", out, task.Status)
	}
	if r.Execution.Status != StatusWaiting {
		t.Fatalf("execution must wait during backoff, got %s", r.Execution.Status)
	}
	task = claim(t, r, KindAction, "a", t0.Add(10*time.Second))
	if r.Execution.Status != StatusRunning {
		t.Fatal("claiming a retry must resume RUNNING")
	}
	out, _ = r.RecordFailure(task.ID, task.LeaseToken, Failure{Kind: ErrorTimeout, Message: "slow"}, t0.Add(11*time.Second), nil)
	if !out.NextAttemptAt.Equal(t0.Add(31 * time.Second)) {
		t.Fatalf("second backoff wrong: %s", out.NextAttemptAt)
	}
	task = claim(t, r, KindAction, "a", t0.Add(31*time.Second))
	out, _ = r.RecordFailure(task.ID, task.LeaseToken, Failure{Kind: ErrorRetryable, Message: "still"}, t0.Add(32*time.Second), nil)
	if !out.DeadLettered || task.Status != TaskFailed || r.Execution.Status != StatusFailed {
		t.Fatalf("exhausted retries must fail: %+v %s %s", out, task.Status, r.Execution.Status)
	}
	ch := r.Changes()
	if len(ch.DeadLetters) != 1 || ch.DeadLetters[0].Attempts != 3 || len(ch.Attempts) != 3 {
		t.Fatalf("dead letter/attempts not recorded: %+v", ch)
	}
}

func TestPermanentFailureIsNotRetried(t *testing.T) {
	r := startRun(t, orderSpec())
	task := claim(t, r, KindAction, "validate", t0)
	out, err := r.RecordFailure(task.ID, task.LeaseToken, Failure{Kind: ErrorPermanent, Message: "invalid order"}, t0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out.Retried || !out.DeadLettered {
		t.Fatal("permanent errors must not be retried")
	}
	if r.Execution.Status != StatusFailed || !strings.Contains(r.Execution.Error, "invalid order") {
		t.Fatalf("nothing to compensate, execution must fail: %s %q", r.Execution.Status, r.Execution.Error)
	}
	for _, k := range []string{"reserve", "charge", "invoice"} {
		if find(r, KindAction, k).Status != TaskSkipped {
			t.Fatalf("%s must be skipped", k)
		}
	}
}

func TestPanicIsNotRetryable(t *testing.T) {
	if ErrorPanic.Retryable() || ErrorPermanent.Retryable() || !ErrorLeaseExpired.Retryable() || !ErrorTimeout.Retryable() {
		t.Fatal("error classification is wrong")
	}
}

func TestSagaCompensatesInReverseCompletionOrder(t *testing.T) {
	r := startRun(t, orderSpec())
	succeed(t, r, "validate", t0)
	succeed(t, r, "charge", t0.Add(time.Second))
	succeed(t, r, "reserve", t0.Add(2*time.Second))
	inv := claim(t, r, KindAction, "invoice", t0.Add(3*time.Second))
	if _, err := r.RecordFailure(inv.ID, inv.LeaseToken, Failure{Kind: ErrorPermanent, Message: "tax service down"}, t0.Add(3*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if r.Execution.Status != StatusCompensating {
		t.Fatalf("status %s", r.Execution.Status)
	}
	comps := r.compensations()
	if len(comps) != 2 || comps[0].StepKey != "reserve" || comps[1].StepKey != "charge" {
		t.Fatalf("compensations must undo reserve (last completed) before charge: %v", keys(comps))
	}
	if comps[0].Status != TaskReady || comps[1].Status != TaskPending {
		t.Fatal("compensations must run one at a time")
	}
	var in TaskInput
	_ = json.Unmarshal(comps[0].Input, &in)
	if string(in.CompensatedOutput) != `{"step":"reserve"}` {
		t.Fatalf("compensation must receive the original output, got %s", comps[0].Input)
	}

	c := claim(t, r, KindCompensation, "reserve", t0.Add(4*time.Second))
	if err := r.RecordSuccess(c.ID, c.LeaseToken, nil, t0.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	if find(r, KindAction, "reserve").Status != TaskCompensated || comps[1].Status != TaskReady {
		t.Fatal("first compensation must mark the step compensated and ready the next")
	}
	c = claim(t, r, KindCompensation, "charge", t0.Add(5*time.Second))
	_ = r.RecordSuccess(c.ID, c.LeaseToken, nil, t0.Add(5*time.Second))
	if r.Execution.Status != StatusCompensated || !strings.Contains(r.Execution.Error, "tax service down") {
		t.Fatalf("status %s error %q", r.Execution.Status, r.Execution.Error)
	}
	if find(r, KindAction, "validate").Status != TaskCompleted {
		t.Fatal("steps without compensation stay completed")
	}
}

func TestSagaWaitsForRunningSiblingsBeforeCompensating(t *testing.T) {
	r := startRun(t, orderSpec())
	succeed(t, r, "validate", t0)
	reserve := claim(t, r, KindAction, "reserve", t0)
	charge := claim(t, r, KindAction, "charge", t0)
	if _, err := r.RecordFailure(charge.ID, charge.LeaseToken, Failure{Kind: ErrorPermanent, Message: "card declined"}, t0, nil); err != nil {
		t.Fatal(err)
	}
	if r.Execution.Status != StatusRunning || len(r.compensations()) != 0 {
		t.Fatal("must wait for the in-flight reserve before compensating")
	}
	if err := r.RecordSuccess(reserve.ID, reserve.LeaseToken, nil, t0.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	comps := r.compensations()
	if r.Execution.Status != StatusCompensating || len(comps) != 1 || comps[0].StepKey != "reserve" {
		t.Fatalf("reserve completed after the failure and must be compensated: %s %v", r.Execution.Status, keys(comps))
	}
	if find(r, KindAction, "invoice").Status != TaskSkipped {
		t.Fatal("invoice must be skipped")
	}
}

func TestCompensationFailureFailsExecution(t *testing.T) {
	r := startRun(t, orderSpec())
	succeed(t, r, "validate", t0)
	succeed(t, r, "reserve", t0.Add(time.Second))
	succeed(t, r, "charge", t0.Add(2*time.Second))
	inv := claim(t, r, KindAction, "invoice", t0.Add(3*time.Second))
	_, _ = r.RecordFailure(inv.ID, inv.LeaseToken, Failure{Kind: ErrorPermanent, Message: "x"}, t0.Add(3*time.Second), nil)
	refund := claim(t, r, KindCompensation, "charge", t0.Add(4*time.Second))
	out, err := r.RecordFailure(refund.ID, refund.LeaseToken, Failure{Kind: ErrorRetryable, Message: "gateway down"}, t0.Add(4*time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !out.DeadLettered || r.Execution.Status != StatusFailed || !strings.Contains(r.Execution.Error, "compensation of step \"charge\" failed") {
		t.Fatalf("got %+v %s %q", out, r.Execution.Status, r.Execution.Error)
	}
	if find(r, KindCompensation, "reserve").Status != TaskPending {
		t.Fatal("later compensations must not run after a failed one")
	}
}

func TestCompensationRetriesAreScheduledPersistently(t *testing.T) {
	r := startRun(t, orderSpec())
	succeed(t, r, "validate", t0)
	succeed(t, r, "reserve", t0.Add(time.Second))
	succeed(t, r, "charge", t0.Add(2*time.Second))
	inv := claim(t, r, KindAction, "invoice", t0.Add(3*time.Second))
	_, _ = r.RecordFailure(inv.ID, inv.LeaseToken, Failure{Kind: ErrorPermanent}, t0.Add(3*time.Second), nil)
	succeedComp := func(key string, at time.Time) {
		c := claim(t, r, KindCompensation, key, at)
		if err := r.RecordSuccess(c.ID, c.LeaseToken, nil, at); err != nil {
			t.Fatal(err)
		}
	}
	succeedComp("charge", t0.Add(4*time.Second))
	release := claim(t, r, KindCompensation, "reserve", t0.Add(5*time.Second))
	out, _ := r.RecordFailure(release.ID, release.LeaseToken, Failure{Kind: ErrorRetryable, Message: "db"}, t0.Add(5*time.Second), nil)
	if !out.Retried || r.Execution.Status != StatusCompensating {
		t.Fatalf("compensation must be retried: %+v %s", out, r.Execution.Status)
	}
	succeedComp("reserve", out.NextAttemptAt)
	if r.Execution.Status != StatusCompensated {
		t.Fatalf("status %s", r.Execution.Status)
	}
}

func TestLeaseFencingRejectsStaleResults(t *testing.T) {
	r := startRun(t, orderSpec())
	task := claim(t, r, KindAction, "validate", t0)
	stale := task.LeaseToken
	if _, err := r.RecordAbandoned(task.ID, t0.Add(10*time.Second), nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("lease not yet expired must not be reaped")
	}
	out, err := r.RecordAbandoned(task.ID, t0.Add(time.Minute), nil)
	if err != nil || !out.Retried {
		t.Fatalf("expired lease must be retried: %v %+v", err, out)
	}
	if err := r.RecordSuccess(task.ID, stale, nil, t0.Add(2*time.Minute)); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("result from a worker that lost its lease must be rejected")
	}
	task = claim(t, r, KindAction, "validate", out.NextAttemptAt)
	if err := r.RecordSuccess(task.ID, stale, nil, out.NextAttemptAt); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("old lease token must be fenced after re-claim")
	}
	if err := r.RecordSuccess(task.ID, task.LeaseToken, nil, out.NextAttemptAt); err != nil {
		t.Fatal(err)
	}
}

func TestCancelFencesRunningTasks(t *testing.T) {
	r := startRun(t, orderSpec())
	task := claim(t, r, KindAction, "validate", t0)
	if err := r.Cancel("customer request", false, t0); err != nil {
		t.Fatal(err)
	}
	if r.Execution.Status != StatusCancelled || task.Status != TaskCancelled {
		t.Fatalf("got %s/%s", r.Execution.Status, task.Status)
	}
	if err := r.RecordSuccess(task.ID, task.LeaseToken, nil, t0); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("completion after cancel must be rejected")
	}
	if err := r.Cancel("again", false, t0); !errors.Is(err, ErrInvalidTransition) {
		t.Fatal("cancel of a terminal execution must fail")
	}
	if a := r.Changes().Attempts; len(a) != 1 || a[0].Status != AttemptCancelled {
		t.Fatalf("running attempt must be closed as cancelled: %+v", a)
	}
}

func TestCancelWithCompensation(t *testing.T) {
	r := startRun(t, orderSpec())
	succeed(t, r, "validate", t0)
	succeed(t, r, "reserve", t0.Add(time.Second))
	if err := r.Cancel("fraud", true, t0.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if r.Execution.Status != StatusCompensating || len(r.compensations()) != 1 {
		t.Fatalf("got %s", r.Execution.Status)
	}
	// Nothing compensable: cancel ends directly.
	r2 := startRun(t, orderSpec())
	if err := r2.Cancel("", true, t0); err != nil || r2.Execution.Status != StatusCancelled {
		t.Fatalf("got %v %s", err, r2.Execution.Status)
	}
}

func TestCancelIsRejectedDuringCompensation(t *testing.T) {
	r := startRun(t, orderSpec())
	succeed(t, r, "validate", t0)
	succeed(t, r, "reserve", t0)
	_ = r.Cancel("x", true, t0)
	if err := r.Cancel("y", false, t0); !errors.Is(err, ErrInvalidTransition) {
		t.Fatal("compensation must not be interrupted")
	}
}

func TestPauseDefersDecisionsUntilResume(t *testing.T) {
	r := startRun(t, orderSpec())
	task := claim(t, r, KindAction, "validate", t0)
	if err := r.Pause(t0); err != nil {
		t.Fatal(err)
	}
	if err := r.RecordSuccess(task.ID, task.LeaseToken, nil, t0); err != nil {
		t.Fatal("in-flight task results are accepted while paused")
	}
	if find(r, KindAction, "reserve").Status != TaskPending {
		t.Fatal("no new work may be scheduled while paused")
	}
	if err := r.Resume(t0); err != nil {
		t.Fatal(err)
	}
	if find(r, KindAction, "reserve").Status != TaskReady || r.Execution.Status != StatusRunning {
		t.Fatal("resume must apply deferred decisions")
	}
	if err := r.Resume(t0); !errors.Is(err, ErrInvalidTransition) {
		t.Fatal("resume of a running execution must fail")
	}
}

func TestAdvanceIsIdempotent(t *testing.T) {
	r := startRun(t, orderSpec())
	before := len(r.Changes().Events)
	if err := r.Advance(t0); err != nil {
		t.Fatal(err)
	}
	if len(r.Changes().Events) != before {
		t.Fatal("advance without changes must not emit events")
	}
}

func TestEveryTransitionIsRecorded(t *testing.T) {
	r := startRun(t, orderSpec())
	succeed(t, r, "validate", t0)
	var execChanges, taskChanges int
	for _, e := range r.Changes().Events {
		switch e.Type {
		case EventExecutionStatusChanged:
			execChanges++
		case EventTaskStatusChanged:
			taskChanges++
		}
		if e.ExecutionID != "exec" || e.TenantID != "tenant" {
			t.Fatalf("event not attributed: %+v", e)
		}
	}
	// PENDING->RUNNING; validate READY, COMPLETED; reserve/charge READY.
	if execChanges != 1 || taskChanges != 4 {
		t.Fatalf("exec=%d task=%d", execChanges, taskChanges)
	}
}

func keys(tasks []*Task) []string {
	var out []string
	for _, t := range tasks {
		out = append(out, t.StepKey)
	}
	return out
}
