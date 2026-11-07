package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace/noop"

	"github.com/MatheusAndrell/go-flow/internal/application/engine"
	"github.com/MatheusAndrell/go-flow/internal/domain/execution"
	"github.com/MatheusAndrell/go-flow/pkg/handler"
)

// fakeQueue hands out a fixed set of tasks and tracks lease ownership.
type fakeQueue struct {
	mu        sync.Mutex
	pending   []engine.ClaimedTask
	revoked   map[string]bool
	claimErr  error
	maxAsked  int
	deregistd bool
}

func (q *fakeQueue) Register(context.Context, Registration) error { return nil }
func (q *fakeQueue) Deregister(context.Context, string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.deregistd = true
	return nil
}

func (q *fakeQueue) Claim(_ context.Context, _ string, _ []string, limit int, _ time.Duration) ([]engine.ClaimedTask, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.claimErr != nil {
		return nil, q.claimErr
	}
	q.maxAsked = max(q.maxAsked, limit)
	n := min(limit, len(q.pending))
	out := q.pending[:n]
	q.pending = q.pending[n:]
	return out, nil
}

func (q *fakeQueue) Heartbeat(_ context.Context, _ string, leases []engine.Lease, _ time.Duration) ([]string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var alive []string
	for _, l := range leases {
		if !q.revoked[l.TaskID] {
			alive = append(alive, l.TaskID)
		}
	}
	return alive, nil
}

type report struct {
	taskID  string
	ok      bool
	failure execution.Failure
}

type fakeReporter struct {
	mu      sync.Mutex
	reports []report
	done    chan struct{}
	want    int
}

func newReporter(want int) *fakeReporter { return &fakeReporter{done: make(chan struct{}), want: want} }

func (r *fakeReporter) TaskStarted(context.Context, engine.Lease) error { return nil }

func (r *fakeReporter) CompleteTask(_ context.Context, l engine.Lease, _ json.RawMessage) error {
	r.add(report{taskID: l.TaskID, ok: true})
	return nil
}

func (r *fakeReporter) FailTask(_ context.Context, l engine.Lease, f execution.Failure) (execution.Outcome, error) {
	r.add(report{taskID: l.TaskID, failure: f})
	return execution.Outcome{}, nil
}

func (r *fakeReporter) add(rep report) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reports = append(r.reports, rep)
	if len(r.reports) == r.want {
		close(r.done)
	}
}

func (r *fakeReporter) byTask() map[string]report {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := map[string]report{}
	for _, rep := range r.reports {
		out[rep.taskID] = rep
	}
	return out
}

type nopMetrics struct{}

func (nopMetrics) TaskClaimed(string, time.Duration)  {}
func (nopMetrics) TaskExecuted(string, time.Duration) {}
func (nopMetrics) WorkerInFlight(string, int)         {}
func (nopMetrics) WorkerCapacity(string, int)         {}

func tasks(n int, handlerName string, timeout time.Duration) []engine.ClaimedTask {
	out := make([]engine.ClaimedTask, n)
	for i := range out {
		out[i] = engine.ClaimedTask{
			Lease:   engine.Lease{TaskID: fmt.Sprintf("t%d", i), ExecutionID: "e", Token: 1, Handler: handlerName, Kind: execution.KindAction},
			Attempt: 1, Timeout: timeout, Input: json.RawMessage(`{"workflow_input":{}}`),
		}
	}
	return out
}

func newWorker(q Queue, r Reporter, reg *handler.Registry, cfg Config) *Worker {
	return New(cfg, q, r, reg, nopMetrics{}, noop.NewTracerProvider().Tracer("test"), slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func runUntil(t *testing.T, w *Worker, done <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for reports")
	}
	cancel()
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestConcurrencyLimitIsNeverExceeded(t *testing.T) {
	const limit, total = 4, 40
	var running, peak atomic.Int32
	reg := handler.NewRegistry()
	reg.MustRegister("work", handler.Func(func(ctx context.Context, _ handler.Request) (json.RawMessage, error) {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		running.Add(-1)
		return json.RawMessage(`{}`), nil
	}))
	q := &fakeQueue{pending: tasks(total, "work", time.Second)}
	r := newReporter(total)
	runUntil(t, newWorker(q, r, reg, Config{Concurrency: limit, PollInterval: time.Millisecond}), r.done)
	if peak.Load() > limit {
		t.Fatalf("peak concurrency %d exceeded limit %d", peak.Load(), limit)
	}
	if q.maxAsked > limit {
		t.Fatalf("claimed %d tasks at once with only %d slots", q.maxAsked, limit)
	}
	if len(r.byTask()) != total {
		t.Fatal("not every task was reported")
	}
	if !q.deregistd {
		t.Fatal("worker must deregister on shutdown")
	}
}

func TestFailureClassification(t *testing.T) {
	reg := handler.NewRegistry()
	reg.MustRegister("permanent", handler.Func(func(context.Context, handler.Request) (json.RawMessage, error) {
		return nil, handler.Permanentf("bad input")
	}))
	reg.MustRegister("transient", handler.Func(func(context.Context, handler.Request) (json.RawMessage, error) {
		return nil, errors.New("connection reset")
	}))
	reg.MustRegister("panics", handler.Func(func(context.Context, handler.Request) (json.RawMessage, error) {
		panic("nil map")
	}))
	reg.MustRegister("slow", handler.Func(func(ctx context.Context, _ handler.Request) (json.RawMessage, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	reg.MustRegister("ignores-deadline", handler.Func(func(context.Context, handler.Request) (json.RawMessage, error) {
		time.Sleep(50 * time.Millisecond)
		return json.RawMessage(`{}`), nil
	}))
	reg.MustRegister("bad-json", handler.Func(func(context.Context, handler.Request) (json.RawMessage, error) {
		return json.RawMessage(`{nope`), nil
	}))
	var all []engine.ClaimedTask
	want := map[string]execution.ErrorKind{
		"permanent": execution.ErrorPermanent, "transient": execution.ErrorRetryable, "panics": execution.ErrorPanic,
		"slow": execution.ErrorTimeout, "ignores-deadline": execution.ErrorTimeout, "bad-json": execution.ErrorPermanent,
	}
	for name := range want {
		tk := tasks(1, name, 10*time.Millisecond)[0]
		tk.TaskID = name
		all = append(all, tk)
	}
	q := &fakeQueue{pending: all}
	r := newReporter(len(all))
	runUntil(t, newWorker(q, r, reg, Config{Concurrency: 8, PollInterval: time.Millisecond}), r.done)
	got := r.byTask()
	for name, kind := range want {
		if got[name].ok || got[name].failure.Kind != kind {
			t.Errorf("%s: got %+v, want kind %s", name, got[name], kind)
		}
	}
}

func TestLostLeaseCancelsHandlerWithoutReporting(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	reg := handler.NewRegistry()
	reg.MustRegister("block", handler.Func(func(ctx context.Context, _ handler.Request) (json.RawMessage, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return nil, ctx.Err()
	}))
	q := &fakeQueue{pending: tasks(1, "block", time.Minute), revoked: map[string]bool{}}
	r := newReporter(1)
	w := newWorker(q, r, reg, Config{Concurrency: 1, PollInterval: time.Millisecond, LeaseDuration: 300 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()
	<-started
	q.mu.Lock()
	q.revoked["t0"] = true
	q.mu.Unlock()
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not cancelled after the lease was lost")
	}
	cancel()
	<-errc
	if len(r.byTask()) != 0 {
		t.Fatal("a worker that lost its lease must not report a result")
	}
}

func TestShutdownAbortsTasksAfterTimeout(t *testing.T) {
	started := make(chan struct{})
	reg := handler.NewRegistry()
	reg.MustRegister("block", handler.Func(func(ctx context.Context, _ handler.Request) (json.RawMessage, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	q := &fakeQueue{pending: tasks(1, "block", time.Minute)}
	r := newReporter(1)
	w := newWorker(q, r, reg, Config{Concurrency: 1, PollInterval: time.Millisecond, ShutdownTimeout: 50 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()
	<-started
	cancel()
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if got := r.byTask()["t0"]; got.failure.Kind != execution.ErrorShutdown {
		t.Fatalf("aborted task must be reported as retryable shutdown, got %+v", got)
	}
}

func TestShutdownDrainsInFlightTasks(t *testing.T) {
	started := make(chan struct{})
	reg := handler.NewRegistry()
	reg.MustRegister("work", handler.Func(func(ctx context.Context, _ handler.Request) (json.RawMessage, error) {
		close(started)
		time.Sleep(100 * time.Millisecond)
		return json.RawMessage(`{}`), ctx.Err()
	}))
	q := &fakeQueue{pending: tasks(1, "work", time.Minute)}
	r := newReporter(1)
	w := newWorker(q, r, reg, Config{Concurrency: 1, PollInterval: time.Millisecond, ShutdownTimeout: 5 * time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()
	<-started
	cancel()
	<-errc
	if !r.byTask()["t0"].ok {
		t.Fatal("in-flight task must finish during graceful shutdown")
	}
}

func TestClaimErrorsBackOffWithoutCrashing(t *testing.T) {
	reg := handler.NewRegistry()
	reg.MustRegister("x", handler.Func(func(context.Context, handler.Request) (json.RawMessage, error) { return nil, nil }))
	q := &fakeQueue{claimErr: errors.New("connection refused")}
	w := newWorker(q, newReporter(1), reg, Config{Concurrency: 1, PollInterval: time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := w.Run(ctx); err != nil {
		t.Fatalf("worker must survive storage errors: %v", err)
	}
}

func TestRunRequiresHandlers(t *testing.T) {
	w := newWorker(&fakeQueue{}, newReporter(1), handler.NewRegistry(), Config{})
	if err := w.Run(context.Background()); err == nil {
		t.Fatal("expected error")
	}
}
