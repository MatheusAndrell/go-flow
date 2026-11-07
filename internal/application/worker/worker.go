// Package worker runs step handlers. A worker claims tasks from a persistent
// queue under a lease, keeps the lease alive with heartbeats while the handler
// runs, and reports the outcome to the engine. Many workers (in one or many
// processes) can run against the same database.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/MatheusAndrell/go-flow/internal/application/engine"
	"github.com/MatheusAndrell/go-flow/internal/domain/execution"
	"github.com/MatheusAndrell/go-flow/pkg/handler"
)

type Registration struct {
	ID          string
	Hostname    string
	Handlers    []string
	Concurrency int
}

type Queue interface {
	Register(ctx context.Context, r Registration) error
	Deregister(ctx context.Context, workerID string) error
	// Claim atomically moves up to limit runnable tasks to RUNNING under a lease owned by workerID.
	Claim(ctx context.Context, workerID string, handlers []string, limit int, lease time.Duration) ([]engine.ClaimedTask, error)
	// Heartbeat extends the given leases and returns the ids of tasks still owned by the worker.
	Heartbeat(ctx context.Context, workerID string, leases []engine.Lease, lease time.Duration) ([]string, error)
}

type Reporter interface {
	TaskStarted(ctx context.Context, l engine.Lease) error
	CompleteTask(ctx context.Context, l engine.Lease, output json.RawMessage) error
	FailTask(ctx context.Context, l engine.Lease, f execution.Failure) (execution.Outcome, error)
}

type Metrics interface {
	TaskClaimed(handler string, queueWait time.Duration)
	TaskExecuted(handler string, duration time.Duration)
	WorkerInFlight(workerID string, n int)
	WorkerCapacity(workerID string, n int)
}

type Config struct {
	ID                string
	Concurrency       int
	BatchSize         int
	PollInterval      time.Duration
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	ShutdownTimeout   time.Duration
}

func (c Config) withDefaults() Config {
	if c.ID == "" {
		host, _ := os.Hostname()
		c.ID = fmt.Sprintf("%s-%d-%08x", host, os.Getpid(), rand.Uint32())
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 10
	}
	if c.BatchSize <= 0 || c.BatchSize > c.Concurrency {
		c.BatchSize = c.Concurrency
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 500 * time.Millisecond
	}
	if c.LeaseDuration <= 0 {
		c.LeaseDuration = 30 * time.Second
	}
	if c.HeartbeatInterval <= 0 || c.HeartbeatInterval >= c.LeaseDuration {
		c.HeartbeatInterval = c.LeaseDuration / 3
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = 30 * time.Second
	}
	return c
}

var (
	errLeaseLost = errors.New("lease lost")
	errShutdown  = errors.New("worker shutting down")
)

type inflight struct {
	lease  engine.Lease
	cancel context.CancelCauseFunc
}

type Worker struct {
	cfg      Config
	queue    Queue
	reporter Reporter
	handlers *handler.Registry
	metrics  Metrics
	tracer   trace.Tracer
	log      *slog.Logger

	mu       sync.Mutex
	inflight map[string]*inflight
}

func New(cfg Config, queue Queue, reporter Reporter, handlers *handler.Registry, metrics Metrics, tracer trace.Tracer, log *slog.Logger) *Worker {
	cfg = cfg.withDefaults()
	return &Worker{
		cfg: cfg, queue: queue, reporter: reporter, handlers: handlers, metrics: metrics, tracer: tracer,
		log: log.With("worker_id", cfg.ID), inflight: map[string]*inflight{},
	}
}

func (w *Worker) ID() string { return w.cfg.ID }

// Run polls for tasks until ctx is cancelled, then drains in-flight tasks for
// up to ShutdownTimeout before aborting them. Aborted tasks are reported as
// retryable failures so another worker picks them up immediately.
func (w *Worker) Run(ctx context.Context) error {
	names := w.handlers.Names()
	if len(names) == 0 {
		return errors.New("worker has no handlers registered")
	}
	host, _ := os.Hostname()
	if err := w.queue.Register(ctx, Registration{ID: w.cfg.ID, Hostname: host, Handlers: names, Concurrency: w.cfg.Concurrency}); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	w.metrics.WorkerCapacity(w.cfg.ID, w.cfg.Concurrency)
	w.log.Info("worker started", "concurrency", w.cfg.Concurrency, "handlers", names)

	// In-flight tasks and heartbeats outlive ctx so the drain can finish.
	detached := context.WithoutCancel(ctx)
	hbCtx, stopHeartbeat := context.WithCancel(detached)
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		w.heartbeatLoop(hbCtx)
	}()
	taskBase, abortTasks := context.WithCancelCause(detached)
	defer abortTasks(nil)

	var wg sync.WaitGroup
	w.pollLoop(ctx, taskBase, names, &wg)

	w.log.Info("worker draining", "in_flight", w.inFlightCount())
	drained := make(chan struct{})
	go func() { wg.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(w.cfg.ShutdownTimeout):
		w.log.Warn("shutdown timeout reached, aborting in-flight tasks", "in_flight", w.inFlightCount())
		abortTasks(errShutdown)
		<-drained
	}
	stopHeartbeat()
	<-hbDone

	deregCtx, cancel := context.WithTimeout(detached, 5*time.Second)
	defer cancel()
	if err := w.queue.Deregister(deregCtx, w.cfg.ID); err != nil {
		w.log.Warn("deregister worker", "error", err)
	}
	w.metrics.WorkerInFlight(w.cfg.ID, 0)
	w.log.Info("worker stopped")
	return nil
}

// pollLoop claims only as many tasks as there are free slots: the semaphore
// is the backpressure mechanism, so a saturated worker stops claiming.
func (w *Worker) pollLoop(ctx context.Context, taskBase context.Context, names []string, wg *sync.WaitGroup) {
	slots := make(chan struct{}, w.cfg.Concurrency)
	failures := 0
	for {
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		acquired := 1
		for acquired < w.cfg.BatchSize {
			select {
			case slots <- struct{}{}:
				acquired++
				continue
			default:
			}
			break
		}

		tasks, err := w.queue.Claim(ctx, w.cfg.ID, names, acquired, w.cfg.LeaseDuration)
		for range acquired - len(tasks) {
			<-slots
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			failures++
			delay := min(w.cfg.PollInterval*time.Duration(1<<min(failures, 6)), 30*time.Second)
			w.log.Error("claim tasks", "error", err, "retry_in", delay)
			if !sleep(ctx, delay) {
				return
			}
			continue
		}
		failures = 0
		for _, t := range tasks {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-slots }()
				w.execute(taskBase, t)
			}()
		}
		if len(tasks) == 0 {
			// Jitter spreads polls of many idle workers.
			if !sleep(ctx, w.cfg.PollInterval/2+rand.N(w.cfg.PollInterval)) {
				return
			}
		}
	}
}

func (w *Worker) execute(base context.Context, t engine.ClaimedTask) {
	ctx, cancel := context.WithCancelCause(base)
	defer cancel(nil)
	w.track(t.Lease, cancel)
	defer w.untrack(t.TaskID)

	w.metrics.TaskClaimed(t.Handler, t.ClaimedAt.Sub(t.RunAt))
	ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": t.TraceParent})
	ctx, span := w.tracer.Start(ctx, "task "+t.Handler, trace.WithSpanKind(trace.SpanKindConsumer), trace.WithAttributes(
		attribute.String("goflow.execution_id", t.ExecutionID),
		attribute.String("goflow.task_id", t.TaskID),
		attribute.String("goflow.step", t.StepKey),
		attribute.String("goflow.task_kind", string(t.Kind)),
		attribute.Int("goflow.attempt", t.Attempt),
	))
	defer span.End()
	log := w.log.With("task_id", t.TaskID, "execution_id", t.ExecutionID, "step", t.StepKey,
		"handler", t.Handler, "attempt", t.Attempt, "trace_id", span.SpanContext().TraceID().String())

	if err := w.report(ctx, func(rc context.Context) error { return w.reporter.TaskStarted(rc, t.Lease) }); err != nil {
		if errors.Is(err, execution.ErrLeaseLost) {
			log.Info("task no longer owned before start; skipping")
			return
		}
		log.Warn("could not record task start; executing anyway", "error", err)
	}

	start := time.Now()
	out, err := w.invoke(ctx, t)
	w.metrics.TaskExecuted(t.Handler, time.Since(start))

	if cause := context.Cause(ctx); errors.Is(cause, errLeaseLost) {
		// Another worker may own the task now; reporting would be rejected anyway.
		log.Warn("lease lost during execution; result discarded")
		span.SetStatus(codes.Error, "lease lost")
		return
	}
	if err == nil {
		err = w.report(ctx, func(rc context.Context) error { return w.reporter.CompleteTask(rc, t.Lease, out) })
		w.logReport(log, err, "task completed")
		return
	}

	failure := classify(ctx, err)
	span.RecordError(err)
	span.SetStatus(codes.Error, string(failure.Kind))
	var outcome execution.Outcome
	rerr := w.report(ctx, func(rc context.Context) error {
		var err error
		outcome, err = w.reporter.FailTask(rc, t.Lease, failure)
		return err
	})
	w.logReport(log.With("error_kind", failure.Kind, "error", failure.Message, "retried", outcome.Retried,
		"dead_lettered", outcome.DeadLettered), rerr, "task failed")
}

type panicError struct {
	value any
	stack []byte
}

func (p panicError) Error() string { return fmt.Sprintf("panic: %v", p.value) }

// invoke runs the handler with the task timeout and converts panics into errors
// so a faulty handler cannot crash the worker process.
func (w *Worker) invoke(ctx context.Context, t engine.ClaimedTask) (out json.RawMessage, err error) {
	h, ok := w.handlers.Lookup(t.Handler)
	if !ok {
		return nil, handler.Permanentf("handler %q is not registered on worker %s", t.Handler, w.cfg.ID)
	}
	var in execution.TaskInput
	if len(t.Input) > 0 {
		if err := json.Unmarshal(t.Input, &in); err != nil {
			return nil, handler.Permanentf("decode task input: %w", err)
		}
	}
	tctx, cancel := context.WithTimeout(ctx, t.Timeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			err = panicError{value: r, stack: debug.Stack()}
		}
	}()
	out, err = h.Handle(tctx, handler.Request{
		TaskID: t.TaskID, ExecutionID: t.ExecutionID, TenantID: t.TenantID, StepKey: t.StepKey,
		Attempt: t.Attempt, Compensation: t.Kind == execution.KindCompensation,
		IdempotencyKey: fmt.Sprintf("%s/%s/%s", t.ExecutionID, t.Kind, t.StepKey),
		WorkflowInput:  in.WorkflowInput, Params: in.Params, Dependencies: in.Dependencies,
		CompensatedOutput: in.CompensatedOutput,
	})
	if err == nil && len(out) > 0 && !json.Valid(out) {
		return nil, handler.Permanentf("handler returned invalid JSON")
	}
	if err == nil && tctx.Err() != nil && ctx.Err() == nil {
		// The handler ignored its deadline; its result arrived too late to trust.
		return nil, context.DeadlineExceeded
	}
	return out, err
}

func classify(ctx context.Context, err error) execution.Failure {
	msg := err.Error()
	var p panicError
	switch {
	case errors.Is(context.Cause(ctx), errShutdown):
		return execution.Failure{Kind: execution.ErrorShutdown, Message: "aborted by worker shutdown: " + msg}
	case errors.As(err, &p):
		return execution.Failure{Kind: execution.ErrorPanic, Message: msg + "\n" + string(p.stack)}
	case handler.IsPermanent(err):
		return execution.Failure{Kind: execution.ErrorPermanent, Message: msg}
	case errors.Is(err, context.DeadlineExceeded):
		return execution.Failure{Kind: execution.ErrorTimeout, Message: "timed out: " + msg}
	default:
		return execution.Failure{Kind: execution.ErrorRetryable, Message: msg}
	}
}

// report delivers an outcome, retrying transient storage errors. If it still
// fails the lease eventually expires and recovery retries the task:
// at-least-once execution is preserved.
func (w *Worker) report(ctx context.Context, fn func(context.Context) error) error {
	base := context.WithoutCancel(ctx)
	var err error
	for attempt := range 5 {
		rc, cancel := context.WithTimeout(base, 10*time.Second)
		err = fn(rc)
		cancel()
		if err == nil || errors.Is(err, execution.ErrLeaseLost) || errors.Is(err, execution.ErrTaskNotFound) {
			return err
		}
		time.Sleep(time.Duration(100*(1<<attempt)) * time.Millisecond)
	}
	return err
}

func (w *Worker) logReport(log *slog.Logger, err error, msg string) {
	switch {
	case err == nil:
		log.Info(msg)
	case errors.Is(err, execution.ErrLeaseLost):
		log.Warn(msg+"; result rejected because the lease was lost", "report_error", err)
	default:
		log.Error(msg+"; outcome could not be stored, lease expiry will trigger recovery", "report_error", err)
	}
}

// heartbeatLoop extends leases of in-flight tasks. Tasks the worker no longer
// owns (cancelled execution, reclaimed after expiry) are cancelled. If the
// database is unreachable for longer than a lease, every task is cancelled
// because other workers may already be running them.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()
	lastOK := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		leases := w.leases()
		w.metrics.WorkerInFlight(w.cfg.ID, len(leases))
		hctx, cancel := context.WithTimeout(ctx, w.cfg.HeartbeatInterval)
		alive, err := w.queue.Heartbeat(hctx, w.cfg.ID, leases, w.cfg.LeaseDuration)
		cancel()
		if err != nil {
			w.log.Error("heartbeat failed", "error", err)
			if time.Since(lastOK) >= w.cfg.LeaseDuration {
				w.cancelWhere(func(string) bool { return true })
			}
			continue
		}
		lastOK = time.Now()
		owned := make(map[string]bool, len(alive))
		for _, id := range alive {
			owned[id] = true
		}
		checked := make(map[string]bool, len(leases))
		for _, l := range leases {
			checked[l.TaskID] = true
		}
		// Only judge tasks included in this heartbeat; newer claims are checked next tick.
		w.cancelWhere(func(id string) bool { return checked[id] && !owned[id] })
	}
}

func (w *Worker) track(l engine.Lease, cancel context.CancelCauseFunc) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.inflight[l.TaskID] = &inflight{lease: l, cancel: cancel}
}

func (w *Worker) untrack(taskID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.inflight, taskID)
}

func (w *Worker) leases() []engine.Lease {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]engine.Lease, 0, len(w.inflight))
	for _, f := range w.inflight {
		out = append(out, f.lease)
	}
	return out
}

func (w *Worker) cancelWhere(pred func(taskID string) bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for id, f := range w.inflight {
		if pred(id) {
			f.cancel(errLeaseLost)
		}
	}
}

func (w *Worker) inFlightCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.inflight)
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
