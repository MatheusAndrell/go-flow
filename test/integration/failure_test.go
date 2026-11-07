package integration

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/domain/execution"
	"github.com/MatheusAndrell/go-flow/pkg/handler"
)

// A worker that dies (no heartbeat, no report) loses its lease; recovery
// reschedules the task and another worker finishes it.
func TestCrashedWorkerTaskIsRecovered(t *testing.T) {
	e := newEnv(t)
	var runs atomic.Int32
	reg := handler.NewRegistry()
	reg.MustRegister(e.h("x"), handler.Func(func(context.Context, handler.Request) (json.RawMessage, error) {
		runs.Add(1)
		return json.RawMessage(`{}`), nil
	}))
	stop := runWorker(t, reg, workerOpts{})
	wf := e.publish("crash", []step{{"key": "x", "handler": e.h("x"), "retry": fastRetry(3)}})
	stop()

	id := e.start(wf, nil)
	// The "crashed" worker claims with a short lease and then disappears.
	claimed, err := shared.Store.Claim(context.Background(), "crashed-worker", []string{e.h("x")}, 1, 200*time.Millisecond)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %d", err, len(claimed))
	}
	time.Sleep(300 * time.Millisecond)
	n, err := shared.Engine.RecoverAbandoned(context.Background(), 100)
	if err != nil || n < 1 {
		t.Fatalf("recover: %d %v", n, err)
	}
	if err := shared.Engine.CompleteTask(context.Background(), claimed[0].Lease, nil); !errors.Is(err, execution.ErrLeaseLost) {
		t.Fatalf("late result from the crashed worker must be fenced, got %v", err)
	}
	runWorker(t, reg, workerOpts{})
	e.waitStatus(id, 10*time.Second, "COMPLETED")
	task := e.tasks(id)["ACTION/x"]
	attempts := e.attempts(task["id"].(string))
	if len(attempts) != 2 || attempts[0]["error_kind"] != "LEASE_EXPIRED" || attempts[0]["worker_id"] != "crashed-worker" {
		t.Fatalf("attempt history must show the abandoned attempt: %v", attempts)
	}
	if runs.Load() != 1 {
		t.Fatalf("handler should have run once on the healthy worker, ran %d", runs.Load())
	}
}

// Stopping the engine mid-flight (shutdown timeout exceeded) must not lose
// work: aborted tasks are retried by the next worker instance.
func TestEngineRestartResumesWork(t *testing.T) {
	e := newEnv(t)
	started := make(chan struct{}, 10)
	var blocking atomic.Bool
	blocking.Store(true)
	reg := handler.NewRegistry()
	reg.MustRegister(e.h("x"), handler.Func(func(ctx context.Context, _ handler.Request) (json.RawMessage, error) {
		if blocking.Load() {
			started <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return json.RawMessage(`{}`), nil
	}))
	stop := runWorker(t, reg, workerOpts{shutdown: 100 * time.Millisecond})
	wf := e.publish("restart", []step{{"key": "x", "handler": e.h("x"), "timeout": "1m", "retry": fastRetry(3)}})
	ids := []string{e.start(wf, nil), e.start(wf, nil), e.start(wf, nil)}
	for range ids {
		<-started
	}
	stop()
	blocking.Store(false)
	runWorker(t, reg, workerOpts{})
	for _, id := range ids {
		e.waitStatus(id, 10*time.Second, "COMPLETED")
		a := e.attempts(e.tasks(id)["ACTION/x"]["id"].(string))
		if a[0]["error_kind"] != "WORKER_SHUTDOWN" {
			t.Fatalf("first attempt should be aborted by shutdown: %v", a)
		}
	}
}

// Dropping every database connection mid-run: pools reconnect, failed
// reports are retried or recovered, and every workflow completes.
func TestDatabaseConnectionLoss(t *testing.T) {
	e := newEnv(t)
	reg := handler.NewRegistry()
	reg.MustRegister(e.h("x"), handler.Func(func(context.Context, handler.Request) (json.RawMessage, error) {
		time.Sleep(20 * time.Millisecond)
		return json.RawMessage(`{}`), nil
	}))
	reg.MustRegister(e.h("y"), ok(`{}`))
	runWorker(t, reg, workerOpts{concurrency: 4})
	wf := e.publish("dbloss", []step{{"key": "x", "handler": e.h("x"), "retry": fastRetry(5)}, {"key": "y", "handler": e.h("y"), "depends_on": []string{"x"}}})
	var ids []string
	for range 20 {
		ids = append(ids, e.start(wf, nil))
	}
	ctx := context.Background()
	for range 3 {
		time.Sleep(30 * time.Millisecond)
		if _, err := shared.Pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity
			WHERE datname = current_database() AND pid <> pg_backend_pid()`); err != nil {
			t.Logf("terminate: %v", err)
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for _, id := range ids {
		for {
			_, _ = shared.Engine.RecoverAbandoned(ctx, 100)
			r := e.do("GET", "/api/v1/executions/"+id, nil)
			if r.status == 200 && r.json(t)["status"] == "COMPLETED" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("execution %s did not complete after connection loss: %s", id, r.body)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// Duplicate delivery: a task re-run after an expired lease receives the same
// idempotency key, so handlers can deduplicate side effects.
func TestRedeliveryKeepsIdempotencyKey(t *testing.T) {
	e := newEnv(t)
	keys := make(chan string, 4)
	reg := handler.NewRegistry()
	reg.MustRegister(e.h("x"), handler.Func(func(_ context.Context, req handler.Request) (json.RawMessage, error) {
		keys <- req.IdempotencyKey
		if req.Attempt == 1 {
			return nil, errors.New("lost response")
		}
		return json.RawMessage(`{}`), nil
	}))
	runWorker(t, reg, workerOpts{})
	id := e.start(e.publish("redelivery", []step{{"key": "x", "handler": e.h("x"), "retry": fastRetry(3)}}), nil)
	e.waitStatus(id, 10*time.Second, "COMPLETED")
	first, second := <-keys, <-keys
	if first != second || first == "" {
		t.Fatalf("idempotency key must be stable across attempts: %q vs %q", first, second)
	}
}
