package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/application/engine"
	"github.com/MatheusAndrell/go-flow/internal/domain/execution"
	"github.com/MatheusAndrell/go-flow/pkg/handler"
)

// Without failures, at-least-once delivery degenerates to exactly-once: no
// task may be executed twice even with many competing workers.
func TestManyWorkersExecuteEachTaskOnce(t *testing.T) {
	e := newEnv(t)
	var mu sync.Mutex
	seen := map[string]int{}
	count := func(context.Context, handler.Request) (json.RawMessage, error) { return nil, nil }
	reg := handler.NewRegistry()
	for _, name := range []string{"a", "b", "c", "d"} {
		reg.MustRegister(e.h(name), handler.Func(func(ctx context.Context, req handler.Request) (json.RawMessage, error) {
			mu.Lock()
			seen[req.IdempotencyKey]++
			mu.Unlock()
			time.Sleep(2 * time.Millisecond)
			return count(ctx, req)
		}))
	}
	for i := range 4 {
		runWorker(t, reg, workerOpts{concurrency: 6, id: fmt.Sprintf("%s-w%d", e.prefix, i)})
	}
	wf := e.publish("fanout", []step{
		{"key": "a", "handler": e.h("a")},
		{"key": "b", "handler": e.h("b"), "depends_on": []string{"a"}},
		{"key": "c", "handler": e.h("c"), "depends_on": []string{"a"}},
		{"key": "d", "handler": e.h("d"), "depends_on": []string{"b", "c"}},
	})
	const executions = 60
	ids := make([]string, executions)
	var wg sync.WaitGroup
	for i := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids[i] = e.start(wf, map[string]any{"i": i})
		}()
	}
	wg.Wait()
	for _, id := range ids {
		e.waitStatus(id, 30*time.Second, "COMPLETED")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != executions*4 {
		t.Fatalf("expected %d distinct task executions, got %d", executions*4, len(seen))
	}
	for key, n := range seen {
		if n != 1 {
			t.Fatalf("task %s executed %d times", key, n)
		}
	}
	var workers int
	_ = shared.Pool.QueryRow(context.Background(), `SELECT count(DISTINCT worker_id) FROM task_attempts WHERE tenant_id = $1`, e.tenant).Scan(&workers)
	if workers < 2 {
		t.Fatalf("work should be spread across workers, used %d", workers)
	}
}

// Competing claimers on the same queue must never receive the same task.
func TestConcurrentClaimsNeverOverlap(t *testing.T) {
	e := newEnv(t)
	reg := handler.NewRegistry()
	reg.MustRegister(e.h("x"), ok(`{}`))
	stop := runWorker(t, reg, workerOpts{})
	steps := make([]step, 20)
	for i := range steps {
		steps[i] = step{"key": fmt.Sprintf("s%d", i), "handler": e.h("x")}
	}
	wf := e.publish("claims", steps)
	stop()
	for range 5 {
		e.start(wf, nil)
	}
	var mu sync.Mutex
	claimed := map[string]int{}
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				tasks, err := shared.Store.Claim(context.Background(), fmt.Sprintf("claimer-%d", i), []string{e.h("x")}, 3, time.Minute)
				if err != nil {
					t.Error(err)
					return
				}
				if len(tasks) == 0 {
					return
				}
				mu.Lock()
				for _, tk := range tasks {
					claimed[tk.TaskID]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(claimed) != 100 {
		t.Fatalf("expected 100 claimed tasks, got %d", len(claimed))
	}
	for id, n := range claimed {
		if n != 1 {
			t.Fatalf("task %s claimed %d times", id, n)
		}
	}
}

// Cancelling while workers report results must leave every execution in a
// single consistent terminal state.
func TestCancelRacingWithCompletion(t *testing.T) {
	e := newEnv(t)
	reg := handler.NewRegistry()
	reg.MustRegister(e.h("x"), ok(`{}`))
	stop := runWorker(t, reg, workerOpts{})
	wf := e.publish("race", []step{{"key": "x", "handler": e.h("x")}})
	stop()

	const n = 30
	ids := make([]string, n)
	for i := range ids {
		ids[i] = e.start(wf, nil)
	}
	leases := map[string]engine.ClaimedTask{}
	for len(leases) < n {
		tasks, err := shared.Store.Claim(context.Background(), "racer", []string{e.h("x")}, n, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		for _, tk := range tasks {
			leases[tk.ExecutionID] = tk
		}
	}
	var completed, cancelled atomic.Int32
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(2)
		go func() {
			defer wg.Done()
			err := shared.Engine.CompleteTask(context.Background(), leases[id].Lease, json.RawMessage(`{}`))
			switch {
			case err == nil:
				completed.Add(1)
			case !errors.Is(err, execution.ErrLeaseLost):
				t.Errorf("complete: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			r := e.do("POST", "/api/v1/executions/"+id+"/cancel", nil)
			switch r.status {
			case 200:
				cancelled.Add(1)
			case 409:
			default:
				t.Errorf("cancel: %d %s", r.status, r.body)
			}
		}()
	}
	wg.Wait()
	if completed.Load()+cancelled.Load() != n {
		t.Fatalf("exactly one of complete/cancel must win per execution: %d + %d", completed.Load(), cancelled.Load())
	}
	for _, id := range ids {
		ex := e.execution(id)
		task := e.tasks(id)["ACTION/x"]
		switch ex["status"] {
		case "COMPLETED":
			if task["status"] != "COMPLETED" {
				t.Fatalf("inconsistent: %v / %v", ex["status"], task["status"])
			}
		case "CANCELLED":
			if task["status"] != "CANCELLED" {
				t.Fatalf("inconsistent: %v / %v", ex["status"], task["status"])
			}
		default:
			t.Fatalf("unexpected status %v", ex["status"])
		}
	}
}

// Parallel branches completing at the same time must not lose updates.
func TestParallelCompletionsOfOneExecution(t *testing.T) {
	e := newEnv(t)
	reg := handler.NewRegistry()
	for i := range 10 {
		reg.MustRegister(e.h(fmt.Sprintf("p%d", i)), ok(`{}`))
	}
	reg.MustRegister(e.h("join"), handler.Func(func(_ context.Context, req handler.Request) (json.RawMessage, error) {
		if len(req.Dependencies) != 10 {
			return nil, handler.Permanentf("expected 10 dependencies, got %d", len(req.Dependencies))
		}
		return json.RawMessage(`{}`), nil
	}))
	runWorker(t, reg, workerOpts{concurrency: 10})
	runWorker(t, reg, workerOpts{concurrency: 10})
	steps := []step{}
	deps := []string{}
	for i := range 10 {
		steps = append(steps, step{"key": fmt.Sprintf("p%d", i), "handler": e.h(fmt.Sprintf("p%d", i))})
		deps = append(deps, fmt.Sprintf("p%d", i))
	}
	steps = append(steps, step{"key": "join", "handler": e.h("join"), "depends_on": deps})
	wf := e.publish("parallel", steps)
	for range 10 {
		e.waitStatus(e.start(wf, nil), 20*time.Second, "COMPLETED")
	}
}
