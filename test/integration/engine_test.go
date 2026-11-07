package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MatheusAndrell/go-flow/pkg/handler"
)

func TestDAGExecutionPassesOutputsAndRunsBranchesInParallel(t *testing.T) {
	e := newEnv(t)
	var mu sync.Mutex
	var order []string
	var running, peak atomic.Int32
	record := func(name string, delay time.Duration) handler.Func {
		return func(ctx context.Context, req handler.Request) (json.RawMessage, error) {
			n := running.Add(1)
			defer running.Add(-1)
			for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
			}
			time.Sleep(delay)
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			out, _ := json.Marshal(map[string]any{"step": name, "deps": len(req.Dependencies)})
			return out, nil
		}
	}
	reg := handler.NewRegistry()
	reg.MustRegister(e.h("validate"), record("validate", 0))
	reg.MustRegister(e.h("stock"), record("stock", 200*time.Millisecond))
	reg.MustRegister(e.h("payment"), record("payment", 200*time.Millisecond))
	reg.MustRegister(e.h("finish"), handler.Func(func(_ context.Context, req handler.Request) (json.RawMessage, error) {
		var stock map[string]any
		_ = json.Unmarshal(req.Dependencies["stock"], &stock)
		if stock["step"] != "stock" || len(req.Dependencies) != 2 {
			return nil, handler.Permanentf("dependency outputs missing: %v", req.Dependencies)
		}
		mu.Lock()
		order = append(order, "finish")
		mu.Unlock()
		return json.Marshal(map[string]any{"input": json.RawMessage(req.WorkflowInput)})
	}))
	runWorker(t, reg, workerOpts{concurrency: 4})
	wf := e.publish("dag", []step{
		{"key": "validate", "handler": e.h("validate")},
		{"key": "stock", "handler": e.h("stock"), "depends_on": []string{"validate"}},
		{"key": "payment", "handler": e.h("payment"), "depends_on": []string{"validate"}},
		{"key": "finish", "handler": e.h("finish"), "depends_on": []string{"stock", "payment"}},
	})
	ex := e.waitStatus(e.start(wf, map[string]any{"order": 42}), 10*time.Second, "COMPLETED")
	if order[0] != "validate" || order[3] != "finish" {
		t.Fatalf("dependency order violated: %v", order)
	}
	if peak.Load() < 2 {
		t.Fatal("independent branches should run in parallel")
	}
	out := ex["output"].(map[string]any)
	if out["finish"].(map[string]any)["input"].(map[string]any)["order"].(float64) != 42 || len(out) != 4 {
		t.Fatalf("unexpected output %v", out)
	}
	hist := e.expect(e.do("GET", "/api/v1/executions/"+ex["id"].(string)+"/history?limit=3", nil), 200)
	if len(hist["items"].([]any)) != 3 || hist["next_cursor"] == "" {
		t.Fatal("history must paginate")
	}
}

func TestRetryWithBackoffThenSuccess(t *testing.T) {
	e := newEnv(t)
	var calls atomic.Int32
	var times []time.Time
	var mu sync.Mutex
	reg := handler.NewRegistry()
	reg.MustRegister(e.h("flaky"), handler.Func(func(_ context.Context, req handler.Request) (json.RawMessage, error) {
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		if calls.Add(1) <= 2 {
			return nil, errors.New("temporary outage")
		}
		return json.RawMessage(fmt.Sprintf(`{"attempt":%d}`, req.Attempt)), nil
	}))
	runWorker(t, reg, workerOpts{})
	wf := e.publish("retry", []step{{"key": "flaky", "handler": e.h("flaky"),
		"retry": map[string]any{"max_attempts": 5, "initial_interval": "200ms", "max_interval": "1s", "multiplier": 2, "jitter": 0}}})
	ex := e.waitStatus(e.start(wf, nil), 10*time.Second, "COMPLETED")
	task := e.tasks(ex["id"].(string))["ACTION/flaky"]
	if task["attempts"].(float64) != 3 {
		t.Fatalf("expected 3 attempts, got %v", task["attempts"])
	}
	attempts := e.attempts(task["id"].(string))
	if len(attempts) != 3 || attempts[0]["status"] != "FAILED" || attempts[0]["error_kind"] != "RETRYABLE" || attempts[2]["status"] != "SUCCEEDED" {
		t.Fatalf("attempt history wrong: %v", attempts)
	}
	if gap := times[1].Sub(times[0]); gap < 180*time.Millisecond {
		t.Fatalf("first backoff too short: %s", gap)
	}
	if gap := times[2].Sub(times[1]); gap < 380*time.Millisecond {
		t.Fatalf("second backoff must grow exponentially: %s", gap)
	}
}

func TestTimeoutPanicAndDeadLetterQueue(t *testing.T) {
	e := newEnv(t)
	var panics atomic.Int32
	reg := handler.NewRegistry()
	reg.MustRegister(e.h("slow"), handler.Func(func(ctx context.Context, _ handler.Request) (json.RawMessage, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
			return json.RawMessage(`{}`), nil
		}
	}))
	reg.MustRegister(e.h("panic"), handler.Func(func(context.Context, handler.Request) (json.RawMessage, error) {
		panics.Add(1)
		var m map[string]int
		m["boom"] = 1
		return nil, nil
	}))
	runWorker(t, reg, workerOpts{})
	slow := e.publish("timeout", []step{{"key": "slow", "handler": e.h("slow"), "timeout": "100ms", "retry": fastRetry(2)}})
	pan := e.publish("panic", []step{{"key": "p", "handler": e.h("panic"), "retry": fastRetry(5)}})

	ex := e.waitStatus(e.start(slow, nil), 10*time.Second, "FAILED")
	task := e.tasks(ex["id"].(string))["ACTION/slow"]
	if task["last_error_kind"] != "TIMEOUT" || task["attempts"].(float64) != 2 {
		t.Fatalf("timeouts must be retried then fail: %v", task)
	}
	ex2 := e.waitStatus(e.start(pan, nil), 10*time.Second, "FAILED")
	if panics.Load() != 1 {
		t.Fatalf("panics must not be retried, ran %d times", panics.Load())
	}
	if !strings.Contains(ex2["error"].(string), "panic") {
		t.Fatalf("panic not reported: %v", ex2["error"])
	}
	dl := e.expect(e.do("GET", "/api/v1/dead-letters", nil), 200)["items"].([]any)
	if len(dl) != 2 {
		t.Fatalf("expected 2 dead letters, got %d", len(dl))
	}
	kinds := map[string]bool{}
	for _, it := range dl {
		kinds[it.(map[string]any)["error_kind"].(string)] = true
	}
	if !kinds["TIMEOUT"] || !kinds["PANIC"] {
		t.Fatalf("dead letters %v", dl)
	}
}

func sagaRegistry(e *env, failCompensation bool) (*handler.Registry, *sync.Mutex, *[]string) {
	var mu sync.Mutex
	var log []string
	track := func(name string, err error) handler.Func {
		return func(_ context.Context, req handler.Request) (json.RawMessage, error) {
			mu.Lock()
			log = append(log, name)
			mu.Unlock()
			if err != nil {
				return nil, err
			}
			if req.Compensation && len(req.CompensatedOutput) == 0 {
				return nil, handler.Permanentf("missing compensated output")
			}
			return json.RawMessage(`{"id":"` + name + `"}`), nil
		}
	}
	reg := handler.NewRegistry()
	reg.MustRegister(e.h("reserve"), track("reserve", nil))
	reg.MustRegister(e.h("release"), track("release", nil))
	reg.MustRegister(e.h("charge"), track("charge", nil))
	var refundErr error
	if failCompensation {
		refundErr = errors.New("gateway unavailable")
	}
	reg.MustRegister(e.h("refund"), track("refund", refundErr))
	reg.MustRegister(e.h("invoice"), track("invoice", handler.Permanentf("tax service rejected invoice")))
	return reg, &mu, &log
}

func sagaSteps(e *env) []step {
	return []step{
		{"key": "reserve", "handler": e.h("reserve"), "compensation": map[string]any{"handler": e.h("release"), "retry": fastRetry(3)}},
		{"key": "charge", "handler": e.h("charge"), "depends_on": []string{"reserve"}, "compensation": map[string]any{"handler": e.h("refund"), "retry": fastRetry(3)}},
		{"key": "invoice", "handler": e.h("invoice"), "depends_on": []string{"charge"}},
	}
}

func TestSagaCompensatesInReverseOrder(t *testing.T) {
	e := newEnv(t)
	reg, mu, log := sagaRegistry(e, false)
	runWorker(t, reg, workerOpts{})
	ex := e.waitStatus(e.start(e.publish("saga", sagaSteps(e)), nil), 10*time.Second, "COMPENSATED")
	mu.Lock()
	got := strings.Join(*log, ",")
	mu.Unlock()
	if got != "reserve,charge,invoice,refund,release" {
		t.Fatalf("unexpected saga order: %s", got)
	}
	tasks := e.tasks(ex["id"].(string))
	if tasks["ACTION/reserve"]["status"] != "COMPENSATED" || tasks["ACTION/charge"]["status"] != "COMPENSATED" ||
		tasks["ACTION/invoice"]["status"] != "FAILED" || tasks["COMPENSATION/charge"]["status"] != "COMPLETED" {
		t.Fatalf("unexpected task states: %v", tasks)
	}
	if !strings.Contains(ex["error"].(string), "tax service rejected invoice") {
		t.Fatalf("original failure must be kept: %v", ex["error"])
	}
}

func TestCompensationFailureStopsSagaAndFails(t *testing.T) {
	e := newEnv(t)
	reg, mu, log := sagaRegistry(e, true)
	runWorker(t, reg, workerOpts{})
	ex := e.waitStatus(e.start(e.publish("saga-fail", sagaSteps(e)), nil), 10*time.Second, "FAILED")
	mu.Lock()
	got := strings.Join(*log, ",")
	mu.Unlock()
	if got != "reserve,charge,invoice,refund,refund,refund" {
		t.Fatalf("refund must be retried 3 times and release never run: %s", got)
	}
	if !strings.Contains(ex["error"].(string), "compensation of step \"charge\" failed") {
		t.Fatalf("error %v", ex["error"])
	}
	tasks := e.tasks(ex["id"].(string))
	if tasks["COMPENSATION/reserve"]["status"] != "PENDING" || tasks["ACTION/reserve"]["status"] != "COMPLETED" {
		t.Fatal("remaining compensation must stay pending for manual intervention")
	}
}

func TestScheduledExecutionSurvivesUntilDue(t *testing.T) {
	e := newEnv(t)
	reg := handler.NewRegistry()
	reg.MustRegister(e.h("a"), ok(`{}`))
	runWorker(t, reg, workerOpts{})
	wf := e.publish("scheduled", []step{{"key": "a", "handler": e.h("a")}})
	at := time.Now().Add(1500 * time.Millisecond).UTC()
	ex := e.expect(e.do("POST", "/api/v1/executions", map[string]any{"workflow_id": wf, "scheduled_at": at}), 201)
	id := ex["id"].(string)
	if ex["status"] != "PENDING" {
		t.Fatalf("expected PENDING, got %v", ex["status"])
	}
	if _, err := shared.Engine.ActivateDue(context.Background(), 100); err != nil {
		t.Fatal(err)
	}
	if e.execution(id)["status"] != "PENDING" {
		t.Fatal("execution activated before its time")
	}
	time.Sleep(time.Until(at) + 50*time.Millisecond)
	// A fresh scheduler (as after a restart) picks it up from the database.
	shared.Scheduler().Tick(context.Background())
	e.waitStatus(id, 10*time.Second, "COMPLETED")
}

func TestPauseAndResume(t *testing.T) {
	e := newEnv(t)
	release := make(chan struct{})
	reg := handler.NewRegistry()
	reg.MustRegister(e.h("first"), handler.Func(func(context.Context, handler.Request) (json.RawMessage, error) {
		<-release
		return json.RawMessage(`{}`), nil
	}))
	reg.MustRegister(e.h("second"), ok(`{}`))
	runWorker(t, reg, workerOpts{})
	wf := e.publish("pause", []step{{"key": "first", "handler": e.h("first")}, {"key": "second", "handler": e.h("second"), "depends_on": []string{"first"}}})
	id := e.start(wf, nil)
	waitTask(e, id, "ACTION/first", "RUNNING")
	e.expect(e.do("POST", "/api/v1/executions/"+id+"/pause", nil), 200)
	close(release)
	time.Sleep(500 * time.Millisecond)
	tasks := e.tasks(id)
	if e.execution(id)["status"] != "PAUSED" || tasks["ACTION/first"]["status"] != "COMPLETED" || tasks["ACTION/second"]["status"] != "PENDING" {
		t.Fatalf("paused execution must finish in-flight work but start nothing new: %v", tasks)
	}
	e.expect(e.do("POST", "/api/v1/executions/"+id+"/pause", nil), 409)
	e.expect(e.do("POST", "/api/v1/executions/"+id+"/resume", nil), 200)
	e.waitStatus(id, 5*time.Second, "COMPLETED")
	e.expect(e.do("POST", "/api/v1/executions/"+id+"/resume", nil), 409)
}

func TestCancelStopsRunningHandler(t *testing.T) {
	e := newEnv(t)
	started := make(chan struct{})
	cancelled := make(chan struct{})
	reg := handler.NewRegistry()
	reg.MustRegister(e.h("long"), handler.Func(func(ctx context.Context, _ handler.Request) (json.RawMessage, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return nil, ctx.Err()
	}))
	runWorker(t, reg, workerOpts{lease: time.Second, heartbeat: 100 * time.Millisecond})
	id := e.start(e.publish("cancel", []step{{"key": "long", "handler": e.h("long"), "timeout": "1m"}}), nil)
	<-started
	e.expect(e.do("POST", "/api/v1/executions/"+id+"/cancel", map[string]any{"reason": "customer request"}), 200)
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("handler context was not cancelled")
	}
	time.Sleep(200 * time.Millisecond)
	ex := e.execution(id)
	task := e.tasks(id)["ACTION/long"]
	if ex["status"] != "CANCELLED" || task["status"] != "CANCELLED" {
		t.Fatalf("got %v / %v", ex["status"], task["status"])
	}
	if a := e.attempts(task["id"].(string)); a[0]["status"] != "CANCELLED" {
		t.Fatalf("attempt %v", a)
	}
	e.expect(e.do("POST", "/api/v1/executions/"+id+"/cancel", nil), 409)
}

func TestCancelWithCompensation(t *testing.T) {
	e := newEnv(t)
	block := make(chan struct{})
	reg := handler.NewRegistry()
	reg.MustRegister(e.h("reserve"), ok(`{"r":1}`))
	reg.MustRegister(e.h("release"), ok(`{}`))
	reg.MustRegister(e.h("wait"), handler.Func(func(ctx context.Context, _ handler.Request) (json.RawMessage, error) {
		select {
		case <-block:
		case <-ctx.Done():
		}
		return nil, ctx.Err()
	}))
	runWorker(t, reg, workerOpts{lease: time.Second})
	id := e.start(e.publish("cancel-comp", []step{
		{"key": "reserve", "handler": e.h("reserve"), "compensation": map[string]any{"handler": e.h("release")}},
		{"key": "wait", "handler": e.h("wait"), "depends_on": []string{"reserve"}},
	}), nil)
	waitTask(e, id, "ACTION/wait", "RUNNING")
	e.expect(e.do("POST", "/api/v1/executions/"+id+"/cancel", map[string]any{"compensate": true}), 200)
	e.waitStatus(id, 5*time.Second, "COMPENSATED")
	close(block)
}

func waitTask(e *env, execID, key, status string) {
	e.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for e.tasks(execID)[key]["status"] != status {
		if time.Now().After(deadline) {
			e.t.Fatalf("task %s did not reach %s", key, status)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
