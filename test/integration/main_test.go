// Package integration exercises GoFlow end to end against a real PostgreSQL.
// Set GOFLOW_TEST_DATABASE_URL to a database the tests may create other
// databases from (e.g. postgres://postgres:postgres@localhost:5432/postgres);
// the tests are skipped otherwise.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/MatheusAndrell/go-flow/internal/app"
	"github.com/MatheusAndrell/go-flow/internal/application/worker"
	"github.com/MatheusAndrell/go-flow/internal/platform/config"
	"github.com/MatheusAndrell/go-flow/pkg/handler"
)

var (
	shared  *app.App
	server  *httptest.Server
	dbURL   string
	counter atomic.Int64
	logger  = slog.New(slog.NewTextHandler(io.Discard, nil))
)

func TestMain(m *testing.M) {
	base := os.Getenv("GOFLOW_TEST_DATABASE_URL")
	if base == "" {
		fmt.Println("GOFLOW_TEST_DATABASE_URL not set; skipping integration tests")
		os.Exit(0)
	}
	if os.Getenv("GOFLOW_TEST_VERBOSE") != "" {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	ctx := context.Background()
	name := fmt.Sprintf("goflow_it_%d", time.Now().UnixNano())
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		fmt.Println("connect:", err)
		os.Exit(1)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		fmt.Println("create database:", err)
		os.Exit(1)
	}
	u, _ := url.Parse(base)
	u.Path = "/" + name
	dbURL = u.String()

	cfg := config.Config{
		DatabaseURL: dbURL, DatabaseMaxConns: 60, AutoMigrate: true,
		RateLimitRPS: 10_000, RateLimitBurst: 10_000, AuthCacheTTL: time.Second, IdempotencyTTL: time.Hour,
		MaxRequestBytes: 1 << 20, WorkerConcurrency: 8, WorkerPollInterval: 20 * time.Millisecond,
		WorkerLeaseDuration: 2 * time.Second, WorkerHeartbeatInterval: 200 * time.Millisecond,
		ShutdownTimeout: 5 * time.Second, TraceSampleRatio: 1,
	}
	shared, err = app.New(ctx, cfg, logger)
	if err != nil {
		fmt.Println("app:", err)
		os.Exit(1)
	}
	server = httptest.NewServer(shared.HTTPHandler())
	code := m.Run()
	server.Close()
	shared.Close(ctx)
	_, _ = admin.Exec(ctx, "DROP DATABASE "+name+" WITH (FORCE)")
	_ = admin.Close(ctx)
	os.Exit(code)
}

// env isolates a test: its own tenant, API key and handler-name prefix, so
// workers of concurrent tests never claim each other's tasks.
type env struct {
	t      testing.TB
	tenant string
	key    string
	prefix string
}

func newEnv(t testing.TB, perms ...string) *env {
	t.Helper()
	n := counter.Add(1)
	tenantName := fmt.Sprintf("%s-%d", t.Name(), n)
	ten, err := shared.Admin.EnsureTenant(context.Background(), tenantName)
	if err != nil {
		t.Fatal(err)
	}
	if len(perms) == 0 {
		perms = []string{"workflows:read", "workflows:write", "executions:read", "executions:write"}
	}
	secret, _, err := shared.Admin.CreateAPIKey(context.Background(), ten.ID, "test", perms)
	if err != nil {
		t.Fatal(err)
	}
	return &env{t: t, tenant: ten.ID, key: secret, prefix: fmt.Sprintf("it%d", n)}
}

func (e *env) h(name string) string { return e.prefix + "." + name }

type response struct {
	status int
	header http.Header
	body   []byte
}

func (r response) json(t testing.TB) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(r.body, &m); err != nil {
		t.Fatalf("decode %s: %v", r.body, err)
	}
	return m
}

func (e *env) do(method, path string, body any, headers ...string) response {
	e.t.Helper()
	var rdr io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rdr = strings.NewReader(b)
	default:
		data, err := json.Marshal(b)
		if err != nil {
			e.t.Fatal(err)
		}
		rdr = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, server.URL+path, rdr)
	if err != nil {
		e.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+e.key)
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return response{status: resp.StatusCode, header: resp.Header, body: data}
}

func (e *env) expect(r response, status int) map[string]any {
	e.t.Helper()
	if r.status != status {
		e.t.Fatalf("expected HTTP %d, got %d: %s", status, r.status, r.body)
	}
	if len(r.body) == 0 {
		return nil
	}
	return r.json(e.t)
}

type step = map[string]any

// publish creates and publishes a workflow. Handlers must be registered by a running worker.
func (e *env) publish(name string, steps []step, settings ...map[string]any) string {
	e.t.Helper()
	spec := map[string]any{"steps": steps}
	if len(settings) > 0 {
		spec["settings"] = settings[0]
	}
	wf := e.expect(e.do("POST", "/api/v1/workflows", map[string]any{"name": name, "spec": spec}), 201)
	id := wf["id"].(string)
	e.expect(e.do("POST", "/api/v1/workflows/"+id+"/publish", nil), 200)
	return id
}

func (e *env) start(workflowID string, input any) string {
	e.t.Helper()
	ex := e.expect(e.do("POST", "/api/v1/executions", map[string]any{"workflow_id": workflowID, "input": input}), 201)
	return ex["id"].(string)
}

func (e *env) execution(id string) map[string]any {
	e.t.Helper()
	return e.expect(e.do("GET", "/api/v1/executions/"+id, nil), 200)
}

func (e *env) waitStatus(id string, timeout time.Duration, statuses ...string) map[string]any {
	e.t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ex := e.execution(id)
		for _, s := range statuses {
			if ex["status"] == s {
				return ex
			}
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("execution %s did not reach %v within %s; last: %v", id, statuses, timeout, ex)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func (e *env) tasks(execID string) map[string]map[string]any {
	e.t.Helper()
	list := e.expect(e.do("GET", "/api/v1/executions/"+execID+"/tasks", nil), 200)
	out := map[string]map[string]any{}
	for _, it := range list["items"].([]any) {
		task := it.(map[string]any)
		out[task["kind"].(string)+"/"+task["step_key"].(string)] = task
	}
	return out
}

func (e *env) attempts(taskID string) []map[string]any {
	e.t.Helper()
	list := e.expect(e.do("GET", "/api/v1/tasks/"+taskID+"/attempts", nil), 200)
	var out []map[string]any
	for _, it := range list["items"].([]any) {
		out = append(out, it.(map[string]any))
	}
	return out
}

type workerOpts struct {
	concurrency int
	lease       time.Duration
	heartbeat   time.Duration
	shutdown    time.Duration
	id          string
}

// runWorker starts a worker for reg and returns a function that stops it and
// waits for the drain to complete.
func runWorker(t testing.TB, reg *handler.Registry, o workerOpts) func() {
	t.Helper()
	if o.concurrency == 0 {
		o.concurrency = 8
	}
	if o.lease == 0 {
		o.lease = 2 * time.Second
	}
	if o.heartbeat == 0 {
		o.heartbeat = o.lease / 5
	}
	if o.shutdown == 0 {
		o.shutdown = 5 * time.Second
	}
	w := worker.New(worker.Config{
		ID: o.id, Concurrency: o.concurrency, PollInterval: 20 * time.Millisecond, LeaseDuration: o.lease,
		HeartbeatInterval: o.heartbeat, ShutdownTimeout: o.shutdown,
	}, shared.Store, shared.Engine, reg, shared.Metrics, noop.NewTracerProvider().Tracer("it"), logger)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	waitRegistered(t, w.ID())
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		if err := <-done; err != nil {
			t.Errorf("worker: %v", err)
		}
	}
	t.Cleanup(stop)
	return stop
}

func waitRegistered(t testing.TB, workerID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		_ = shared.Pool.QueryRow(context.Background(), `SELECT count(*) FROM workers WHERE id = $1 AND status = 'ACTIVE'`, workerID).Scan(&n)
		if n == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("worker did not register")
}

func fastRetry(attempts int) map[string]any {
	return map[string]any{"max_attempts": attempts, "initial_interval": "50ms", "max_interval": "200ms", "multiplier": 2, "jitter": 0}
}

func ok(output string) handler.Func {
	return func(context.Context, handler.Request) (json.RawMessage, error) { return json.RawMessage(output), nil }
}
