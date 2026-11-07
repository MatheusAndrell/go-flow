package integration

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/application/scheduler"
	"github.com/MatheusAndrell/go-flow/internal/platform/telemetry"
	"github.com/MatheusAndrell/go-flow/pkg/goflow"
	"github.com/MatheusAndrell/go-flow/pkg/handler"
)

// An application embeds a worker through the public SDK.
func TestEmbeddedWorkerThroughSDK(t *testing.T) {
	e := newEnv(t)
	reg := handler.NewRegistry()
	reg.MustRegister(e.h("sdk"), handler.Func(func(_ context.Context, req handler.Request) (json.RawMessage, error) {
		return json.RawMessage(`{"from":"sdk","step":"` + req.StepKey + `"}`), nil
	}))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- goflow.RunWorker(ctx, reg, goflow.WorkerOptions{
			DatabaseURL: dbURL, WorkerID: e.prefix + "-sdk", Concurrency: 2, PollInterval: 20 * time.Millisecond, Logger: logger,
		})
	}()
	waitRegistered(t, e.prefix+"-sdk")
	ex := e.waitStatus(e.start(e.publish("sdk", []step{{"key": "s", "handler": e.h("sdk")}}), nil), 10*time.Second, "COMPLETED")
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if out := ex["output"].(map[string]any)["s"].(map[string]any); out["from"] != "sdk" {
		t.Fatalf("unexpected output %v", out)
	}
	if err := goflow.RunWorker(context.Background(), reg, goflow.WorkerOptions{}); err == nil {
		t.Fatal("missing database url must be rejected")
	}
}

func TestSchedulerHousekeepingAndStats(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t)
	if _, err := shared.Pool.Exec(ctx, `INSERT INTO idempotency_keys (tenant_id, key, fingerprint, state, expires_at)
		VALUES ($1, 'old', '\x00', 'COMPLETED', now() - interval '1 hour')`, e.tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := shared.Pool.Exec(ctx, `INSERT INTO workers (id, hostname, handlers, concurrency, status, started_at, last_heartbeat_at)
		VALUES ($1, 'h', '{}', 1, 'ACTIVE', now() - interval '1 hour', now() - interval '1 hour')`, e.prefix+"-ghost"); err != nil {
		t.Fatal(err)
	}
	s := scheduler.New(scheduler.Config{Interval: 20 * time.Millisecond, HousekeepingEvery: 20 * time.Millisecond, StatsInterval: 20 * time.Millisecond},
		shared.Engine, shared.Store, telemetry.NewStatsCollector(shared.Metrics, shared.Store), logger)
	runCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	s.Run(runCtx)

	var keys int
	_ = shared.Pool.QueryRow(ctx, `SELECT count(*) FROM idempotency_keys WHERE tenant_id = $1`, e.tenant).Scan(&keys)
	var status string
	_ = shared.Pool.QueryRow(ctx, `SELECT status FROM workers WHERE id = $1`, e.prefix+"-ghost").Scan(&status)
	if keys != 0 || status != "STOPPED" {
		t.Fatalf("housekeeping did not run: keys=%d worker=%s", keys, status)
	}
	m := e.do("GET", "/metrics", nil)
	if !strings.Contains(string(m.body), `goflow_executions_active{status="RUNNING"}`) {
		t.Fatal("stats collector must publish active execution gauges")
	}
}
