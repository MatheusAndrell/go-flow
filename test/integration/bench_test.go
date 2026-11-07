package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/app"
	"github.com/MatheusAndrell/go-flow/internal/application/executions"
	"github.com/MatheusAndrell/go-flow/internal/domain/identity"
	"github.com/MatheusAndrell/go-flow/pkg/handler"
)

// BenchmarkWorkflowThroughput measures end-to-end throughput of a 3-step
// workflow (a -> (b, c)) with no-op handlers: start through the application
// service, run on 4 workers x 16 slots, wait for completion. It reports
// executions/s and tasks/s; the bottleneck is PostgreSQL round trips.
func BenchmarkWorkflowThroughput(b *testing.B) {
	e := newEnv(b)
	reg := handler.NewRegistry()
	for _, n := range []string{"a", "b", "c"} {
		reg.MustRegister(e.h(n), handler.Func(func(context.Context, handler.Request) (json.RawMessage, error) { return nil, nil }))
	}
	var stops []func()
	for range 4 {
		stops = append(stops, runWorker(b, reg, workerOpts{concurrency: 16}))
	}
	defer func() {
		for _, s := range stops {
			s()
		}
	}()
	wf := e.publish(fmt.Sprintf("bench-%d", b.N), []step{
		{"key": "a", "handler": e.h("a")},
		{"key": "b", "handler": e.h("b"), "depends_on": []string{"a"}},
		{"key": "c", "handler": e.h("c"), "depends_on": []string{"a"}},
	})
	svc := executions.NewService(shared.Store, shared.Store, shared.Metrics, app.NewID, time.Now)
	p := identity.Principal{TenantID: e.tenant, Permissions: identity.AllPermissions}
	ctx := context.Background()

	b.ResetTimer()
	start := time.Now()
	for range b.N {
		if _, _, err := svc.Start(ctx, p, executions.StartInput{WorkflowID: wf}); err != nil {
			b.Fatal(err)
		}
	}
	for {
		var pending int
		if err := shared.Pool.QueryRow(ctx, `SELECT count(*) FROM executions WHERE tenant_id = $1 AND status <> 'COMPLETED'`, e.tenant).Scan(&pending); err != nil {
			b.Fatal(err)
		}
		if pending == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	elapsed := time.Since(start)
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "executions/s")
	b.ReportMetric(float64(3*b.N)/elapsed.Seconds(), "tasks/s")
}
