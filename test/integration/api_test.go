package integration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/infrastructure/postgres"
	"github.com/MatheusAndrell/go-flow/migrations"
	"github.com/MatheusAndrell/go-flow/pkg/handler"
)

func TestWorkflowDefinitionLifecycle(t *testing.T) {
	e := newEnv(t)
	steps := []step{{"key": "a", "handler": e.h("a")}, {"key": "b", "handler": e.h("b"), "depends_on": []string{"a"}}}
	wf := e.expect(e.do("POST", "/api/v1/workflows", map[string]any{"name": "lifecycle", "description": "d", "spec": map[string]any{"steps": steps}}), 201)
	id := wf["id"].(string)

	r := e.expect(e.do("POST", "/api/v1/workflows/"+id+"/publish", nil), 422)
	if !strings.Contains(fmt.Sprint(r["errors"]), "not registered by any worker") {
		t.Fatalf("publishing unknown handlers must fail: %v", r)
	}
	e.expect(e.do("POST", "/api/v1/executions", map[string]any{"workflow_id": id}), 400)

	reg := handler.NewRegistry()
	reg.MustRegister(e.h("a"), ok(`{"v":1}`))
	reg.MustRegister(e.h("b"), ok(`{"v":2}`))
	runWorker(t, reg, workerOpts{})

	e.expect(e.do("PUT", "/api/v1/workflows/"+id, map[string]any{"description": "updated"}), 200)
	v1 := e.expect(e.do("POST", "/api/v1/workflows/"+id+"/publish", nil), 200)
	if v1["status"] != "PUBLISHED" || v1["number"].(float64) != 1 {
		t.Fatalf("unexpected version %v", v1)
	}
	e.expect(e.do("PUT", "/api/v1/workflows/"+id, map[string]any{"spec": map[string]any{"steps": steps}}), 409)
	e.expect(e.do("POST", "/api/v1/workflows/"+id+"/publish", nil), 409)

	execV1 := e.start(id, map[string]any{"n": 1})
	e.waitStatus(execV1, 10*time.Second, "COMPLETED")

	// Version 2 drops step b. The v1 execution must stay bound to v1.
	v2 := e.expect(e.do("POST", "/api/v1/workflows/"+id+"/versions", map[string]any{"spec": map[string]any{"steps": steps[:1]}}), 201)
	if v2["number"].(float64) != 2 || v2["status"] != "DRAFT" {
		t.Fatalf("unexpected draft %v", v2)
	}
	e.expect(e.do("POST", "/api/v1/workflows/"+id+"/versions", nil), 409)
	e.expect(e.do("POST", "/api/v1/workflows/"+id+"/publish", nil), 200)
	execV2 := e.start(id, nil)
	e.waitStatus(execV2, 10*time.Second, "COMPLETED")
	if len(e.tasks(execV1)) != 2 || len(e.tasks(execV2)) != 1 {
		t.Fatal("executions must keep the version they started with")
	}
	pinned := e.expect(e.do("POST", "/api/v1/executions", map[string]any{"workflow_id": id, "version": 1}), 201)
	if pinned["version"].(float64) != 1 {
		t.Fatal("explicit version not honoured")
	}

	got := e.expect(e.do("GET", "/api/v1/workflows/"+id, nil), 200)
	if got["description"] != "updated" || got["latest_published_version"].(float64) != 2 || len(got["versions"].([]any)) != 2 {
		t.Fatalf("unexpected workflow %v", got)
	}
}

func TestPublishedVersionIsImmutableInDatabase(t *testing.T) {
	e := newEnv(t)
	reg := handler.NewRegistry()
	reg.MustRegister(e.h("a"), ok(`{}`))
	runWorker(t, reg, workerOpts{})
	id := e.publish("immutable", []step{{"key": "a", "handler": e.h("a")}})
	_, err := shared.Pool.Exec(context.Background(), `UPDATE workflow_versions SET spec = '{}' WHERE definition_id = $1`, id)
	if err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("trigger must block changes to published versions, got %v", err)
	}
}

func TestSpecValidationErrors(t *testing.T) {
	e := newEnv(t)
	r := e.expect(e.do("POST", "/api/v1/workflows", map[string]any{"name": "cyclic", "spec": map[string]any{"steps": []step{
		{"key": "a", "handler": "x.a", "depends_on": []string{"b"}},
		{"key": "b", "handler": "x.b", "depends_on": []string{"a"}},
	}}}), 422)
	if !strings.Contains(fmt.Sprint(r["errors"]), "cycle") {
		t.Fatalf("cycle not reported: %v", r)
	}
	e.expect(e.do("POST", "/api/v1/workflows", `{"name":"x","spec":{},"unknown":1}`), 400)
	e.expect(e.do("POST", "/api/v1/workflows", `{"name":`), 400)
	e.expect(e.do("POST", "/api/v1/workflows", map[string]any{"name": "big", "spec": map[string]any{"steps": []step{{"key": "a", "handler": "x.a", "params": map[string]string{"blob": strings.Repeat("x", 2<<20)}}}}}), 413)
}

func TestTenantIsolation(t *testing.T) {
	a, b := newEnv(t), newEnv(t)
	reg := handler.NewRegistry()
	reg.MustRegister(a.h("a"), ok(`{"secret":"a"}`))
	runWorker(t, reg, workerOpts{})
	wf := a.publish("isolated", []step{{"key": "a", "handler": a.h("a")}})
	ex := a.start(wf, nil)
	a.waitStatus(ex, 10*time.Second, "COMPLETED")
	taskID := a.tasks(ex)["ACTION/a"]["id"].(string)

	for _, path := range []string{"/api/v1/workflows/" + wf, "/api/v1/executions/" + ex, "/api/v1/executions/" + ex + "/tasks",
		"/api/v1/executions/" + ex + "/history", "/api/v1/tasks/" + taskID, "/api/v1/tasks/" + taskID + "/attempts"} {
		b.expect(b.do("GET", path, nil), 404)
	}
	b.expect(b.do("POST", "/api/v1/executions", map[string]any{"workflow_id": wf}), 404)
	b.expect(b.do("POST", "/api/v1/executions/"+ex+"/cancel", nil), 404)
	if list := b.expect(b.do("GET", "/api/v1/executions", nil), 200); len(list["items"].([]any)) != 0 {
		t.Fatal("tenant b must not list tenant a executions")
	}
	if list := b.expect(b.do("GET", "/api/v1/workflows", nil), 200); len(list["items"].([]any)) != 0 {
		t.Fatal("tenant b must not list tenant a workflows")
	}
	a.expect(a.do("GET", "/api/v1/executions/not-a-uuid", nil), 404)
}

func TestAuthenticationAndPermissions(t *testing.T) {
	reader := newEnv(t, "workflows:read", "executions:read")
	reader.expect(reader.do("GET", "/api/v1/workflows", nil), 200)
	reader.expect(reader.do("POST", "/api/v1/workflows", map[string]any{"name": "x", "spec": map[string]any{}}), 403)
	reader.expect(reader.do("POST", "/api/v1/executions", map[string]any{"workflow_id": "x"}), 403)

	anon := &env{t: t, key: "gf_invalid_key_that_does_not_exist"}
	anon.expect(anon.do("GET", "/api/v1/workflows", nil), 401)

	resp := reader.do("GET", "/health", nil)
	if resp.status != 200 {
		t.Fatal("health must be public")
	}
	reader.expect(reader.do("GET", "/ready", nil), 200)
	if m := reader.do("GET", "/metrics", nil); m.status != 200 || !strings.Contains(string(m.body), "goflow_http_requests_total") {
		t.Fatal("metrics endpoint must expose goflow metrics")
	}
}

func TestStartIsIdempotent(t *testing.T) {
	e := newEnv(t)
	reg := handler.NewRegistry()
	reg.MustRegister(e.h("a"), ok(`{}`))
	runWorker(t, reg, workerOpts{})
	wf := e.publish("idem", []step{{"key": "a", "handler": e.h("a")}})
	body := map[string]any{"workflow_id": wf, "input": map[string]any{"order": 1}}

	var wg sync.WaitGroup
	ids := make([]string, 10)
	codes := make([]int, 10)
	for i := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := e.do("POST", "/api/v1/executions", body, "Idempotency-Key", "order-1")
			codes[i] = r.status
			if r.status < 300 {
				ids[i] = r.json(t)["id"].(string)
			}
		}()
	}
	wg.Wait()
	created := ""
	for i, id := range ids {
		switch codes[i] {
		case 200, 201:
			if created == "" {
				created = id
			}
			if id != created {
				t.Fatalf("same key produced different executions: %v", ids)
			}
		case 409:
			// concurrent duplicate while the first was in flight
		default:
			t.Fatalf("unexpected status %d", codes[i])
		}
	}
	var n int
	_ = shared.Pool.QueryRow(context.Background(), `SELECT count(*) FROM executions WHERE tenant_id = $1`, e.tenant).Scan(&n)
	if n != 1 {
		t.Fatalf("expected exactly one execution, got %d", n)
	}
	replay := e.do("POST", "/api/v1/executions", body, "Idempotency-Key", "order-1")
	if replay.status != 201 || replay.header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("expected replayed 201, got %d %v", replay.status, replay.header)
	}
	e.expect(e.do("POST", "/api/v1/executions", map[string]any{"workflow_id": wf, "input": map[string]any{"order": 2}}, "Idempotency-Key", "order-1"), 422)
}

func TestPaginationAndFilters(t *testing.T) {
	e := newEnv(t)
	reg := handler.NewRegistry()
	reg.MustRegister(e.h("a"), ok(`{}`))
	runWorker(t, reg, workerOpts{})
	wf := e.publish("paged", []step{{"key": "a", "handler": e.h("a")}})
	for i := range 5 {
		e.start(wf, map[string]any{"i": i})
	}
	seen := map[string]bool{}
	cursor := ""
	pages := 0
	for {
		r := e.expect(e.do("GET", "/api/v1/executions?limit=2&cursor="+cursor, nil), 200)
		for _, it := range r["items"].([]any) {
			seen[it.(map[string]any)["id"].(string)] = true
		}
		pages++
		next, _ := r["next_cursor"].(string)
		if next == "" {
			break
		}
		cursor = next
	}
	if len(seen) != 5 || pages != 3 {
		t.Fatalf("pagination returned %d items in %d pages", len(seen), pages)
	}
	e.expect(e.do("GET", "/api/v1/executions?limit=1000", nil), 400)
	e.expect(e.do("GET", "/api/v1/executions?status=BOGUS", nil), 400)
	e.expect(e.do("GET", "/api/v1/executions?cursor=not-base64!", nil), 400)
	time.Sleep(500 * time.Millisecond)
	r := e.expect(e.do("GET", "/api/v1/executions?status=COMPLETED&workflow_id="+wf, nil), 200)
	if len(r["items"].([]any)) == 0 {
		t.Fatal("status filter returned nothing")
	}
}

func TestMigrationsAreIdempotent(t *testing.T) {
	if err := postgres.Migrate(context.Background(), shared.Pool, migrations.FS, logger); err != nil {
		t.Fatal(err)
	}
	if err := shared.Store.MigrationsApplied(context.Background(), migrations.FS); err != nil {
		t.Fatal(err)
	}
}
