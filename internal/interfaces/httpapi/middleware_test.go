package httpapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/MatheusAndrell/go-flow/internal/application"
	"github.com/MatheusAndrell/go-flow/internal/application/idempotency"
	"github.com/MatheusAndrell/go-flow/internal/domain/execution"
	"github.com/MatheusAndrell/go-flow/internal/domain/identity"
	"github.com/MatheusAndrell/go-flow/internal/domain/workflow"
)

func TestRateLimiterTokenBucket(t *testing.T) {
	now := time.Unix(0, 0)
	l := NewRateLimiter(2, 3, func() time.Time { return now })
	for i := range 3 {
		if ok, _ := l.Allow("a"); !ok {
			t.Fatalf("burst request %d rejected", i)
		}
	}
	ok, wait := l.Allow("a")
	if ok || wait != 500*time.Millisecond {
		t.Fatalf("expected rejection with 500ms wait, got %v %s", ok, wait)
	}
	if ok, _ := l.Allow("b"); !ok {
		t.Fatal("keys must be isolated")
	}
	now = now.Add(500 * time.Millisecond)
	if ok, _ := l.Allow("a"); !ok {
		t.Fatal("token must be refilled")
	}
}

func TestProblemMapping(t *testing.T) {
	cases := map[error]int{
		identity.ErrUnauthenticated:                         401,
		fmt.Errorf("x: %w", identity.ErrForbidden):          403,
		application.ErrNotFound:                             404,
		&workflow.ValidationError{Problems: []string{"x"}}:  422,
		application.Invalid("bad"):                          400,
		fmt.Errorf("%w: a", execution.ErrInvalidTransition): 409,
		fmt.Errorf("%w: name", application.ErrConflict):     409,
		workflow.ErrDraftExists:                             409,
		errors.New("boom"):                                  500,
	}
	for err, want := range cases {
		if got := problemFor(err).Status; got != want {
			t.Errorf("%v: got %d want %d", err, got, want)
		}
	}
	if p := problemFor(errors.New("secret dsn postgres://u:p@h")); p.Detail != "" {
		t.Fatal("internal errors must not leak details")
	}
}

type memIdem struct {
	mu   sync.Mutex
	recs map[string]*struct {
		fp     []byte
		done   bool
		status int
		body   []byte
	}
}

func (m *memIdem) BeginIdempotent(_ context.Context, tenant, key string, fp []byte, _ time.Duration) (idempotency.Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := tenant + "/" + key
	r, ok := m.recs[k]
	if !ok {
		m.recs[k] = &struct {
			fp     []byte
			done   bool
			status int
			body   []byte
		}{fp: fp}
		return idempotency.Record{State: idempotency.StateNew}, nil
	}
	switch {
	case !bytes.Equal(r.fp, fp):
		return idempotency.Record{State: idempotency.StateMismatch}, nil
	case !r.done:
		return idempotency.Record{State: idempotency.StateInProgress}, nil
	}
	return idempotency.Record{State: idempotency.StateCompleted, Status: r.status, Body: r.body}, nil
}

func (m *memIdem) CompleteIdempotent(_ context.Context, tenant, key string, status int, body []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.recs[tenant+"/"+key]
	r.done, r.status, r.body = true, status, body
	return nil
}

func (m *memIdem) ReleaseIdempotent(_ context.Context, tenant, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.recs, tenant+"/"+key)
	return nil
}

type staticAuth struct{}

func (staticAuth) Authenticate(_ context.Context, token string) (identity.Principal, error) {
	switch token {
	case "tenant-a":
		return identity.Principal{TenantID: "a", Permissions: identity.AllPermissions}, nil
	case "tenant-b":
		return identity.Principal{TenantID: "b", Permissions: identity.AllPermissions}, nil
	}
	return identity.Principal{}, identity.ErrUnauthenticated
}

type nopMetrics struct{}

func (nopMetrics) HTTPRequest(string, string, int, time.Duration) {}
func (nopMetrics) RateLimited()                                   {}

func testServer(rps float64, burst int) *Server {
	return NewServer(Deps{
		Auth: staticAuth{}, Idempotency: &memIdem{recs: map[string]*struct {
			fp     []byte
			done   bool
			status int
			body   []byte
		}{}},
		IdempotencyTTL: time.Hour, Limiter: NewRateLimiter(rps, burst, time.Now), Metrics: nopMetrics{},
		Registry: prometheus.NewRegistry(), Tracer: noop.NewTracerProvider().Tracer("t"),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Ready: func(context.Context) error { return nil },
	})
}

func do(t *testing.T, h http.Handler, token, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/x", bytes.NewBufferString(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		req.Header.Set(headerIdempotency, key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestIdempotencyReplaysAndRejectsReuse(t *testing.T) {
	s := testServer(1000, 1000)
	var calls atomic.Int32
	h := s.correlate(s.authenticate(s.idempotent(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := calls.Add(1)
		writeJSON(w, http.StatusCreated, map[string]int32{"call": n})
	}))))

	first := do(t, h, "tenant-a", "k1", `{"a":1}`)
	second := do(t, h, "tenant-a", "k1", `{"a":1}`)
	if first.Code != 201 || second.Code != 201 || first.Body.String() != second.Body.String() {
		t.Fatalf("replay mismatch: %d %s / %d %s", first.Code, first.Body, second.Code, second.Body)
	}
	if second.Header().Get(headerReplayed) != "true" || calls.Load() != 1 {
		t.Fatal("handler must run once and the replay must be flagged")
	}
	if rec := do(t, h, "tenant-a", "k1", `{"a":2}`); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("reused key with another body: got %d", rec.Code)
	}
	if rec := do(t, h, "tenant-b", "k1", `{"a":1}`); rec.Code != 201 || calls.Load() != 2 {
		t.Fatal("idempotency keys must be scoped per tenant")
	}
	do(t, h, "tenant-a", "", `{}`)
	do(t, h, "tenant-a", "", `{}`)
	if calls.Load() != 4 {
		t.Fatal("requests without a key are not deduplicated")
	}
}

func TestIdempotencyReleasesKeyOnServerError(t *testing.T) {
	s := testServer(1000, 1000)
	fail := true
	h := s.authenticate(s.idempotent(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail {
			writeError(w, r, s.Log, errors.New("db down"))
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"ok": "1"})
	})))
	if rec := do(t, h, "tenant-a", "k", `{}`); rec.Code != 500 {
		t.Fatal(rec.Code)
	}
	fail = false
	if rec := do(t, h, "tenant-a", "k", `{}`); rec.Code != 201 {
		t.Fatalf("a failed request must be retryable with the same key, got %d", rec.Code)
	}
}

func TestAuthenticationAndRateLimit(t *testing.T) {
	s := testServer(0.001, 2)
	h := s.correlate(s.authenticate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })))
	if rec := do(t, h, "", "", ""); rec.Code != 401 || rec.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("missing token: %d", rec.Code)
	}
	if rec := do(t, h, "tenant-a", "", ""); rec.Code != 204 {
		t.Fatal(rec.Code)
	}
	do(t, h, "tenant-a", "", "")
	rec := do(t, h, "tenant-a", "", "")
	if rec.Code != 429 || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("expected 429 with Retry-After, got %d", rec.Code)
	}
	if rec := do(t, h, "tenant-b", "", ""); rec.Code != 204 {
		t.Fatal("rate limits are per tenant")
	}
	if rec.Header().Get(headerCorrelationID) == "" {
		t.Fatal("responses must carry a correlation id")
	}
}

func TestPanicsBecome500(t *testing.T) {
	s := testServer(1000, 1000)
	h := s.recoverPanics(s.correlate(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") })))
	if rec := do(t, h, "", "", ""); rec.Code != 500 {
		t.Fatal(rec.Code)
	}
}

func TestDecodeJSONIsStrict(t *testing.T) {
	var v struct {
		A int `json:"a"`
	}
	for _, body := range []string{`{"a":1,"b":2}`, `{"a":1}{"a":2}`, `{`} {
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
		var invalid *application.InvalidInputError
		if err := decodeJSON(req, &v); !errors.As(err, &invalid) {
			t.Errorf("%s: expected invalid input, got %v", body, err)
		}
	}
}
