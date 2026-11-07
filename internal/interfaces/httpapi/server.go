// Package httpapi exposes the REST API (v1), health probes and metrics.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/trace"

	"github.com/MatheusAndrell/go-flow/internal/application/definitions"
	"github.com/MatheusAndrell/go-flow/internal/application/executions"
	"github.com/MatheusAndrell/go-flow/internal/application/idempotency"
	"github.com/MatheusAndrell/go-flow/internal/domain/identity"
)

type Authenticator interface {
	Authenticate(ctx context.Context, token string) (identity.Principal, error)
}

type Metrics interface {
	HTTPRequest(method, route string, code int, d time.Duration)
	RateLimited()
}

// ReadinessCheck returns nil when the process can serve traffic.
type ReadinessCheck func(ctx context.Context) error

type Deps struct {
	Definitions    *definitions.Service
	Executions     *executions.Service
	Auth           Authenticator
	Idempotency    idempotency.Store
	IdempotencyTTL time.Duration
	Limiter        *RateLimiter
	Metrics        Metrics
	Registry       *prometheus.Registry
	Tracer         trace.Tracer
	Ready          ReadinessCheck
	Log            *slog.Logger
	MaxBodyBytes   int64
}

type Server struct {
	Deps
}

func NewServer(d Deps) *Server {
	if d.MaxBodyBytes <= 0 {
		d.MaxBodyBytes = 1 << 20
	}
	return &Server{Deps: d}
}

type route struct {
	pattern string
	handler http.HandlerFunc
}

// apiRoutes is the v1 routing table; it must match api/openapi.yaml (see contract_test.go).
func (s *Server) apiRoutes() []route {
	return []route{
		{"POST /api/v1/workflows", s.createWorkflow},
		{"GET /api/v1/workflows", s.listWorkflows},
		{"GET /api/v1/workflows/{id}", s.getWorkflow},
		{"PUT /api/v1/workflows/{id}", s.updateWorkflow},
		{"POST /api/v1/workflows/{id}/versions", s.createVersion},
		{"POST /api/v1/workflows/{id}/publish", s.publishWorkflow},

		{"POST /api/v1/executions", s.startExecution},
		{"GET /api/v1/executions", s.listExecutions},
		{"GET /api/v1/executions/{id}", s.getExecution},
		{"GET /api/v1/executions/{id}/history", s.executionHistory},
		{"GET /api/v1/executions/{id}/tasks", s.executionTasks},
		{"POST /api/v1/executions/{id}/cancel", s.cancelExecution},
		{"POST /api/v1/executions/{id}/pause", s.pauseExecution},
		{"POST /api/v1/executions/{id}/resume", s.resumeExecution},

		{"GET /api/v1/tasks/{id}", s.getTask},
		{"GET /api/v1/tasks/{id}/attempts", s.taskAttempts},
		{"GET /api/v1/dead-letters", s.listDeadLetters},
	}
}

// Handler builds the HTTP handler. Business routes go through
// authentication, rate limiting and (for POST) idempotency.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /health", routed("GET /health", http.HandlerFunc(s.health)))
	mux.Handle("GET /ready", routed("GET /ready", http.HandlerFunc(s.ready)))
	mux.Handle("GET /metrics", routed("GET /metrics", promhttp.HandlerFor(s.Registry, promhttp.HandlerOpts{})))
	for _, r := range s.apiRoutes() {
		var h http.Handler = r.handler
		if strings.HasPrefix(r.pattern, http.MethodPost+" ") {
			h = s.idempotent(h)
		}
		mux.Handle(r.pattern, routed(r.pattern, s.authenticate(h)))
	}
	return s.recoverPanics(s.correlate(s.trace(s.observe(s.secureHeaders(s.limitBody(mux))))))
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := s.Ready(ctx); err != nil {
		s.Log.WarnContext(ctx, "readiness check failed", "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable", "reason": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
