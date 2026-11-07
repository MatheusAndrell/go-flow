package httpapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/MatheusAndrell/go-flow/internal/application"
	"github.com/MatheusAndrell/go-flow/internal/application/idempotency"
	"github.com/MatheusAndrell/go-flow/internal/domain/identity"
	"github.com/MatheusAndrell/go-flow/internal/platform/telemetry"
)

const (
	headerCorrelationID = "X-Correlation-ID"
	headerIdempotency   = "Idempotency-Key"
	headerReplayed      = "Idempotent-Replayed"
)

var correlationPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

type principalKey struct{}

// requestInfo is shared by outer middlewares (which run before routing) and
// inner ones (which know the route and the tenant).
type requestInfo struct {
	route  string
	tenant string
}

type requestInfoKey struct{}

func infoFrom(ctx context.Context) *requestInfo {
	if i, ok := ctx.Value(requestInfoKey{}).(*requestInfo); ok {
		return i
	}
	return &requestInfo{}
}

// routed records the route pattern (without method) for logs, metrics and spans.
func routed(pattern string, next http.Handler) http.Handler {
	route := pattern
	if _, after, ok := strings.Cut(pattern, " "); ok {
		route = after
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		infoFrom(r.Context()).route = route
		next.ServeHTTP(w, r)
	})
}

func principalFrom(ctx context.Context) identity.Principal {
	p, _ := ctx.Value(principalKey{}).(identity.Principal)
	return p
}

// statusRecorder captures the status code (and optionally the body) written by a handler.
type statusRecorder struct {
	http.ResponseWriter
	status int
	buf    *bytes.Buffer
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	if r.buf != nil {
		r.buf.Write(b)
	}
	return r.ResponseWriter.Write(b)
}

func (r *statusRecorder) code() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}

func (s *Server) recoverPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				s.Log.ErrorContext(r.Context(), "panic in http handler", "panic", v, "stack", string(debug.Stack()))
				writeError(w, r, s.Log, errInternal)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type internalError struct{}

func (internalError) Error() string { return "internal error" }

var errInternal = internalError{}

// correlate propagates a client-supplied correlation id or generates one.
func (s *Server) correlate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(headerCorrelationID)
		if !correlationPattern.MatchString(id) {
			buf := make([]byte, 12)
			_, _ = rand.Read(buf)
			id = hex.EncodeToString(buf)
		}
		w.Header().Set(headerCorrelationID, id)
		ctx := context.WithValue(telemetry.WithCorrelationID(r.Context(), id), requestInfoKey{}, &requestInfo{})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) trace(next http.Handler) http.Handler {
	prop := propagation.TraceContext{}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := prop.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := s.Tracer.Start(ctx, r.Method+" "+r.URL.Path, trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attribute.String("http.request.method", r.Method), attribute.String("url.path", r.URL.Path),
				attribute.String("goflow.correlation_id", telemetry.CorrelationID(ctx))))
		defer span.End()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r.WithContext(ctx))
		if route := infoFrom(ctx).route; route != "" {
			span.SetName(r.Method + " " + route)
			span.SetAttributes(attribute.String("http.route", route))
		}
		span.SetAttributes(attribute.Int("http.response.status_code", rec.code()))
		if rec.code() >= 500 {
			span.SetStatus(codes.Error, http.StatusText(rec.code()))
		}
	})
}

// observe logs every request and records RED metrics, labelled by route pattern.
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		info := infoFrom(r.Context())
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		route := info.route
		if route == "" {
			route = "unmatched"
		}
		d := time.Since(start)
		s.Metrics.HTTPRequest(r.Method, route, rec.code(), d)
		if route == "/metrics" || route == "/health" || route == "/ready" {
			return
		}
		s.Log.InfoContext(r.Context(), "http request", "method", r.Method, "route", route, "status", rec.code(),
			"duration_ms", d.Milliseconds(), "tenant_id", info.tenant)
	})
}

func (s *Server) secureHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Cache-Control", "no-store")
		h.Set("X-Frame-Options", "DENY")
		next.ServeHTTP(w, r)
	})
}

func (s *Server) limitBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, s.MaxBodyBytes)
		next.ServeHTTP(w, r)
	})
}

// authenticate resolves the bearer API key, then applies the rate limit keyed
// by tenant. Unauthenticated callers are limited by IP so that key guessing
// is throttled too.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		var (
			p   identity.Principal
			err error
		)
		if ok {
			p, err = s.Auth.Authenticate(r.Context(), strings.TrimSpace(token))
		} else {
			err = identity.ErrUnauthenticated
		}
		key := "tenant:" + p.TenantID
		if err != nil {
			key = "ip:" + clientIP(r)
		}
		if allowed, wait := s.Limiter.Allow(key); !allowed {
			s.Metrics.RateLimited()
			w.Header().Set("Retry-After", strconv.Itoa(int(wait/time.Second)+1))
			writeJSON(w, http.StatusTooManyRequests, Problem{Type: "rate-limited", Title: "Too many requests", Status: http.StatusTooManyRequests,
				CorrelationID: telemetry.CorrelationID(r.Context())})
			return
		}
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="goflow"`)
			writeError(w, r, s.Log, err)
			return
		}
		infoFrom(r.Context()).tenant = p.TenantID
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, p)))
	})
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// idempotent makes POST requests carrying an Idempotency-Key safe to retry:
// the first response (if not a 5xx) is stored and replayed for repeats with
// the same body; reusing a key with a different body is rejected.
func (s *Server) idempotent(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get(headerIdempotency)
		if key == "" {
			next.ServeHTTP(w, r)
			return
		}
		if len(key) > 255 {
			writeError(w, r, s.Log, application.Invalid("Idempotency-Key must not exceed 255 characters"))
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, r, s.Log, err)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		sum := sha256.New()
		sum.Write([]byte(r.Method + " " + r.URL.Path + "\n"))
		sum.Write(body)
		fingerprint := sum.Sum(nil)

		tenant := principalFrom(r.Context()).TenantID
		rec, err := s.Idempotency.BeginIdempotent(r.Context(), tenant, key, fingerprint, s.IdempotencyTTL)
		if err != nil {
			writeError(w, r, s.Log, err)
			return
		}
		switch rec.State {
		case idempotency.StateMismatch:
			writeJSON(w, http.StatusUnprocessableEntity, Problem{Type: "idempotency-key-reused", Status: http.StatusUnprocessableEntity,
				Title: "Idempotency-Key was already used with a different request", CorrelationID: telemetry.CorrelationID(r.Context())})
			return
		case idempotency.StateInProgress:
			w.Header().Set("Retry-After", "1")
			writeJSON(w, http.StatusConflict, Problem{Type: "idempotency-key-in-progress", Status: http.StatusConflict,
				Title: "A request with this Idempotency-Key is still being processed", CorrelationID: telemetry.CorrelationID(r.Context())})
			return
		case idempotency.StateCompleted:
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set(headerReplayed, "true")
			w.WriteHeader(rec.Status)
			_, _ = w.Write(rec.Body)
			return
		}

		capture := &statusRecorder{ResponseWriter: w, buf: &bytes.Buffer{}}
		next.ServeHTTP(capture, r)
		// The response is already sent; storing it must not depend on the client connection.
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
		defer cancel()
		if capture.code() >= 500 {
			if err := s.Idempotency.ReleaseIdempotent(ctx, tenant, key); err != nil {
				s.Log.ErrorContext(ctx, "release idempotency key", "error", err)
			}
			return
		}
		if err := s.Idempotency.CompleteIdempotent(ctx, tenant, key, capture.code(), capture.buf.Bytes()); err != nil {
			s.Log.ErrorContext(ctx, "store idempotent response", "error", err)
		}
	})
}
