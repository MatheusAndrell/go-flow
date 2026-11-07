// Package handler is the public contract between GoFlow and the code that
// executes workflow steps. Applications implement Handler and register it on
// a Registry; GoFlow never executes code supplied through the API.
package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sync"
)

// Request describes one attempt of a step (or of its compensation).
type Request struct {
	TaskID      string
	ExecutionID string
	TenantID    string
	StepKey     string
	Attempt     int
	// Compensation is true when this call must undo the step identified by StepKey.
	Compensation bool
	// IdempotencyKey is stable across every attempt of the same task. Delivery
	// is at-least-once: handlers with external side effects must use this key
	// to make repeated calls harmless.
	IdempotencyKey string

	WorkflowInput json.RawMessage
	Params        json.RawMessage
	// Dependencies holds the outputs of the steps this step depends on.
	Dependencies map[string]json.RawMessage
	// CompensatedOutput is the output of the step being compensated.
	CompensatedOutput json.RawMessage
}

// Handler executes a step. The context carries the step timeout and is
// cancelled when the execution is cancelled or the worker loses the task's
// lease; handlers must honour it. Returning an error wrapped with Permanent
// fails the task immediately; any other error is retried per the step's policy.
type Handler interface {
	Handle(ctx context.Context, req Request) (json.RawMessage, error)
}

// Func adapts an ordinary function to Handler.
type Func func(ctx context.Context, req Request) (json.RawMessage, error)

func (f Func) Handle(ctx context.Context, req Request) (json.RawMessage, error) { return f(ctx, req) }

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent marks an error as non-retryable (validation failures, business
// rule violations, declined payments...).
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

// Permanentf is fmt.Errorf followed by Permanent.
func Permanentf(format string, args ...any) error {
	return Permanent(fmt.Errorf(format, args...))
}

func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

var namePattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)

// Registry maps handler names (referenced by workflow steps) to implementations.
// It is safe for concurrent use.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func NewRegistry() *Registry {
	return &Registry{handlers: map[string]Handler{}}
}

func (r *Registry) Register(name string, h Handler) error {
	if !namePattern.MatchString(name) {
		return fmt.Errorf("invalid handler name %q", name)
	}
	if h == nil {
		return fmt.Errorf("handler %q is nil", name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[name]; exists {
		return fmt.Errorf("handler %q already registered", name)
	}
	r.handlers[name] = h
	return nil
}

// MustRegister is Register for program initialization.
func (r *Registry) MustRegister(name string, h Handler) {
	if err := r.Register(name, h); err != nil {
		panic(err)
	}
}

func (r *Registry) Lookup(name string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[name]
	return h, ok
}

func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.handlers))
	for n := range r.handlers {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}
