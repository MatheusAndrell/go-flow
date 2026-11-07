// Package idempotency defines how repeated client requests carrying the same
// Idempotency-Key are recognised and answered with the original response.
package idempotency

import (
	"context"
	"time"
)

type State int

const (
	// StateNew means the caller reserved the key and must process the request.
	StateNew State = iota
	// StateInProgress means another request with the key is being processed.
	StateInProgress
	// StateCompleted means a stored response must be replayed.
	StateCompleted
	// StateMismatch means the key was reused with a different request.
	StateMismatch
)

type Record struct {
	State  State
	Status int
	Body   []byte
}

type Store interface {
	BeginIdempotent(ctx context.Context, tenantID, key string, fingerprint []byte, ttl time.Duration) (Record, error)
	CompleteIdempotent(ctx context.Context, tenantID, key string, status int, body []byte) error
	// ReleaseIdempotent forgets a reservation whose request failed, so the client can retry.
	ReleaseIdempotent(ctx context.Context, tenantID, key string) error
}
