// Package retry models how failed task attempts are retried.
package retry

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"
)

const (
	maxAttemptsLimit = 100
	maxIntervalLimit = 24 * time.Hour
)

// Policy describes exponential backoff with jitter.
// MaxAttempts counts every attempt, including the first one.
type Policy struct {
	MaxAttempts     int
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	// Jitter is the fraction of the computed delay that is randomized, in [0, 1].
	// A jitter of 0.2 spreads a 10s delay uniformly over [8s, 12s].
	Jitter float64
}

// Default is applied to steps that declare no policy.
func Default() Policy {
	return Policy{
		MaxAttempts:     3,
		InitialInterval: time.Second,
		MaxInterval:     time.Minute,
		Multiplier:      2,
		Jitter:          0.2,
	}
}

// NoRetry runs a task exactly once.
func NoRetry() Policy {
	return Policy{MaxAttempts: 1, InitialInterval: time.Second, MaxInterval: time.Second, Multiplier: 1}
}

func (p Policy) Validate() error {
	var errs []error
	if p.MaxAttempts < 1 || p.MaxAttempts > maxAttemptsLimit {
		errs = append(errs, fmt.Errorf("max_attempts must be between 1 and %d", maxAttemptsLimit))
	}
	if p.InitialInterval <= 0 {
		errs = append(errs, errors.New("initial_interval must be positive"))
	}
	if p.MaxInterval < p.InitialInterval {
		errs = append(errs, errors.New("max_interval must be greater than or equal to initial_interval"))
	}
	if p.MaxInterval > maxIntervalLimit {
		errs = append(errs, fmt.Errorf("max_interval must not exceed %s", maxIntervalLimit))
	}
	if p.Multiplier < 1 {
		errs = append(errs, errors.New("multiplier must be at least 1"))
	}
	if p.Jitter < 0 || p.Jitter > 1 {
		errs = append(errs, errors.New("jitter must be between 0 and 1"))
	}
	return errors.Join(errs...)
}

// CanRetry reports whether another attempt is allowed after `attempts` attempts were made.
func (p Policy) CanRetry(attempts int) bool {
	return attempts < p.MaxAttempts
}

// Backoff returns the delay before the next attempt, given that `attempt`
// attempts (>= 1) have already failed. rnd must return values in [0, 1).
func (p Policy) Backoff(attempt int, rnd func() float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	base := float64(p.InitialInterval) * math.Pow(p.Multiplier, float64(attempt-1))
	if base > float64(p.MaxInterval) || math.IsInf(base, 0) || math.IsNaN(base) {
		base = float64(p.MaxInterval)
	}
	if p.Jitter > 0 && rnd != nil {
		spread := base * p.Jitter
		base = base - spread + 2*spread*rnd()
	}
	if base > float64(p.MaxInterval) {
		base = float64(p.MaxInterval)
	}
	if base < 0 {
		base = 0
	}
	return time.Duration(base)
}

type policyJSON struct {
	MaxAttempts     int     `json:"max_attempts"`
	InitialInterval string  `json:"initial_interval"`
	MaxInterval     string  `json:"max_interval"`
	Multiplier      float64 `json:"multiplier"`
	Jitter          float64 `json:"jitter"`
}

func (p Policy) MarshalJSON() ([]byte, error) {
	return json.Marshal(policyJSON{
		MaxAttempts:     p.MaxAttempts,
		InitialInterval: p.InitialInterval.String(),
		MaxInterval:     p.MaxInterval.String(),
		Multiplier:      p.Multiplier,
		Jitter:          p.Jitter,
	})
}

func (p *Policy) UnmarshalJSON(data []byte) error {
	raw := policyJSON{Multiplier: 2}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	var err error
	out := Policy{MaxAttempts: raw.MaxAttempts, Multiplier: raw.Multiplier, Jitter: raw.Jitter}
	if out.InitialInterval, err = parseDuration("initial_interval", raw.InitialInterval, time.Second); err != nil {
		return err
	}
	if out.MaxInterval, err = parseDuration("max_interval", raw.MaxInterval, time.Minute); err != nil {
		return err
	}
	*p = out
	return nil
}

func parseDuration(field, value string, fallback time.Duration) (time.Duration, error) {
	if value == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", field, err)
	}
	return d, nil
}
