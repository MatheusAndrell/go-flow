package httpapi

import (
	"math"
	"sync"
	"time"
)

// RateLimiter is a token bucket per key (tenant, or client IP for
// unauthenticated requests). State is per process: with N API replicas the
// effective limit is N times the configured one (see known limitations).
type RateLimiter struct {
	rate  float64
	burst float64
	now   func() time.Time

	mu      sync.Mutex
	buckets map[string]*bucket
	calls   int
}

type bucket struct {
	tokens float64
	last   time.Time
}

const (
	bucketIdleTTL     = 10 * time.Minute
	sweepEveryNthCall = 1024
)

func NewRateLimiter(rps float64, burst int, now func() time.Time) *RateLimiter {
	return &RateLimiter{rate: rps, burst: float64(burst), now: now, buckets: map[string]*bucket{}}
}

// Allow consumes a token. When none is available it returns the wait until the next one.
func (l *RateLimiter) Allow(key string) (bool, time.Duration) {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	if l.calls%sweepEveryNthCall == 0 {
		for k, b := range l.buckets {
			if now.Sub(b.last) > bucketIdleTTL {
				delete(l.buckets, k)
			}
		}
	}
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	b.tokens = math.Min(l.burst, b.tokens+now.Sub(b.last).Seconds()*l.rate)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	wait := time.Duration((1 - b.tokens) / l.rate * float64(time.Second))
	return false, wait
}
