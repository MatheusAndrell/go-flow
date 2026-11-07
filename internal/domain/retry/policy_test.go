package retry

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBackoffGrowsExponentiallyAndIsCapped(t *testing.T) {
	p := Policy{MaxAttempts: 10, InitialInterval: time.Second, MaxInterval: 10 * time.Second, Multiplier: 2}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second, 10 * time.Second}
	for i, w := range want {
		if got := p.Backoff(i+1, nil); got != w {
			t.Fatalf("attempt %d: got %s, want %s", i+1, got, w)
		}
	}
}

func TestBackoffJitterStaysWithinBounds(t *testing.T) {
	p := Policy{MaxAttempts: 5, InitialInterval: 10 * time.Second, MaxInterval: time.Minute, Multiplier: 1, Jitter: 0.2}
	cases := map[float64]time.Duration{0: 8 * time.Second, 0.5: 10 * time.Second, 0.999999: 12 * time.Second}
	for r, want := range cases {
		got := p.Backoff(1, func() float64 { return r })
		if diff := got - want; diff > time.Millisecond || diff < -time.Millisecond {
			t.Fatalf("rnd=%v: got %s, want ~%s", r, got, want)
		}
	}
}

func TestBackoffJitterNeverExceedsMaxInterval(t *testing.T) {
	p := Policy{MaxAttempts: 5, InitialInterval: time.Second, MaxInterval: 4 * time.Second, Multiplier: 4, Jitter: 1}
	if got := p.Backoff(5, func() float64 { return 0.99 }); got > 4*time.Second {
		t.Fatalf("backoff %s exceeded max interval", got)
	}
}

func TestBackoffHandlesOverflow(t *testing.T) {
	p := Policy{MaxAttempts: 100, InitialInterval: time.Hour, MaxInterval: 2 * time.Hour, Multiplier: 10}
	if got := p.Backoff(90, nil); got != 2*time.Hour {
		t.Fatalf("got %s", got)
	}
}

func TestCanRetry(t *testing.T) {
	p := Policy{MaxAttempts: 3}
	if !p.CanRetry(2) || p.CanRetry(3) {
		t.Fatal("max attempts must include the first attempt")
	}
}

func TestValidate(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatalf("default policy invalid: %v", err)
	}
	bad := Policy{MaxAttempts: 0, InitialInterval: 0, MaxInterval: -1, Multiplier: 0.5, Jitter: 2}
	if err := bad.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestJSONRoundTrip(t *testing.T) {
	in := Policy{MaxAttempts: 4, InitialInterval: 500 * time.Millisecond, MaxInterval: 30 * time.Second, Multiplier: 3, Jitter: 0.1}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out Policy
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("got %+v, want %+v", out, in)
	}
}

func TestUnmarshalAppliesDefaultsAndRejectsBadDurations(t *testing.T) {
	var p Policy
	if err := json.Unmarshal([]byte(`{"max_attempts":2}`), &p); err != nil {
		t.Fatal(err)
	}
	if p.InitialInterval != time.Second || p.MaxInterval != time.Minute || p.Multiplier != 2 {
		t.Fatalf("defaults not applied: %+v", p)
	}
	if err := json.Unmarshal([]byte(`{"max_attempts":2,"initial_interval":"soon"}`), &p); err == nil {
		t.Fatal("expected error for invalid duration")
	}
}
