// Package config loads runtime configuration from environment variables
// (12-factor). Secrets such as the database URL are never logged.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL      string
	DatabaseMaxConns int32
	AutoMigrate      bool

	HTTPAddr        string
	ShutdownTimeout time.Duration
	RateLimitRPS    float64
	RateLimitBurst  int
	AuthCacheTTL    time.Duration
	IdempotencyTTL  time.Duration
	MaxRequestBytes int64

	WorkerID                string
	WorkerConcurrency       int
	WorkerPollInterval      time.Duration
	WorkerLeaseDuration     time.Duration
	WorkerHeartbeatInterval time.Duration
	ExampleHandlers         bool

	SchedulerInterval  time.Duration
	SchedulerBatchSize int

	LogLevel         string
	LogFormat        string
	OTLPEndpoint     string
	TraceSampleRatio float64
}

func Load() (Config, error) {
	l := loader{}
	c := Config{
		DatabaseURL:      l.str("GOFLOW_DATABASE_URL", ""),
		DatabaseMaxConns: int32(l.int("GOFLOW_DATABASE_MAX_CONNS", 20)),
		AutoMigrate:      l.bool("GOFLOW_AUTO_MIGRATE", false),

		HTTPAddr:        l.str("GOFLOW_HTTP_ADDR", ":8080"),
		ShutdownTimeout: l.dur("GOFLOW_SHUTDOWN_TIMEOUT", 30*time.Second),
		RateLimitRPS:    l.float("GOFLOW_RATE_LIMIT_RPS", 50),
		RateLimitBurst:  l.int("GOFLOW_RATE_LIMIT_BURST", 100),
		AuthCacheTTL:    l.dur("GOFLOW_AUTH_CACHE_TTL", 30*time.Second),
		IdempotencyTTL:  l.dur("GOFLOW_IDEMPOTENCY_TTL", 24*time.Hour),
		MaxRequestBytes: int64(l.int("GOFLOW_MAX_REQUEST_BYTES", 1<<20)),

		WorkerID:                l.str("GOFLOW_WORKER_ID", ""),
		WorkerConcurrency:       l.int("GOFLOW_WORKER_CONCURRENCY", 10),
		WorkerPollInterval:      l.dur("GOFLOW_WORKER_POLL_INTERVAL", 500*time.Millisecond),
		WorkerLeaseDuration:     l.dur("GOFLOW_WORKER_LEASE_DURATION", 30*time.Second),
		WorkerHeartbeatInterval: l.dur("GOFLOW_WORKER_HEARTBEAT_INTERVAL", 10*time.Second),
		ExampleHandlers:         l.bool("GOFLOW_EXAMPLE_HANDLERS", false),

		SchedulerInterval:  l.dur("GOFLOW_SCHEDULER_INTERVAL", time.Second),
		SchedulerBatchSize: l.int("GOFLOW_SCHEDULER_BATCH_SIZE", 100),

		LogLevel:         l.str("GOFLOW_LOG_LEVEL", "info"),
		LogFormat:        l.str("GOFLOW_LOG_FORMAT", "json"),
		OTLPEndpoint:     l.str("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", ""),
		TraceSampleRatio: l.float("GOFLOW_TRACE_SAMPLE_RATIO", 1),
	}
	if len(l.errs) > 0 {
		return c, errors.Join(l.errs...)
	}
	return c, c.validate()
}

func (c Config) validate() error {
	var errs []error
	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("GOFLOW_DATABASE_URL is required"))
	}
	if c.WorkerConcurrency < 1 || c.WorkerConcurrency > 1000 {
		errs = append(errs, errors.New("GOFLOW_WORKER_CONCURRENCY must be between 1 and 1000"))
	}
	if c.WorkerHeartbeatInterval >= c.WorkerLeaseDuration {
		errs = append(errs, errors.New("GOFLOW_WORKER_HEARTBEAT_INTERVAL must be shorter than GOFLOW_WORKER_LEASE_DURATION"))
	}
	if c.RateLimitRPS <= 0 || c.RateLimitBurst < 1 {
		errs = append(errs, errors.New("rate limit must be positive"))
	}
	if c.TraceSampleRatio < 0 || c.TraceSampleRatio > 1 {
		errs = append(errs, errors.New("GOFLOW_TRACE_SAMPLE_RATIO must be between 0 and 1"))
	}
	return errors.Join(errs...)
}

// RedactedDatabaseURL is safe to log.
func (c Config) RedactedDatabaseURL() string {
	u, err := url.Parse(c.DatabaseURL)
	if err != nil {
		return "<invalid>"
	}
	return u.Redacted()
}

type loader struct{ errs []error }

func (l *loader) str(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return def
}

func (l *loader) int(key string, def int) int {
	v := l.str(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: %w", key, err))
	}
	return n
}

func (l *loader) float(key string, def float64) float64 {
	v := l.str(key, "")
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: %w", key, err))
	}
	return f
}

func (l *loader) bool(key string, def bool) bool {
	v := l.str(key, "")
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: %w", key, err))
	}
	return b
}

func (l *loader) dur(key string, def time.Duration) time.Duration {
	v := l.str(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: %w", key, err))
	}
	return d
}
