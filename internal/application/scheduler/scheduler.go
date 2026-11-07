// Package scheduler runs the periodic, database-driven background jobs:
// activating scheduled executions, recovering abandoned tasks and purging
// expired idempotency keys. Nothing is kept in in-memory timers: every due
// time lives in PostgreSQL, so a restart loses nothing, and every job is safe
// to run concurrently on several instances (SKIP LOCKED / conditional updates).
package scheduler

import (
	"context"
	"log/slog"
	"time"
)

type Engine interface {
	ActivateDue(ctx context.Context, limit int) (int, error)
	RecoverAbandoned(ctx context.Context, limit int) (int, error)
}

type Housekeeping interface {
	PurgeExpiredIdempotencyKeys(ctx context.Context) (int64, error)
	MarkStaleWorkersStopped(ctx context.Context, olderThan time.Duration) (int64, error)
}

// StatsCollector refreshes gauges derived from database state.
type StatsCollector interface {
	Collect(ctx context.Context) error
}

type Config struct {
	Interval          time.Duration
	BatchSize         int
	HousekeepingEvery time.Duration
	StaleWorkerAfter  time.Duration
	StatsInterval     time.Duration
}

type Scheduler struct {
	cfg    Config
	engine Engine
	house  Housekeeping
	stats  StatsCollector
	log    *slog.Logger
}

func New(cfg Config, engine Engine, house Housekeeping, stats StatsCollector, log *slog.Logger) *Scheduler {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.HousekeepingEvery <= 0 {
		cfg.HousekeepingEvery = 5 * time.Minute
	}
	if cfg.StaleWorkerAfter <= 0 {
		cfg.StaleWorkerAfter = 2 * time.Minute
	}
	if cfg.StatsInterval <= 0 {
		cfg.StatsInterval = 15 * time.Second
	}
	return &Scheduler{cfg: cfg, engine: engine, house: house, stats: stats, log: log.With("component", "scheduler")}
}

// Run blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	s.log.Info("scheduler started", "interval", s.cfg.Interval)
	tick := time.NewTicker(s.cfg.Interval)
	house := time.NewTicker(s.cfg.HousekeepingEvery)
	stats := time.NewTicker(s.cfg.StatsInterval)
	defer tick.Stop()
	defer house.Stop()
	defer stats.Stop()
	s.collectStats(ctx)
	for {
		select {
		case <-ctx.Done():
			s.log.Info("scheduler stopped")
			return
		case <-tick.C:
			s.Tick(ctx)
		case <-house.C:
			s.housekeeping(ctx)
		case <-stats.C:
			s.collectStats(ctx)
		}
	}
}

// Tick runs one activation and recovery pass, draining full batches.
func (s *Scheduler) Tick(ctx context.Context) {
	for {
		n, err := s.engine.ActivateDue(ctx, s.cfg.BatchSize)
		if err != nil {
			s.log.ErrorContext(ctx, "activate due executions", "error", err)
			break
		}
		if n > 0 {
			s.log.InfoContext(ctx, "activated scheduled executions", "count", n)
		}
		if n < s.cfg.BatchSize {
			break
		}
	}
	for {
		n, err := s.engine.RecoverAbandoned(ctx, s.cfg.BatchSize)
		if err != nil {
			s.log.ErrorContext(ctx, "recover abandoned tasks", "error", err)
			return
		}
		if n < s.cfg.BatchSize {
			return
		}
	}
}

func (s *Scheduler) housekeeping(ctx context.Context) {
	if n, err := s.house.PurgeExpiredIdempotencyKeys(ctx); err != nil {
		s.log.ErrorContext(ctx, "purge idempotency keys", "error", err)
	} else if n > 0 {
		s.log.InfoContext(ctx, "purged expired idempotency keys", "count", n)
	}
	if n, err := s.house.MarkStaleWorkersStopped(ctx, s.cfg.StaleWorkerAfter); err != nil {
		s.log.ErrorContext(ctx, "mark stale workers", "error", err)
	} else if n > 0 {
		s.log.WarnContext(ctx, "marked unresponsive workers as stopped", "count", n)
	}
}

func (s *Scheduler) collectStats(ctx context.Context) {
	if err := s.stats.Collect(ctx); err != nil {
		s.log.ErrorContext(ctx, "collect stats", "error", err)
	}
}
