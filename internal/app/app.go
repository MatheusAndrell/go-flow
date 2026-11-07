// Package app is the composition root: it wires adapters to use cases and
// runs the API server, the scheduler and workers.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"

	"github.com/MatheusAndrell/go-flow/internal/application/access"
	"github.com/MatheusAndrell/go-flow/internal/application/definitions"
	"github.com/MatheusAndrell/go-flow/internal/application/engine"
	"github.com/MatheusAndrell/go-flow/internal/application/executions"
	"github.com/MatheusAndrell/go-flow/internal/application/scheduler"
	"github.com/MatheusAndrell/go-flow/internal/application/worker"
	"github.com/MatheusAndrell/go-flow/internal/infrastructure/postgres"
	"github.com/MatheusAndrell/go-flow/internal/interfaces/httpapi"
	"github.com/MatheusAndrell/go-flow/internal/platform/config"
	"github.com/MatheusAndrell/go-flow/internal/platform/telemetry"
	"github.com/MatheusAndrell/go-flow/migrations"
	"github.com/MatheusAndrell/go-flow/pkg/handler"
)

const Version = "1.0.0"

type App struct {
	Config  config.Config
	Log     *slog.Logger
	Pool    *pgxpool.Pool
	Store   *postgres.Store
	Metrics *telemetry.Metrics
	Engine  *engine.Engine
	Admin   *access.Admin

	shutdownTracing func(context.Context) error
}

func NewID() string { return uuid.Must(uuid.NewV7()).String() }

// New connects to the database, applies migrations when configured and sets up telemetry.
func New(ctx context.Context, cfg config.Config, log *slog.Logger) (*App, error) {
	shutdown, err := telemetry.SetupTracing(ctx, "goflow", Version, cfg.OTLPEndpoint, cfg.TraceSampleRatio)
	if err != nil {
		return nil, err
	}
	pool, err := postgres.Connect(ctx, postgres.PoolConfig{URL: cfg.DatabaseURL, MaxConns: cfg.DatabaseMaxConns})
	if err != nil {
		_ = shutdown(ctx)
		return nil, err
	}
	log.Info("connected to database", "url", cfg.RedactedDatabaseURL())
	if cfg.AutoMigrate {
		if err := postgres.Migrate(ctx, pool, migrations.FS, log); err != nil {
			pool.Close()
			_ = shutdown(ctx)
			return nil, fmt.Errorf("migrate: %w", err)
		}
	}
	store := postgres.New(pool, NewID)
	metrics := telemetry.NewMetrics()
	return &App{
		Config: cfg, Log: log, Pool: pool, Store: store, Metrics: metrics,
		Engine:          engine.New(store, metrics, log.With("component", "engine")),
		Admin:           access.NewAdmin(store, NewID, time.Now),
		shutdownTracing: shutdown,
	}, nil
}

func (a *App) Close(ctx context.Context) {
	a.Pool.Close()
	if err := a.shutdownTracing(ctx); err != nil {
		a.Log.Warn("flush traces", "error", err)
	}
}

// HTTPHandler builds the API handler (exported for end-to-end tests).
func (a *App) HTTPHandler() http.Handler {
	defs := definitions.NewService(a.Store, a.Store, NewID, time.Now)
	execs := executions.NewService(a.Store, a.Store, a.Metrics, NewID, time.Now)
	srv := httpapi.NewServer(httpapi.Deps{
		Definitions: defs, Executions: execs,
		Auth:        access.NewAuthenticator(a.Store, a.Config.AuthCacheTTL, time.Now),
		Idempotency: a.Store, IdempotencyTTL: a.Config.IdempotencyTTL,
		Limiter: httpapi.NewRateLimiter(a.Config.RateLimitRPS, a.Config.RateLimitBurst, time.Now),
		Metrics: a.Metrics, Registry: a.Metrics.Registry, Tracer: otel.Tracer("goflow/http"),
		Ready: func(ctx context.Context) error {
			if err := a.Store.Ping(ctx); err != nil {
				return fmt.Errorf("database: %w", err)
			}
			return a.Store.MigrationsApplied(ctx, migrations.FS)
		},
		Log: a.Log.With("component", "http"), MaxBodyBytes: a.Config.MaxRequestBytes,
	})
	return srv.Handler()
}

// RunServer serves the API and runs the scheduler until ctx is cancelled.
func (a *App) RunServer(ctx context.Context) error {
	srv := &http.Server{
		Addr: a.Config.HTTPAddr, Handler: a.HTTPHandler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 2 * time.Minute,
	}
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		a.Log.Info("http server listening", "addr", a.Config.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.Config.ShutdownTimeout)
		defer cancel()
		a.Log.Info("shutting down http server")
		return srv.Shutdown(shutdownCtx)
	})
	g.Go(func() error {
		a.Scheduler().Run(ctx)
		return nil
	})
	return g.Wait()
}

func (a *App) Scheduler() *scheduler.Scheduler {
	return scheduler.New(scheduler.Config{Interval: a.Config.SchedulerInterval, BatchSize: a.Config.SchedulerBatchSize},
		a.Engine, a.Store, telemetry.NewStatsCollector(a.Metrics, a.Store), a.Log)
}

// Worker builds a worker executing the given handlers.
func (a *App) Worker(handlers *handler.Registry) *worker.Worker {
	return worker.New(worker.Config{
		ID: a.Config.WorkerID, Concurrency: a.Config.WorkerConcurrency, PollInterval: a.Config.WorkerPollInterval,
		LeaseDuration: a.Config.WorkerLeaseDuration, HeartbeatInterval: a.Config.WorkerHeartbeatInterval,
		ShutdownTimeout: a.Config.ShutdownTimeout,
	}, a.Store, a.Engine, handlers, a.Metrics, otel.Tracer("goflow/worker"), a.Log.With("component", "worker"))
}

// MetricsServer exposes /metrics and /health for worker-only processes.
func (a *App) MetricsServer(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(a.Metrics.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{"status":"ok"}`)) })
	return &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
}
