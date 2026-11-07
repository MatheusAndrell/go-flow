// Package goflow lets applications run GoFlow workers that execute their own
// handlers. The worker connects to the GoFlow database, claims tasks whose
// handler names it registered, and reports outcomes; workflows reference
// those names in their step definitions.
//
//	reg := handler.NewRegistry()
//	reg.MustRegister("billing.charge", handler.Func(charge))
//	err := goflow.RunWorker(ctx, reg, goflow.WorkerOptions{DatabaseURL: os.Getenv("GOFLOW_DATABASE_URL")})
package goflow

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/app"
	"github.com/MatheusAndrell/go-flow/internal/platform/config"
	"github.com/MatheusAndrell/go-flow/pkg/handler"
)

type WorkerOptions struct {
	DatabaseURL string
	// WorkerID must be unique per process; generated when empty.
	WorkerID          string
	Concurrency       int
	PollInterval      time.Duration
	LeaseDuration     time.Duration
	HeartbeatInterval time.Duration
	ShutdownTimeout   time.Duration
	Logger            *slog.Logger
}

// RunWorker blocks until ctx is cancelled, then drains in-flight tasks.
func RunWorker(ctx context.Context, reg *handler.Registry, opts WorkerOptions) error {
	if opts.DatabaseURL == "" {
		return errors.New("goflow: DatabaseURL is required")
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}
	cfg := config.Config{
		DatabaseURL: opts.DatabaseURL, DatabaseMaxConns: int32(max(opts.Concurrency, 4) + 4),
		WorkerID: opts.WorkerID, WorkerConcurrency: opts.Concurrency, WorkerPollInterval: opts.PollInterval,
		WorkerLeaseDuration: opts.LeaseDuration, WorkerHeartbeatInterval: opts.HeartbeatInterval,
		ShutdownTimeout: opts.ShutdownTimeout, TraceSampleRatio: 1,
	}
	a, err := app.New(ctx, cfg, opts.Logger)
	if err != nil {
		return err
	}
	defer a.Close(context.WithoutCancel(ctx))
	return a.Worker(reg).Run(ctx)
}
