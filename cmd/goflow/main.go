// Command goflow runs the GoFlow orchestration engine.
//
//	goflow serve      API server + scheduler/recovery loop
//	goflow worker     task worker
//	goflow all        serve and worker in one process (local development)
//	goflow migrate    apply database migrations
//	goflow tenant create -name NAME
//	goflow apikey create -tenant NAME -name NAME -permissions p1,p2
//	goflow apikey revoke -id KEY_ID
//	goflow probe [URL]  exit 0 when URL answers 2xx (container healthchecks)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/MatheusAndrell/go-flow/internal/app"
	"github.com/MatheusAndrell/go-flow/internal/domain/identity"
	"github.com/MatheusAndrell/go-flow/internal/examples/orders"
	"github.com/MatheusAndrell/go-flow/internal/infrastructure/postgres"
	"github.com/MatheusAndrell/go-flow/internal/platform/config"
	"github.com/MatheusAndrell/go-flow/internal/platform/telemetry"
	"github.com/MatheusAndrell/go-flow/migrations"
	"github.com/MatheusAndrell/go-flow/pkg/handler"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: goflow <serve|worker|all|migrate|tenant|apikey|probe|version>")
	}
	switch args[0] {
	case "version":
		fmt.Println("goflow", app.Version)
		return nil
	case "probe":
		return probe(args[1:])
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := telemetry.NewLogger(os.Stdout, cfg.LogLevel, cfg.LogFormat)
	slog.SetDefault(log)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch args[0] {
	case "migrate":
		pool, err := postgres.Connect(ctx, postgres.PoolConfig{URL: cfg.DatabaseURL, MaxConns: 2})
		if err != nil {
			return err
		}
		defer pool.Close()
		return postgres.Migrate(ctx, pool, migrations.FS, log)
	case "serve", "worker", "all", "tenant", "apikey":
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}

	a, err := app.New(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer a.Close(context.Background())

	switch args[0] {
	case "serve":
		return a.RunServer(ctx)
	case "worker":
		return runWorker(ctx, a, true)
	case "all":
		g, gctx := errgroup.WithContext(ctx)
		g.Go(func() error { return a.RunServer(gctx) })
		g.Go(func() error { return runWorker(gctx, a, false) })
		return g.Wait()
	case "tenant":
		return tenantCmd(ctx, a, args[1:])
	default:
		return apikeyCmd(ctx, a, args[1:])
	}
}

func handlers(cfg config.Config) (*handler.Registry, error) {
	reg := handler.NewRegistry()
	if cfg.ExampleHandlers {
		inv := orders.NewInventory(map[string]int{"SKU-1": 1_000_000, "SKU-2": 1_000_000, "SKU-3": 10})
		if err := orders.Register(reg, inv, orders.NewPayments()); err != nil {
			return nil, err
		}
	}
	if len(reg.Names()) == 0 {
		return nil, errors.New("no handlers registered: set GOFLOW_EXAMPLE_HANDLERS=true or embed a worker with pkg/goflow")
	}
	return reg, nil
}

func runWorker(ctx context.Context, a *app.App, serveMetrics bool) error {
	reg, err := handlers(a.Config)
	if err != nil {
		return err
	}
	g, ctx := errgroup.WithContext(ctx)
	if serveMetrics {
		addr := os.Getenv("GOFLOW_METRICS_ADDR")
		if addr == "" {
			addr = ":9090"
		}
		srv := a.MetricsServer(addr)
		g.Go(func() error {
			a.Log.Info("worker metrics listening", "addr", addr)
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		})
		g.Go(func() error {
			<-ctx.Done()
			sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			return srv.Shutdown(sctx)
		})
	}
	g.Go(func() error { return a.Worker(reg).Run(ctx) })
	return g.Wait()
}

func tenantCmd(ctx context.Context, a *app.App, args []string) error {
	if len(args) == 0 || args[0] != "create" {
		return errors.New("usage: goflow tenant create -name NAME")
	}
	fs := flag.NewFlagSet("tenant create", flag.ContinueOnError)
	name := fs.String("name", "", "tenant name")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	t, err := a.Admin.EnsureTenant(ctx, *name)
	if err != nil {
		return err
	}
	fmt.Printf("tenant %s (%s)\n", t.Name, t.ID)
	return nil
}

func apikeyCmd(ctx context.Context, a *app.App, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: goflow apikey <create|revoke>")
	}
	switch args[0] {
	case "create":
		fs := flag.NewFlagSet("apikey create", flag.ContinueOnError)
		tenant := fs.String("tenant", "", "tenant name (created if missing)")
		name := fs.String("name", "default", "key name")
		perms := fs.String("permissions", allPermissions(), "comma-separated permissions")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		t, err := a.Admin.EnsureTenant(ctx, *tenant)
		if err != nil {
			return err
		}
		secret, key, err := a.Admin.CreateAPIKey(ctx, t.ID, *name, strings.Split(*perms, ","))
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "created API key %s for tenant %s with permissions %v\nthe secret below is shown only once:\n", key.ID, t.Name, key.Permissions)
		fmt.Println(secret)
		return nil
	case "revoke":
		fs := flag.NewFlagSet("apikey revoke", flag.ContinueOnError)
		id := fs.String("id", "", "API key id")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		return a.Admin.RevokeAPIKey(ctx, *id)
	}
	return fmt.Errorf("unknown apikey command %q", args[0])
}

func allPermissions() string {
	out := make([]string, len(identity.AllPermissions))
	for i, p := range identity.AllPermissions {
		out[i] = string(p)
	}
	return strings.Join(out, ",")
}

func probe(args []string) error {
	target := "http://127.0.0.1:8080/ready"
	if len(args) > 0 {
		target = args[0]
	}
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(target)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("%s returned %d", target, resp.StatusCode)
	}
	return nil
}
