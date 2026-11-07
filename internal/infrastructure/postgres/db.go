// Package postgres implements every persistence port on PostgreSQL, which is
// both the system of record and the task queue (see ADR-0002).
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MatheusAndrell/go-flow/internal/application"
)

type Store struct {
	pool  *pgxpool.Pool
	newID func() string
}

type PoolConfig struct {
	URL      string
	MaxConns int32
}

func Connect(ctx context.Context, cfg PoolConfig) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if cfg.MaxConns > 0 {
		pc.MaxConns = cfg.MaxConns
	}
	pc.MaxConnIdleTime = 5 * time.Minute
	pc.HealthCheckPeriod = 30 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

// New creates a store. newID generates identifiers for entities created
// while applying domain decisions (compensation tasks, dead letters).
func New(pool *pgxpool.Pool, newID func() string) *Store { return &Store{pool: pool, newID: newID} }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

const maxTxRetries = 3

// inTx runs fn in a transaction, retrying on serialization failures and
// deadlocks. fn must be safe to re-run from scratch.
func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	var err error
	for attempt := range maxTxRetries {
		err = pgx.BeginFunc(ctx, s.pool, fn)
		if !retryable(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 20 * time.Millisecond):
		}
	}
	return err
}

func retryable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01")
}

// mapErr translates driver errors into application errors.
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return application.ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return fmt.Errorf("%w: %s", application.ErrConflict, pgErr.ConstraintName)
		case "22P02":
			// invalid_text_representation, e.g. a malformed UUID in a path parameter.
			return application.ErrNotFound
		}
	}
	return err
}

// nullJSON stores empty raw JSON as SQL NULL.
func nullJSON(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}

func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func strPtr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
