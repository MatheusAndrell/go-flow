package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"log/slog"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// migrationLockID is an arbitrary constant for pg_advisory_xact_lock so that
// concurrent instances never apply migrations twice.
const migrationLockID = 7_316_204_115

// Migrate applies pending *.sql files from fsys in lexical order. Each file
// runs in its own transaction; applied files are checksummed so an edited
// migration is detected instead of silently diverging.
func Migrate(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS, log *slog.Logger) error {
	names, err := fs.Glob(fsys, "*.sql")
	if err != nil {
		return err
	}
	slices.Sort(names)
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    text PRIMARY KEY,
		checksum   text NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	for _, name := range names {
		body, err := fs.ReadFile(fsys, name)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(body)
		checksum := hex.EncodeToString(sum[:])
		version := strings.TrimSuffix(name, ".sql")
		applied := false
		err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockID); err != nil {
				return err
			}
			var existing string
			err := tx.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version = $1`, version).Scan(&existing)
			if err == nil {
				if existing != checksum {
					return fmt.Errorf("migration %s was modified after being applied", name)
				}
				return nil
			}
			if err != pgx.ErrNoRows {
				return err
			}
			if _, err := tx.Exec(ctx, string(body)); err != nil {
				return fmt.Errorf("apply %s: %w", name, err)
			}
			_, err = tx.Exec(ctx, `INSERT INTO schema_migrations (version, checksum) VALUES ($1, $2)`, version, checksum)
			applied = err == nil
			return err
		})
		if err != nil {
			return err
		}
		if applied {
			log.Info("applied migration", "version", version)
		}
	}
	return nil
}

// MigrationsApplied reports whether every migration in fsys has been applied.
// Used by the readiness probe.
func (s *Store) MigrationsApplied(ctx context.Context, fsys fs.FS) error {
	names, err := fs.Glob(fsys, "*.sql")
	if err != nil {
		return err
	}
	var count int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&count); err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	if count < len(names) {
		return fmt.Errorf("%d of %d migrations applied", count, len(names))
	}
	return nil
}
