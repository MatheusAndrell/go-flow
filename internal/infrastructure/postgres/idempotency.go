package postgres

import (
	"bytes"
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/MatheusAndrell/go-flow/internal/application/idempotency"
)

// BeginIdempotent reserves (tenant, key). It returns the stored response when
// the key was already completed with the same request fingerprint.
func (s *Store) BeginIdempotent(ctx context.Context, tenantID, key string, fingerprint []byte, ttl time.Duration) (idempotency.Record, error) {
	var rec idempotency.Record
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO idempotency_keys (tenant_id, key, fingerprint, state, expires_at)
			VALUES ($1, $2, $3, 'PROCESSING', now() + $4 * interval '1 millisecond')
			ON CONFLICT (tenant_id, key) DO UPDATE
			SET fingerprint = EXCLUDED.fingerprint, state = 'PROCESSING', response_status = NULL, response_body = NULL,
			    created_at = now(), expires_at = EXCLUDED.expires_at
			WHERE idempotency_keys.expires_at < now()`, tenantID, key, fingerprint, ttl.Milliseconds())
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 1 {
			rec = idempotency.Record{State: idempotency.StateNew}
			return nil
		}
		var (
			stored []byte
			state  string
			status *int
			body   []byte
		)
		if err := tx.QueryRow(ctx, `SELECT fingerprint, state, response_status, response_body FROM idempotency_keys
			WHERE tenant_id = $1 AND key = $2`, tenantID, key).Scan(&stored, &state, &status, &body); err != nil {
			return err
		}
		switch {
		case !bytes.Equal(stored, fingerprint):
			rec = idempotency.Record{State: idempotency.StateMismatch}
		case state == "PROCESSING":
			rec = idempotency.Record{State: idempotency.StateInProgress}
		default:
			rec = idempotency.Record{State: idempotency.StateCompleted, Status: *status, Body: body}
		}
		return nil
	})
	return rec, mapErr(err)
}

func (s *Store) CompleteIdempotent(ctx context.Context, tenantID, key string, status int, body []byte) error {
	_, err := s.pool.Exec(ctx, `UPDATE idempotency_keys SET state = 'COMPLETED', response_status = $3, response_body = $4
		WHERE tenant_id = $1 AND key = $2`, tenantID, key, status, body)
	return err
}

func (s *Store) ReleaseIdempotent(ctx context.Context, tenantID, key string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM idempotency_keys WHERE tenant_id = $1 AND key = $2 AND state = 'PROCESSING'`, tenantID, key)
	return err
}

func (s *Store) PurgeExpiredIdempotencyKeys(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM idempotency_keys WHERE expires_at < now()`)
	return tag.RowsAffected(), err
}
