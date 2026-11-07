package postgres

import (
	"context"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/application"
	"github.com/MatheusAndrell/go-flow/internal/domain/identity"
)

func (s *Store) CreateTenant(ctx context.Context, t identity.Tenant) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO tenants (id, name, created_at) VALUES ($1, $2, $3)`, t.ID, t.Name, t.CreatedAt)
	return mapErr(err)
}

func (s *Store) GetTenantByName(ctx context.Context, name string) (identity.Tenant, error) {
	var t identity.Tenant
	err := s.pool.QueryRow(ctx, `SELECT id, name, created_at FROM tenants WHERE name = $1`, name).Scan(&t.ID, &t.Name, &t.CreatedAt)
	return t, mapErr(err)
}

func (s *Store) CreateAPIKey(ctx context.Context, k identity.APIKey) error {
	perms := make([]string, len(k.Permissions))
	for i, p := range k.Permissions {
		perms[i] = string(p)
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO api_keys (id, tenant_id, name, prefix, key_hash, permissions, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`, k.ID, k.TenantID, k.Name, k.Prefix, k.Hash, perms, k.CreatedAt)
	return mapErr(err)
}

func (s *Store) FindAPIKeyByHash(ctx context.Context, hash []byte) (identity.APIKey, error) {
	var (
		k     identity.APIKey
		perms []string
	)
	err := s.pool.QueryRow(ctx, `SELECT id, tenant_id, name, prefix, key_hash, permissions, created_at, revoked_at
		FROM api_keys WHERE key_hash = $1`, hash).Scan(&k.ID, &k.TenantID, &k.Name, &k.Prefix, &k.Hash, &perms, &k.CreatedAt, &k.RevokedAt)
	if err != nil {
		return k, mapErr(err)
	}
	for _, p := range perms {
		k.Permissions = append(k.Permissions, identity.Permission(p))
	}
	return k, nil
}

func (s *Store) RevokeAPIKey(ctx context.Context, id string, at time.Time) error {
	tag, err := s.pool.Exec(ctx, `UPDATE api_keys SET revoked_at = $2 WHERE id = $1 AND revoked_at IS NULL`, id, at)
	if err != nil {
		return mapErr(err)
	}
	if tag.RowsAffected() == 0 {
		return application.ErrNotFound
	}
	return nil
}
