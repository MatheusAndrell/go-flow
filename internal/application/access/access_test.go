package access

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/application"
	"github.com/MatheusAndrell/go-flow/internal/domain/identity"
)

type memStore struct {
	tenants map[string]identity.Tenant
	keys    map[string]identity.APIKey
	lookups int
}

func newMemStore() *memStore {
	return &memStore{tenants: map[string]identity.Tenant{}, keys: map[string]identity.APIKey{}}
}

func (m *memStore) CreateTenant(_ context.Context, t identity.Tenant) error {
	if _, ok := m.tenants[t.Name]; ok {
		return application.ErrConflict
	}
	m.tenants[t.Name] = t
	return nil
}

func (m *memStore) GetTenantByName(_ context.Context, name string) (identity.Tenant, error) {
	t, ok := m.tenants[name]
	if !ok {
		return t, application.ErrNotFound
	}
	return t, nil
}

func (m *memStore) CreateAPIKey(_ context.Context, k identity.APIKey) error {
	m.keys[string(k.Hash)] = k
	return nil
}

func (m *memStore) FindAPIKeyByHash(_ context.Context, hash []byte) (identity.APIKey, error) {
	m.lookups++
	k, ok := m.keys[string(hash)]
	if !ok {
		return k, application.ErrNotFound
	}
	return k, nil
}

func (m *memStore) RevokeAPIKey(_ context.Context, id string, at time.Time) error {
	for h, k := range m.keys {
		if k.ID == id {
			k.RevokedAt = &at
			m.keys[h] = k
			return nil
		}
	}
	return application.ErrNotFound
}

func ids() func() string {
	n := 0
	return func() string { n++; return fmt.Sprint(n) }
}

func TestAdminAndAuthenticator(t *testing.T) {
	ctx := context.Background()
	store := newMemStore()
	now := time.Unix(0, 0)
	clock := func() time.Time { return now }
	admin := NewAdmin(store, ids(), clock)

	t1, err := admin.EnsureTenant(ctx, "acme")
	if err != nil {
		t.Fatal(err)
	}
	if again, _ := admin.EnsureTenant(ctx, "acme"); again.ID != t1.ID {
		t.Fatal("EnsureTenant must be idempotent")
	}
	if _, _, err := admin.CreateAPIKey(ctx, t1.ID, "ci", []string{"root"}); err == nil {
		t.Fatal("unknown permissions must be rejected")
	}
	secret, key, err := admin.CreateAPIKey(ctx, t1.ID, "ci", []string{"executions:read"})
	if err != nil {
		t.Fatal(err)
	}

	auth := NewAuthenticator(store, time.Minute, clock)
	p, err := auth.Authenticate(ctx, secret)
	if err != nil || p.TenantID != t1.ID {
		t.Fatalf("authenticate: %v %+v", err, p)
	}
	_, _ = auth.Authenticate(ctx, secret)
	if store.lookups != 1 {
		t.Fatalf("second lookup must hit the cache, got %d store lookups", store.lookups)
	}
	if err := admin.RevokeAPIKey(ctx, key.ID); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if _, err := auth.Authenticate(ctx, secret); !errors.Is(err, identity.ErrUnauthenticated) {
		t.Fatal("revoked key must be rejected once the cache entry expires")
	}
	if _, err := auth.Authenticate(ctx, "garbage"); !errors.Is(err, identity.ErrUnauthenticated) {
		t.Fatal("malformed tokens must be rejected")
	}
	if _, err := auth.Authenticate(ctx, "gf_"+"unknownunknownunknownunknown"); !errors.Is(err, identity.ErrUnauthenticated) {
		t.Fatal("unknown keys must be rejected")
	}
}
