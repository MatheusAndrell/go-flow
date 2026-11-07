// Package access implements the Identity and Access use cases: tenant and API
// key administration and request authentication.
package access

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/application"
	"github.com/MatheusAndrell/go-flow/internal/domain/identity"
)

type Store interface {
	CreateTenant(ctx context.Context, t identity.Tenant) error
	GetTenantByName(ctx context.Context, name string) (identity.Tenant, error)
	CreateAPIKey(ctx context.Context, k identity.APIKey) error
	FindAPIKeyByHash(ctx context.Context, hash []byte) (identity.APIKey, error)
	RevokeAPIKey(ctx context.Context, id string, at time.Time) error
}

type Admin struct {
	store Store
	newID func() string
	now   func() time.Time
}

func NewAdmin(store Store, newID func() string, now func() time.Time) *Admin {
	return &Admin{store: store, newID: newID, now: now}
}

// EnsureTenant returns the tenant with that name, creating it if needed.
func (a *Admin) EnsureTenant(ctx context.Context, name string) (identity.Tenant, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 128 {
		return identity.Tenant{}, application.Invalid("tenant name must have 1-128 characters")
	}
	t, err := a.store.GetTenantByName(ctx, name)
	if err == nil {
		return t, nil
	}
	if !errors.Is(err, application.ErrNotFound) {
		return identity.Tenant{}, err
	}
	t = identity.Tenant{ID: a.newID(), Name: name, CreatedAt: a.now()}
	if err := a.store.CreateTenant(ctx, t); err != nil {
		if errors.Is(err, application.ErrConflict) {
			return a.store.GetTenantByName(ctx, name)
		}
		return identity.Tenant{}, err
	}
	return t, nil
}

// CreateAPIKey returns the plaintext secret; only its hash is stored.
func (a *Admin) CreateAPIKey(ctx context.Context, tenantID, name string, perms []string) (string, identity.APIKey, error) {
	parsed, err := identity.ParsePermissions(perms)
	if err != nil {
		return "", identity.APIKey{}, application.Invalid("%s", err.Error())
	}
	secret, key, err := identity.GenerateKey(a.newID(), tenantID, name, parsed, a.now())
	if err != nil {
		return "", identity.APIKey{}, err
	}
	if err := a.store.CreateAPIKey(ctx, key); err != nil {
		return "", identity.APIKey{}, err
	}
	return secret, key, nil
}

func (a *Admin) RevokeAPIKey(ctx context.Context, id string) error {
	return a.store.RevokeAPIKey(ctx, id, a.now())
}

// Authenticator resolves bearer tokens to principals. Successful lookups are
// cached for a short TTL to keep the hot path off the database; a revoked key
// therefore stays valid for at most CacheTTL.
type Authenticator struct {
	store Store
	ttl   time.Duration
	now   func() time.Time

	mu    sync.RWMutex
	cache map[string]cachedPrincipal
}

type cachedPrincipal struct {
	p       identity.Principal
	expires time.Time
}

const maxCacheEntries = 10_000

func NewAuthenticator(store Store, ttl time.Duration, now func() time.Time) *Authenticator {
	return &Authenticator{store: store, ttl: ttl, now: now, cache: map[string]cachedPrincipal{}}
}

func (a *Authenticator) Authenticate(ctx context.Context, token string) (identity.Principal, error) {
	if !identity.LooksLikeKey(token) {
		return identity.Principal{}, identity.ErrUnauthenticated
	}
	hash := string(identity.HashKey(token))
	now := a.now()
	a.mu.RLock()
	c, ok := a.cache[hash]
	a.mu.RUnlock()
	if ok && now.Before(c.expires) {
		return c.p, nil
	}
	key, err := a.store.FindAPIKeyByHash(ctx, []byte(hash))
	if errors.Is(err, application.ErrNotFound) {
		return identity.Principal{}, identity.ErrUnauthenticated
	}
	if err != nil {
		return identity.Principal{}, err
	}
	p, err := key.Principal(token)
	if err != nil {
		return identity.Principal{}, err
	}
	if a.ttl > 0 {
		a.mu.Lock()
		if len(a.cache) >= maxCacheEntries {
			clear(a.cache)
		}
		a.cache[hash] = cachedPrincipal{p: p, expires: now.Add(a.ttl)}
		a.mu.Unlock()
	}
	return p, nil
}
