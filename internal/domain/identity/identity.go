// Package identity holds the Identity and Access bounded context: tenants,
// API keys and the permissions they grant.
package identity

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

type Permission string

const (
	PermWorkflowsRead   Permission = "workflows:read"
	PermWorkflowsWrite  Permission = "workflows:write"
	PermExecutionsRead  Permission = "executions:read"
	PermExecutionsWrite Permission = "executions:write"
)

var AllPermissions = []Permission{PermWorkflowsRead, PermWorkflowsWrite, PermExecutionsRead, PermExecutionsWrite}

var (
	ErrUnauthenticated = errors.New("unauthenticated")
	ErrForbidden       = errors.New("forbidden")
)

func ParsePermissions(values []string) ([]Permission, error) {
	out := make([]Permission, 0, len(values))
	for _, v := range values {
		p := Permission(strings.TrimSpace(v))
		if !slices.Contains(AllPermissions, p) {
			return nil, fmt.Errorf("unknown permission %q", v)
		}
		if !slices.Contains(out, p) {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("at least one permission is required")
	}
	return out, nil
}

type Tenant struct {
	ID        string
	Name      string
	CreatedAt time.Time
}

// Principal is the authenticated caller. Every application use case receives
// one and scopes all data access to Principal.TenantID.
type Principal struct {
	TenantID    string
	KeyID       string
	Name        string
	Permissions []Permission
}

func (p Principal) Require(perm Permission) error {
	if p.TenantID == "" {
		return ErrUnauthenticated
	}
	if !slices.Contains(p.Permissions, perm) {
		return fmt.Errorf("%w: missing permission %s", ErrForbidden, perm)
	}
	return nil
}

// APIKey stores only a SHA-256 hash of the secret. API keys are 32 random
// bytes, so a fast hash is sufficient (no dictionary attack is possible).
type APIKey struct {
	ID          string
	TenantID    string
	Name        string
	Prefix      string
	Hash        []byte
	Permissions []Permission
	CreatedAt   time.Time
	RevokedAt   *time.Time
}

const keyPrefix = "gf_"

// GenerateKey returns the plaintext secret (shown once) and its stored form.
func GenerateKey(id, tenantID, name string, perms []Permission, now time.Time) (string, APIKey, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", APIKey{}, err
	}
	secret := keyPrefix + base64.RawURLEncoding.EncodeToString(buf)
	return secret, APIKey{
		ID: id, TenantID: tenantID, Name: name, Prefix: secret[:10],
		Hash: HashKey(secret), Permissions: perms, CreatedAt: now,
	}, nil
}

func HashKey(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

// LooksLikeKey rejects obviously malformed tokens before touching storage.
func LooksLikeKey(secret string) bool {
	return strings.HasPrefix(secret, keyPrefix) && len(secret) > 20 && len(secret) < 128
}

// Principal converts a stored key into a principal, verifying the hash in constant time.
func (k APIKey) Principal(secret string) (Principal, error) {
	if k.RevokedAt != nil || subtle.ConstantTimeCompare(k.Hash, HashKey(secret)) != 1 {
		return Principal{}, ErrUnauthenticated
	}
	return Principal{TenantID: k.TenantID, KeyID: k.ID, Name: k.Name, Permissions: k.Permissions}, nil
}
