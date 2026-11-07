package identity

import (
	"errors"
	"testing"
	"time"
)

func TestGeneratedKeyAuthenticates(t *testing.T) {
	secret, key, err := GenerateKey("k1", "t1", "ci", []Permission{PermExecutionsRead}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if !LooksLikeKey(secret) {
		t.Fatalf("generated key has unexpected format: %s", secret)
	}
	p, err := key.Principal(secret)
	if err != nil {
		t.Fatal(err)
	}
	if p.TenantID != "t1" || p.KeyID != "k1" {
		t.Fatalf("unexpected principal %+v", p)
	}
	if _, err := key.Principal(secret + "x"); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("wrong secret must be rejected")
	}
	now := time.Now()
	key.RevokedAt = &now
	if _, err := key.Principal(secret); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("revoked key must be rejected")
	}
}

func TestRequire(t *testing.T) {
	p := Principal{TenantID: "t", Permissions: []Permission{PermWorkflowsRead}}
	if err := p.Require(PermWorkflowsRead); err != nil {
		t.Fatal(err)
	}
	if err := p.Require(PermWorkflowsWrite); !errors.Is(err, ErrForbidden) {
		t.Fatalf("expected forbidden, got %v", err)
	}
	if err := (Principal{}).Require(PermWorkflowsRead); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("anonymous principal must be unauthenticated")
	}
}

func TestParsePermissions(t *testing.T) {
	perms, err := ParsePermissions([]string{"workflows:read", " executions:write", "workflows:read"})
	if err != nil || len(perms) != 2 {
		t.Fatalf("got %v %v", perms, err)
	}
	if _, err := ParsePermissions([]string{"admin:all"}); err == nil {
		t.Fatal("unknown permission must be rejected")
	}
	if _, err := ParsePermissions(nil); err == nil {
		t.Fatal("empty permissions must be rejected")
	}
}
