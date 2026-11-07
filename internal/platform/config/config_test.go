package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDefaultsAndOverrides(t *testing.T) {
	t.Setenv("GOFLOW_DATABASE_URL", "postgres://user:secret@db:5432/goflow")
	t.Setenv("GOFLOW_WORKER_CONCURRENCY", "25")
	t.Setenv("GOFLOW_WORKER_LEASE_DURATION", "45s")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.WorkerConcurrency != 25 || c.WorkerLeaseDuration != 45*time.Second || c.HTTPAddr != ":8080" {
		t.Fatalf("unexpected config %+v", c)
	}
	if strings.Contains(c.RedactedDatabaseURL(), "secret") {
		t.Fatal("password must be redacted")
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	t.Setenv("GOFLOW_DATABASE_URL", "")
	t.Setenv("GOFLOW_WORKER_CONCURRENCY", "abc")
	t.Setenv("GOFLOW_WORKER_HEARTBEAT_INTERVAL", "1m")
	if _, err := Load(); err == nil {
		t.Fatal("expected errors")
	}
}
