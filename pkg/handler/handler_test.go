package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

func TestPermanentClassification(t *testing.T) {
	base := errors.New("declined")
	err := fmt.Errorf("charge: %w", Permanent(base))
	if !IsPermanent(err) || !errors.Is(err, base) {
		t.Fatal("wrapped permanent error must stay permanent and unwrap to its cause")
	}
	if IsPermanent(base) || Permanent(nil) != nil {
		t.Fatal("plain errors are retryable")
	}
	if !IsPermanent(Permanentf("bad %d", 1)) {
		t.Fatal("Permanentf must be permanent")
	}
}

func TestRegistry(t *testing.T) {
	r := NewRegistry()
	noop := Func(func(context.Context, Request) (json.RawMessage, error) { return nil, nil })
	if err := r.Register("payment.charge", noop); err != nil {
		t.Fatal(err)
	}
	if err := r.Register("payment.charge", noop); err == nil {
		t.Fatal("duplicate registration must fail")
	}
	if err := r.Register("Bad Name", noop); err == nil {
		t.Fatal("invalid name must fail")
	}
	if err := r.Register("x", nil); err == nil {
		t.Fatal("nil handler must fail")
	}
	r.MustRegister("a.first", noop)
	if _, ok := r.Lookup("payment.charge"); !ok {
		t.Fatal("lookup failed")
	}
	if names := r.Names(); len(names) != 2 || names[0] != "a.first" {
		t.Fatalf("names not sorted: %v", names)
	}
}
