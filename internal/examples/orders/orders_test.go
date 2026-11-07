package orders

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/MatheusAndrell/go-flow/pkg/handler"
)

func call(t *testing.T, reg *handler.Registry, name string, req handler.Request) (json.RawMessage, error) {
	t.Helper()
	h, ok := reg.Lookup(name)
	if !ok {
		t.Fatalf("handler %s not registered", name)
	}
	return h.Handle(context.Background(), req)
}

func TestSimulatorsAreIdempotent(t *testing.T) {
	inv := NewInventory(map[string]int{"A": 5})
	pay := NewPayments()
	reg := handler.NewRegistry()
	if err := Register(reg, inv, pay); err != nil {
		t.Fatal(err)
	}
	input := json.RawMessage(`{"order_id":"o","items":[{"sku":"A","quantity":2}],"amount":10}`)
	req := handler.Request{WorkflowInput: input, IdempotencyKey: "e/ACTION/reserve", Attempt: 1}
	out1, err := call(t, reg, "inventory.reserve", req)
	if err != nil {
		t.Fatal(err)
	}
	req.Attempt = 2
	out2, _ := call(t, reg, "inventory.reserve", req)
	if string(out1) != string(out2) || inv.Available("A") != 3 {
		t.Fatalf("redelivered reserve must not reserve twice: stock=%d", inv.Available("A"))
	}
	if _, err := call(t, reg, "inventory.release", handler.Request{Compensation: true, CompensatedOutput: out1}); err != nil {
		t.Fatal(err)
	}
	_, _ = call(t, reg, "inventory.release", handler.Request{Compensation: true, CompensatedOutput: out1})
	if inv.Available("A") != 5 {
		t.Fatalf("release must be idempotent: stock=%d", inv.Available("A"))
	}

	charge := handler.Request{WorkflowInput: input, IdempotencyKey: "e/ACTION/charge"}
	c1, _ := call(t, reg, "payment.charge", charge)
	_, _ = call(t, reg, "payment.charge", charge)
	if pay.Balance() != 10 {
		t.Fatalf("duplicate charge: balance=%v", pay.Balance())
	}
	_, _ = call(t, reg, "payment.refund", handler.Request{WorkflowInput: input, CompensatedOutput: c1})
	if pay.Balance() != 0 {
		t.Fatal("refund failed")
	}
}

func TestValidationAndFailureInjection(t *testing.T) {
	reg := handler.NewRegistry()
	_ = Register(reg, NewInventory(map[string]int{"A": 1}), NewPayments())
	if _, err := call(t, reg, "orders.validate", handler.Request{WorkflowInput: json.RawMessage(`{}`)}); !handler.IsPermanent(err) {
		t.Fatal("invalid orders must fail permanently")
	}
	transient := json.RawMessage(`{"order_id":"o","simulate":{"invoice":"transient:1"}}`)
	if _, err := call(t, reg, "invoice.issue", handler.Request{WorkflowInput: transient, Attempt: 1}); err == nil || handler.IsPermanent(err) {
		t.Fatal("first attempt must fail with a retryable error")
	}
	if _, err := call(t, reg, "invoice.issue", handler.Request{WorkflowInput: transient, Attempt: 2}); err != nil {
		t.Fatal("second attempt must succeed")
	}
	tooMany := json.RawMessage(`{"order_id":"o","items":[{"sku":"A","quantity":5}],"amount":1}`)
	if _, err := call(t, reg, "inventory.reserve", handler.Request{WorkflowInput: tooMany, IdempotencyKey: "k"}); !handler.IsPermanent(err) {
		t.Fatal("insufficient stock is a permanent failure")
	}
}
