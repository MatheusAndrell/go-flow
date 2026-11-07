// Package orders provides the handlers of the order-processing example
// workflow. Inventory and payments are in-process simulators of external
// systems: they exist to demonstrate idempotent side effects, retries and
// compensation, and are only registered when GOFLOW_EXAMPLE_HANDLERS=true.
//
// Failures can be injected per step through the workflow input:
//
//	"simulate": {"invoice": "permanent" | "transient:2" | "panic" | "slow:3s"}
package orders

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MatheusAndrell/go-flow/pkg/handler"
)

type Item struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type Order struct {
	OrderID    string            `json:"order_id"`
	CustomerID string            `json:"customer_id"`
	Items      []Item            `json:"items"`
	Amount     float64           `json:"amount"`
	Simulate   map[string]string `json:"simulate,omitempty"`
}

// Inventory simulates a stock service. Reservations are keyed by the
// GoFlow idempotency key, so a retried reserve never reserves twice.
type Inventory struct {
	mu           sync.Mutex
	stock        map[string]int
	reservations map[string][]Item
}

func NewInventory(stock map[string]int) *Inventory {
	return &Inventory{stock: stock, reservations: map[string][]Item{}}
}

func (inv *Inventory) Reserve(key string, items []Item) (string, error) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	if _, done := inv.reservations[key]; done {
		return key, nil
	}
	for _, it := range items {
		if inv.stock[it.SKU] < it.Quantity {
			return "", handler.Permanentf("insufficient stock for %s", it.SKU)
		}
	}
	for _, it := range items {
		inv.stock[it.SKU] -= it.Quantity
	}
	inv.reservations[key] = items
	return key, nil
}

func (inv *Inventory) Release(reservationID string) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	items, ok := inv.reservations[reservationID]
	if !ok {
		return
	}
	for _, it := range items {
		inv.stock[it.SKU] += it.Quantity
	}
	delete(inv.reservations, reservationID)
}

func (inv *Inventory) Available(sku string) int {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	return inv.stock[sku]
}

// Payments simulates a payment gateway with idempotent charges and refunds.
type Payments struct {
	mu      sync.Mutex
	charges map[string]float64
	refunds map[string]bool
}

func NewPayments() *Payments {
	return &Payments{charges: map[string]float64{}, refunds: map[string]bool{}}
}

func (p *Payments) Charge(key string, amount float64) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.charges[key]; !ok {
		p.charges[key] = amount
	}
	return key
}

func (p *Payments) Refund(chargeID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.charges[chargeID]; ok {
		p.refunds[chargeID] = true
	}
}

// Balance returns charged minus refunded amounts.
func (p *Payments) Balance() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	total := 0.0
	for id, amount := range p.charges {
		if !p.refunds[id] {
			total += amount
		}
	}
	return total
}

// Register installs every example handler.
func Register(reg *handler.Registry, inv *Inventory, pay *Payments) error {
	handlers := map[string]handler.Func{
		"orders.validate":   validate,
		"inventory.reserve": reserve(inv),
		"inventory.release": release(inv),
		"payment.charge":    charge(pay),
		"payment.refund":    refund(pay),
		"invoice.issue":     issueInvoice,
		"notification.send": notify,
	}
	for name, h := range handlers {
		if err := reg.Register(name, h); err != nil {
			return err
		}
	}
	return nil
}

func decodeOrder(req handler.Request) (Order, error) {
	var o Order
	if err := json.Unmarshal(req.WorkflowInput, &o); err != nil {
		return o, handler.Permanentf("decode order: %w", err)
	}
	return o, nil
}

// inject applies the failure simulation configured for a step.
func inject(ctx context.Context, o Order, step string, attempt int) error {
	mode := o.Simulate[step]
	switch {
	case mode == "":
		return nil
	case mode == "permanent":
		return handler.Permanentf("simulated permanent failure in %s", step)
	case mode == "panic":
		panic("simulated panic in " + step)
	case strings.HasPrefix(mode, "transient:"):
		n, err := strconv.Atoi(strings.TrimPrefix(mode, "transient:"))
		if err == nil && attempt <= n {
			return fmt.Errorf("simulated transient failure in %s (attempt %d of %d failing)", step, attempt, n)
		}
	case strings.HasPrefix(mode, "slow:"):
		d, err := time.ParseDuration(strings.TrimPrefix(mode, "slow:"))
		if err != nil {
			return handler.Permanentf("bad slow duration: %w", err)
		}
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func validate(ctx context.Context, req handler.Request) (json.RawMessage, error) {
	o, err := decodeOrder(req)
	if err != nil {
		return nil, err
	}
	if err := inject(ctx, o, "validate", req.Attempt); err != nil {
		return nil, err
	}
	var problems []string
	if o.OrderID == "" {
		problems = append(problems, "order_id is required")
	}
	if len(o.Items) == 0 {
		problems = append(problems, "at least one item is required")
	}
	for _, it := range o.Items {
		if it.SKU == "" || it.Quantity <= 0 {
			problems = append(problems, "items need a sku and a positive quantity")
			break
		}
	}
	if o.Amount <= 0 {
		problems = append(problems, "amount must be positive")
	}
	if len(problems) > 0 {
		return nil, handler.Permanent(errors.New(strings.Join(problems, "; ")))
	}
	return json.Marshal(map[string]any{"valid": true, "order_id": o.OrderID})
}

func reserve(inv *Inventory) handler.Func {
	return func(ctx context.Context, req handler.Request) (json.RawMessage, error) {
		o, err := decodeOrder(req)
		if err != nil {
			return nil, err
		}
		if err := inject(ctx, o, "reserve", req.Attempt); err != nil {
			return nil, err
		}
		id, err := inv.Reserve(req.IdempotencyKey, o.Items)
		if err != nil {
			return nil, err
		}
		return json.Marshal(map[string]string{"reservation_id": id})
	}
}

func release(inv *Inventory) handler.Func {
	return func(ctx context.Context, req handler.Request) (json.RawMessage, error) {
		var out struct {
			ReservationID string `json:"reservation_id"`
		}
		if err := json.Unmarshal(req.CompensatedOutput, &out); err != nil || out.ReservationID == "" {
			return nil, handler.Permanentf("compensation needs the reservation id")
		}
		inv.Release(out.ReservationID)
		return json.Marshal(map[string]string{"released": out.ReservationID})
	}
}

func charge(pay *Payments) handler.Func {
	return func(ctx context.Context, req handler.Request) (json.RawMessage, error) {
		o, err := decodeOrder(req)
		if err != nil {
			return nil, err
		}
		if err := inject(ctx, o, "charge", req.Attempt); err != nil {
			return nil, err
		}
		if o.Simulate["charge"] == "declined" {
			return nil, handler.Permanentf("card declined")
		}
		id := pay.Charge(req.IdempotencyKey, o.Amount)
		return json.Marshal(map[string]any{"charge_id": id, "amount": o.Amount})
	}
}

func refund(pay *Payments) handler.Func {
	return func(ctx context.Context, req handler.Request) (json.RawMessage, error) {
		o, err := decodeOrder(req)
		if err != nil {
			return nil, err
		}
		if err := inject(ctx, o, "refund", req.Attempt); err != nil {
			return nil, err
		}
		var out struct {
			ChargeID string `json:"charge_id"`
		}
		if err := json.Unmarshal(req.CompensatedOutput, &out); err != nil || out.ChargeID == "" {
			return nil, handler.Permanentf("compensation needs the charge id")
		}
		pay.Refund(out.ChargeID)
		return json.Marshal(map[string]string{"refunded": out.ChargeID})
	}
}

func issueInvoice(ctx context.Context, req handler.Request) (json.RawMessage, error) {
	o, err := decodeOrder(req)
	if err != nil {
		return nil, err
	}
	if err := inject(ctx, o, "invoice", req.Attempt); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]string{"invoice_number": "INV-" + o.OrderID})
}

func notify(ctx context.Context, req handler.Request) (json.RawMessage, error) {
	o, err := decodeOrder(req)
	if err != nil {
		return nil, err
	}
	if err := inject(ctx, o, "notify", req.Attempt); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"notified": o.CustomerID, "dependencies": len(req.Dependencies)})
}
