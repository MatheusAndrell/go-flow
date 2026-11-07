package integration

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/examples/orders"
	"github.com/MatheusAndrell/go-flow/pkg/handler"
)

// The shipped example definition must work against the example handlers,
// including stock and payment being restored by compensation.
func TestOrderProcessingExample(t *testing.T) {
	e := newEnv(t)
	raw, err := os.ReadFile("../../examples/order-processing.json")
	if err != nil {
		t.Fatal(err)
	}
	var def map[string]any
	if err := json.Unmarshal(raw, &def); err != nil {
		t.Fatal(err)
	}
	// Prefix handler names so this test's worker is isolated from others.
	text := string(raw)
	for _, h := range []string{"orders.validate", "inventory.reserve", "inventory.release", "payment.charge", "payment.refund", "invoice.issue", "notification.send"} {
		text = strings.ReplaceAll(text, `"`+h+`"`, `"`+e.prefix+"-"+h+`"`)
	}
	_ = json.Unmarshal([]byte(text), &def)

	inv := orders.NewInventory(map[string]int{"SKU-1": 10})
	pay := orders.NewPayments()
	base := handler.NewRegistry()
	if err := orders.Register(base, inv, pay); err != nil {
		t.Fatal(err)
	}
	reg := handler.NewRegistry()
	for _, name := range base.Names() {
		h, _ := base.Lookup(name)
		reg.MustRegister(e.prefix+"-"+name, h)
	}
	runWorker(t, reg, workerOpts{})
	wf := e.expect(e.do("POST", "/api/v1/workflows", def), 201)["id"].(string)
	e.expect(e.do("POST", "/api/v1/workflows/"+wf+"/publish", nil), 200)

	order := func(id string, simulate map[string]string) map[string]any {
		return map[string]any{"order_id": id, "customer_id": "c-1", "amount": 99.9,
			"items": []map[string]any{{"sku": "SKU-1", "quantity": 2}}, "simulate": simulate}
	}
	ok := e.waitStatus(e.start(wf, order("o-1", map[string]string{"invoice": "transient:1"})), 15*time.Second, "COMPLETED")
	if inv.Available("SKU-1") != 8 || pay.Balance() != 99.9 {
		t.Fatalf("successful order must keep reservation and charge: stock=%d balance=%v", inv.Available("SKU-1"), pay.Balance())
	}
	if ok["output"].(map[string]any)["invoice"].(map[string]any)["invoice_number"] != "INV-o-1" {
		t.Fatalf("unexpected output %v", ok["output"])
	}

	e.waitStatus(e.start(wf, order("o-2", map[string]string{"invoice": "permanent"})), 15*time.Second, "COMPENSATED")
	if inv.Available("SKU-1") != 8 || pay.Balance() != 99.9 {
		t.Fatalf("failed order must release stock and refund: stock=%d balance=%v", inv.Available("SKU-1"), pay.Balance())
	}

	bad := e.waitStatus(e.start(wf, map[string]any{"order_id": ""}), 15*time.Second, "FAILED")
	if !strings.Contains(bad["error"].(string), "order_id is required") {
		t.Fatalf("validation error not surfaced: %v", bad["error"])
	}
}
