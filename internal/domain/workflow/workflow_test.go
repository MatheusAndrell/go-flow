package workflow

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/domain/retry"
)

func orderSpec() Spec {
	return Spec{Steps: []Step{
		{Key: "validate", Handler: "orders.validate"},
		{Key: "reserve", Handler: "inventory.reserve", DependsOn: []string{"validate"}, Compensation: &Compensation{Handler: "inventory.release"}},
		{Key: "charge", Handler: "payment.charge", DependsOn: []string{"validate"}, Compensation: &Compensation{Handler: "payment.refund"}},
		{Key: "confirm", Handler: "notification.send", DependsOn: []string{"reserve", "charge"}},
	}}
}

func TestValidSpec(t *testing.T) {
	if err := orderSpec().Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTopologicalOrderRespectsDependencies(t *testing.T) {
	order, err := orderSpec().TopologicalOrder()
	if err != nil {
		t.Fatal(err)
	}
	pos := map[string]int{}
	for i, k := range order {
		pos[k] = i
	}
	for _, s := range orderSpec().Steps {
		for _, d := range s.DependsOn {
			if pos[d] >= pos[s.Key] {
				t.Fatalf("%s scheduled before its dependency %s: %v", s.Key, d, order)
			}
		}
	}
}

func TestCycleIsRejected(t *testing.T) {
	spec := Spec{Steps: []Step{
		{Key: "a", Handler: "h", DependsOn: []string{"c"}},
		{Key: "b", Handler: "h", DependsOn: []string{"a"}},
		{Key: "c", Handler: "h", DependsOn: []string{"b"}},
		{Key: "d", Handler: "h"},
	}}
	err := spec.Validate()
	var verr *ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("expected ValidationError, got %v", err)
	}
	if !strings.Contains(err.Error(), "cycle") || !strings.Contains(err.Error(), "a, b, c") {
		t.Fatalf("cycle not reported with involved steps: %v", err)
	}
	if _, err := spec.TopologicalOrder(); !errors.Is(err, ErrCycle) {
		t.Fatalf("expected ErrCycle, got %v", err)
	}
}

func TestStructuralProblemsAreAllReported(t *testing.T) {
	spec := Spec{Steps: []Step{
		{Key: "Bad Key", Handler: "h"},
		{Key: "x", Handler: ""},
		{Key: "x", Handler: "h", DependsOn: []string{"x", "missing"}},
		{Key: "y", Handler: "h", Timeout: Duration(-time.Second), Retry: &retry.Policy{}},
		{Key: "z", Handler: "h", Params: json.RawMessage(`{bad`)},
	}}
	err := spec.Validate()
	var verr *ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("expected ValidationError, got %v", err)
	}
	for _, fragment := range []string{"key \"Bad Key\"", "handler \"\"", "duplicated", "depends on itself", "unknown step \"missing\"", "timeout", "retry", "params is not valid JSON"} {
		if !strings.Contains(err.Error(), fragment) {
			t.Errorf("missing problem %q in %v", fragment, err)
		}
	}
}

func TestEmptySpecIsRejected(t *testing.T) {
	if err := (Spec{}).Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestEffectivePoliciesAndTimeouts(t *testing.T) {
	custom := retry.Policy{MaxAttempts: 7, InitialInterval: time.Second, MaxInterval: time.Second, Multiplier: 1}
	spec := Spec{
		Settings: Settings{DefaultTimeout: Duration(time.Minute), DefaultRetry: &custom},
		Steps:    []Step{{Key: "a", Handler: "h"}, {Key: "b", Handler: "h", Timeout: Duration(time.Second), Retry: &retry.Policy{MaxAttempts: 1}}},
	}
	if spec.TimeoutFor(spec.Steps[0]) != time.Minute || spec.TimeoutFor(spec.Steps[1]) != time.Second {
		t.Fatal("timeout resolution is wrong")
	}
	if spec.RetryFor(spec.Steps[0]).MaxAttempts != 7 || spec.RetryFor(spec.Steps[1]).MaxAttempts != 1 {
		t.Fatal("retry resolution is wrong")
	}
	if (Spec{}).TimeoutFor(Step{}) != DefaultTimeout {
		t.Fatal("global default timeout not applied")
	}
}

func TestSpecJSONRoundTrip(t *testing.T) {
	raw := `{"steps":[{"key":"a","handler":"h.one","timeout":"30s","retry":{"max_attempts":5,"initial_interval":"2s","max_interval":"1m","multiplier":2,"jitter":0.1}}],"settings":{"max_parallelism":2}}`
	var spec Spec
	if err := json.Unmarshal([]byte(raw), &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Steps[0].Timeout.Std() != 30*time.Second || spec.Steps[0].Retry.MaxAttempts != 5 || spec.Settings.MaxParallelism != 2 {
		t.Fatalf("unexpected decode: %+v", spec)
	}
	if err := spec.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestVersionLifecycle(t *testing.T) {
	now := time.Now()
	w, err := New("wf", "v1", "tenant", "orders", "order flow", orderSpec(), now)
	if err != nil {
		t.Fatal(err)
	}
	if w.Draft() == nil || w.Draft().Number != 1 {
		t.Fatal("first version must be a draft")
	}
	if _, err := w.LatestPublished(); !errors.Is(err, ErrNoPublishedVersion) {
		t.Fatal("expected no published version")
	}
	if _, err := w.NewVersion("v2", nil, now); !errors.Is(err, ErrDraftExists) {
		t.Fatal("only one draft is allowed")
	}

	unknown := func(h string) bool { return h != "payment.refund" }
	if _, err := w.Publish(unknown, now); err == nil || !strings.Contains(err.Error(), "payment.refund") {
		t.Fatalf("publish must reject unregistered handlers, got %v", err)
	}
	all := func(string) bool { return true }
	v1, err := w.Publish(all, now)
	if err != nil {
		t.Fatal(err)
	}
	if v1.Status != VersionPublished || v1.PublishedAt == nil {
		t.Fatal("version not published")
	}
	if err := w.UpdateDraft(orderSpec(), now); !errors.Is(err, ErrNoDraft) {
		t.Fatal("published version must be immutable")
	}

	changed := orderSpec()
	changed.Steps = changed.Steps[:1]
	v2, err := w.NewVersion("v2", &changed, now)
	if err != nil {
		t.Fatal(err)
	}
	if v2.Number != 2 || len(w.Versions[0].Spec.Steps) != 4 {
		t.Fatal("new version must not alter the published one")
	}
	latest, _ := w.LatestPublished()
	if latest.Number != 1 {
		t.Fatal("drafts must not be returned as published")
	}
	if _, err := w.PublishedVersion(2); err == nil {
		t.Fatal("draft version must not be startable")
	}
	if len(w.AddedVersions()) != 2 || len(w.ChangedVersions()) != 0 {
		t.Fatalf("change tracking wrong: added=%d changed=%d", len(w.AddedVersions()), len(w.ChangedVersions()))
	}
}

func TestChangeTrackingOnRestoredAggregate(t *testing.T) {
	now := time.Now()
	v := &Version{ID: "v1", Number: 1, Status: VersionDraft, Spec: orderSpec()}
	w := Restore(Definition{ID: "wf", Name: "orders"}, []*Version{v})
	if _, err := w.Publish(func(string) bool { return true }, now); err != nil {
		t.Fatal(err)
	}
	if len(w.ChangedVersions()) != 1 || len(w.AddedVersions()) != 0 {
		t.Fatal("publish of a stored draft must be tracked as a change")
	}
}

func TestMetadataValidation(t *testing.T) {
	if _, err := New("wf", "v1", "t", "", "", orderSpec(), time.Now()); err == nil {
		t.Fatal("empty name must be rejected")
	}
	if _, err := New("wf", "v1", "t", "ok", strings.Repeat("x", 2001), orderSpec(), time.Now()); err == nil {
		t.Fatal("long description must be rejected")
	}
}
