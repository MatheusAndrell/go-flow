// Package definitions implements the Workflow Definition use cases.
package definitions

import (
	"context"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/application"
	"github.com/MatheusAndrell/go-flow/internal/domain/identity"
	"github.com/MatheusAndrell/go-flow/internal/domain/workflow"
)

type Store interface {
	CreateWorkflow(ctx context.Context, w *workflow.Workflow) error
	// UpdateWorkflow loads the aggregate with a row lock, applies fn and persists its changes atomically.
	UpdateWorkflow(ctx context.Context, tenantID, id string, fn func(*workflow.Workflow) error) (*workflow.Workflow, error)
	GetWorkflow(ctx context.Context, tenantID, id string) (*workflow.Workflow, error)
	ListDefinitions(ctx context.Context, tenantID string, page application.Page) ([]workflow.Definition, string, error)
}

// HandlerCatalog lists handler names registered by workers.
type HandlerCatalog interface {
	KnownHandlers(ctx context.Context) ([]string, error)
}

type Service struct {
	store   Store
	catalog HandlerCatalog
	newID   func() string
	now     func() time.Time
}

func NewService(store Store, catalog HandlerCatalog, newID func() string, now func() time.Time) *Service {
	return &Service{store: store, catalog: catalog, newID: newID, now: now}
}

type CreateInput struct {
	Name        string
	Description string
	Spec        workflow.Spec
}

func (s *Service) Create(ctx context.Context, p identity.Principal, in CreateInput) (*workflow.Workflow, error) {
	if err := p.Require(identity.PermWorkflowsWrite); err != nil {
		return nil, err
	}
	w, err := workflow.New(s.newID(), s.newID(), p.TenantID, in.Name, in.Description, in.Spec, s.now())
	if err != nil {
		return nil, err
	}
	if err := s.store.CreateWorkflow(ctx, w); err != nil {
		return nil, err
	}
	return w, nil
}

func (s *Service) Get(ctx context.Context, p identity.Principal, id string) (*workflow.Workflow, error) {
	if err := p.Require(identity.PermWorkflowsRead); err != nil {
		return nil, err
	}
	return s.store.GetWorkflow(ctx, p.TenantID, id)
}

func (s *Service) List(ctx context.Context, p identity.Principal, page application.Page) ([]workflow.Definition, string, error) {
	if err := p.Require(identity.PermWorkflowsRead); err != nil {
		return nil, "", err
	}
	page, err := page.Normalize()
	if err != nil {
		return nil, "", err
	}
	return s.store.ListDefinitions(ctx, p.TenantID, page)
}

// UpdateInput changes the description and/or the spec of the current draft.
type UpdateInput struct {
	Description *string
	Spec        *workflow.Spec
}

func (s *Service) Update(ctx context.Context, p identity.Principal, id string, in UpdateInput) (*workflow.Workflow, error) {
	if err := p.Require(identity.PermWorkflowsWrite); err != nil {
		return nil, err
	}
	if in.Description == nil && in.Spec == nil {
		return nil, application.Invalid("nothing to update: provide description and/or spec")
	}
	now := s.now()
	return s.store.UpdateWorkflow(ctx, p.TenantID, id, func(w *workflow.Workflow) error {
		if in.Description != nil {
			if err := w.UpdateDescription(*in.Description, now); err != nil {
				return err
			}
		}
		if in.Spec != nil {
			return w.UpdateDraft(*in.Spec, now)
		}
		return nil
	})
}

// CreateVersion opens a new draft, copying the latest spec when spec is nil.
func (s *Service) CreateVersion(ctx context.Context, p identity.Principal, id string, spec *workflow.Spec) (*workflow.Workflow, *workflow.Version, error) {
	if err := p.Require(identity.PermWorkflowsWrite); err != nil {
		return nil, nil, err
	}
	var created *workflow.Version
	w, err := s.store.UpdateWorkflow(ctx, p.TenantID, id, func(w *workflow.Workflow) error {
		v, err := w.NewVersion(s.newID(), spec, s.now())
		created = v
		return err
	})
	return w, created, err
}

// Publish freezes the draft after checking that every referenced handler is
// registered by some worker.
func (s *Service) Publish(ctx context.Context, p identity.Principal, id string) (*workflow.Workflow, *workflow.Version, error) {
	if err := p.Require(identity.PermWorkflowsWrite); err != nil {
		return nil, nil, err
	}
	names, err := s.catalog.KnownHandlers(ctx)
	if err != nil {
		return nil, nil, err
	}
	known := make(map[string]bool, len(names))
	for _, n := range names {
		known[n] = true
	}
	var published *workflow.Version
	w, err := s.store.UpdateWorkflow(ctx, p.TenantID, id, func(w *workflow.Workflow) error {
		v, err := w.Publish(func(h string) bool { return known[h] }, s.now())
		published = v
		return err
	})
	return w, published, err
}
