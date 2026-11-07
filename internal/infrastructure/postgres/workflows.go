package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/MatheusAndrell/go-flow/internal/application"
	"github.com/MatheusAndrell/go-flow/internal/domain/workflow"
)

func (s *Store) CreateWorkflow(ctx context.Context, w *workflow.Workflow) error {
	return mapErr(s.inTx(ctx, func(tx pgx.Tx) error {
		d := w.Definition
		if _, err := tx.Exec(ctx, `INSERT INTO workflow_definitions (id, tenant_id, name, description, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6)`, d.ID, d.TenantID, d.Name, d.Description, d.CreatedAt, d.UpdatedAt); err != nil {
			return err
		}
		return saveVersions(ctx, tx, w)
	}))
}

func (s *Store) UpdateWorkflow(ctx context.Context, tenantID, id string, fn func(*workflow.Workflow) error) (*workflow.Workflow, error) {
	var w *workflow.Workflow
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		w, err = loadWorkflow(ctx, tx, tenantID, id, true)
		if err != nil {
			return err
		}
		if err := fn(w); err != nil {
			return err
		}
		d := w.Definition
		if _, err := tx.Exec(ctx, `UPDATE workflow_definitions SET description = $2, updated_at = $3 WHERE id = $1`,
			d.ID, d.Description, d.UpdatedAt); err != nil {
			return err
		}
		return saveVersions(ctx, tx, w)
	})
	return w, mapErr(err)
}

func (s *Store) GetWorkflow(ctx context.Context, tenantID, id string) (*workflow.Workflow, error) {
	var w *workflow.Workflow
	err := pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		var err error
		w, err = loadWorkflow(ctx, tx, tenantID, id, false)
		return err
	})
	return w, mapErr(err)
}

func (s *Store) ListDefinitions(ctx context.Context, tenantID string, page application.Page) ([]workflow.Definition, string, error) {
	c, err := decodeCursor(page.Cursor)
	if err != nil {
		return nil, "", err
	}
	cond, args := pageArgs(c, 3)
	rows, err := s.pool.Query(ctx, `SELECT id, tenant_id, name, description, created_at, updated_at
		FROM workflow_definitions WHERE tenant_id = $1`+cond+` ORDER BY created_at DESC, id DESC LIMIT $2`,
		append([]any{tenantID, page.Limit + 1}, args...)...)
	if err != nil {
		return nil, "", mapErr(err)
	}
	defs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (workflow.Definition, error) {
		var d workflow.Definition
		err := r.Scan(&d.ID, &d.TenantID, &d.Name, &d.Description, &d.CreatedAt, &d.UpdatedAt)
		return d, err
	})
	if err != nil {
		return nil, "", mapErr(err)
	}
	next := ""
	if len(defs) > page.Limit {
		defs = defs[:page.Limit]
		last := defs[len(defs)-1]
		next = cursor{CreatedAt: last.CreatedAt, ID: last.ID}.encode()
	}
	return defs, next, nil
}

func loadWorkflow(ctx context.Context, tx pgx.Tx, tenantID, id string, lock bool) (*workflow.Workflow, error) {
	q := `SELECT id, tenant_id, name, description, created_at, updated_at FROM workflow_definitions WHERE id = $1 AND tenant_id = $2`
	if lock {
		q += ` FOR UPDATE`
	}
	var d workflow.Definition
	if err := tx.QueryRow(ctx, q, id, tenantID).Scan(&d.ID, &d.TenantID, &d.Name, &d.Description, &d.CreatedAt, &d.UpdatedAt); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT id, definition_id, tenant_id, number, status, spec, created_at, updated_at, published_at
		FROM workflow_versions WHERE definition_id = $1 ORDER BY number`, d.ID)
	if err != nil {
		return nil, err
	}
	versions, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (*workflow.Version, error) {
		var (
			v    workflow.Version
			spec []byte
		)
		if err := r.Scan(&v.ID, &v.DefinitionID, &v.TenantID, &v.Number, &v.Status, &spec, &v.CreatedAt, &v.UpdatedAt, &v.PublishedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(spec, &v.Spec); err != nil {
			return nil, fmt.Errorf("decode spec of version %s: %w", v.ID, err)
		}
		return &v, nil
	})
	if err != nil {
		return nil, err
	}
	return workflow.Restore(d, versions), nil
}

func saveVersions(ctx context.Context, tx pgx.Tx, w *workflow.Workflow) error {
	for _, v := range w.AddedVersions() {
		spec, err := json.Marshal(v.Spec)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO workflow_versions (id, definition_id, tenant_id, number, status, spec, created_at, updated_at, published_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			v.ID, v.DefinitionID, v.TenantID, v.Number, v.Status, spec, v.CreatedAt, v.UpdatedAt, v.PublishedAt); err != nil {
			return err
		}
	}
	for _, v := range w.ChangedVersions() {
		spec, err := json.Marshal(v.Spec)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE workflow_versions SET status = $2, spec = $3, updated_at = $4, published_at = $5 WHERE id = $1`,
			v.ID, v.Status, spec, v.UpdatedAt, v.PublishedAt); err != nil {
			return err
		}
	}
	return nil
}

// KnownHandlers returns handlers advertised by any worker that has ever registered.
func (s *Store) KnownHandlers(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT unnest(handlers) FROM workers`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}
