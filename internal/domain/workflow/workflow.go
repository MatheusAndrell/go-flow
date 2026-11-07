// Package workflow holds the Workflow Definition bounded context: named,
// versioned DAGs of steps. Published versions are immutable so running
// executions are never altered by later edits.
package workflow

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9 _.-]{0,127}$`)

const maxDescriptionLength = 2000

var (
	ErrNoDraft            = errors.New("workflow has no draft version; create a new version first")
	ErrDraftExists        = errors.New("workflow already has a draft version")
	ErrNothingToPublish   = errors.New("workflow has no draft version to publish")
	ErrNoPublishedVersion = errors.New("workflow has no published version")
	ErrVersionNotFound    = errors.New("workflow version not found")
)

type VersionStatus string

const (
	VersionDraft     VersionStatus = "DRAFT"
	VersionPublished VersionStatus = "PUBLISHED"
)

type Definition struct {
	ID          string
	TenantID    string
	Name        string
	Description string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type Version struct {
	ID           string
	DefinitionID string
	TenantID     string
	Number       int
	Status       VersionStatus
	Spec         Spec
	CreatedAt    time.Time
	UpdatedAt    time.Time
	PublishedAt  *time.Time
}

// Workflow is the aggregate root: a definition and all of its versions,
// ordered by ascending version number.
type Workflow struct {
	Definition Definition
	Versions   []*Version

	dirty map[*Version]bool
	added []*Version
}

// New creates a workflow with a first draft version.
func New(id, versionID, tenantID, name, description string, spec Spec, now time.Time) (*Workflow, error) {
	if err := validateMetadata(name, description); err != nil {
		return nil, err
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	w := &Workflow{Definition: Definition{
		ID: id, TenantID: tenantID, Name: strings.TrimSpace(name), Description: description,
		CreatedAt: now, UpdatedAt: now,
	}}
	w.appendVersion(versionID, spec, now)
	return w, nil
}

// Restore rebuilds an aggregate loaded from storage.
func Restore(def Definition, versions []*Version) *Workflow {
	return &Workflow{Definition: def, Versions: versions}
}

func validateMetadata(name, description string) error {
	verr := &ValidationError{}
	if !namePattern.MatchString(strings.TrimSpace(name)) {
		verr.add("name must be 1-128 characters of letters, digits, spaces, '_', '.' or '-'")
	}
	if len(description) > maxDescriptionLength {
		verr.add("description must not exceed %d characters", maxDescriptionLength)
	}
	if len(verr.Problems) > 0 {
		return verr
	}
	return nil
}

func (w *Workflow) Latest() *Version {
	if len(w.Versions) == 0 {
		return nil
	}
	return w.Versions[len(w.Versions)-1]
}

func (w *Workflow) Draft() *Version {
	if v := w.Latest(); v != nil && v.Status == VersionDraft {
		return v
	}
	return nil
}

// LatestPublished returns the newest published version.
func (w *Workflow) LatestPublished() (*Version, error) {
	for i := len(w.Versions) - 1; i >= 0; i-- {
		if w.Versions[i].Status == VersionPublished {
			return w.Versions[i], nil
		}
	}
	return nil, ErrNoPublishedVersion
}

// PublishedVersion returns a specific published version.
func (w *Workflow) PublishedVersion(number int) (*Version, error) {
	for _, v := range w.Versions {
		if v.Number == number {
			if v.Status != VersionPublished {
				return nil, fmt.Errorf("version %d is not published", number)
			}
			return v, nil
		}
	}
	return nil, ErrVersionNotFound
}

// UpdateDescription changes mutable metadata. The name is the workflow's
// identity within a tenant and cannot be renamed.
func (w *Workflow) UpdateDescription(description string, now time.Time) error {
	if err := validateMetadata(w.Definition.Name, description); err != nil {
		return err
	}
	w.Definition.Description = description
	w.Definition.UpdatedAt = now
	return nil
}

// UpdateDraft replaces the spec of the current draft. Published versions cannot change.
func (w *Workflow) UpdateDraft(spec Spec, now time.Time) error {
	draft := w.Draft()
	if draft == nil {
		return ErrNoDraft
	}
	if err := spec.Validate(); err != nil {
		return err
	}
	draft.Spec = spec
	draft.UpdatedAt = now
	w.markDirty(draft)
	w.Definition.UpdatedAt = now
	return nil
}

// NewVersion opens a new draft. When spec is nil the latest spec is copied.
func (w *Workflow) NewVersion(versionID string, spec *Spec, now time.Time) (*Version, error) {
	if w.Draft() != nil {
		return nil, ErrDraftExists
	}
	next := w.Latest().Spec
	if spec != nil {
		if err := spec.Validate(); err != nil {
			return nil, err
		}
		next = *spec
	}
	w.Definition.UpdatedAt = now
	return w.appendVersion(versionID, next, now), nil
}

// Publish freezes the draft. knownHandler reports whether a handler is
// registered by at least one worker; unknown handlers are rejected so that
// executions cannot be started for steps nobody is able to run.
func (w *Workflow) Publish(knownHandler func(string) bool, now time.Time) (*Version, error) {
	draft := w.Draft()
	if draft == nil {
		return nil, ErrNothingToPublish
	}
	if err := draft.Spec.Validate(); err != nil {
		return nil, err
	}
	verr := &ValidationError{}
	for _, h := range draft.Spec.Handlers() {
		if !knownHandler(h) {
			verr.add("handler %q is not registered by any worker", h)
		}
	}
	if len(verr.Problems) > 0 {
		return nil, verr
	}
	draft.Status = VersionPublished
	draft.PublishedAt = &now
	draft.UpdatedAt = now
	w.markDirty(draft)
	w.Definition.UpdatedAt = now
	return draft, nil
}

func (w *Workflow) appendVersion(id string, spec Spec, now time.Time) *Version {
	number := 1
	if latest := w.Latest(); latest != nil {
		number = latest.Number + 1
	}
	v := &Version{
		ID: id, DefinitionID: w.Definition.ID, TenantID: w.Definition.TenantID,
		Number: number, Status: VersionDraft, Spec: spec, CreatedAt: now, UpdatedAt: now,
	}
	w.Versions = append(w.Versions, v)
	w.added = append(w.added, v)
	return v
}

func (w *Workflow) markDirty(v *Version) {
	for _, a := range w.added {
		if a == v {
			return
		}
	}
	if w.dirty == nil {
		w.dirty = map[*Version]bool{}
	}
	w.dirty[v] = true
}

// AddedVersions and ChangedVersions expose pending changes to the repository.
func (w *Workflow) AddedVersions() []*Version { return w.added }

func (w *Workflow) ChangedVersions() []*Version {
	out := make([]*Version, 0, len(w.dirty))
	for _, v := range w.Versions {
		if w.dirty[v] {
			out = append(out, v)
		}
	}
	return out
}
