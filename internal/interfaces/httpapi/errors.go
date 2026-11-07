package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/MatheusAndrell/go-flow/internal/application"
	"github.com/MatheusAndrell/go-flow/internal/domain/execution"
	"github.com/MatheusAndrell/go-flow/internal/domain/identity"
	"github.com/MatheusAndrell/go-flow/internal/domain/workflow"
	"github.com/MatheusAndrell/go-flow/internal/platform/telemetry"
)

// Problem is an RFC 9457 problem details document.
type Problem struct {
	Type          string   `json:"type"`
	Title         string   `json:"title"`
	Status        int      `json:"status"`
	Detail        string   `json:"detail,omitempty"`
	Errors        []string `json:"errors,omitempty"`
	CorrelationID string   `json:"correlation_id,omitempty"`
}

func problemFor(err error) Problem {
	var (
		verr    *workflow.ValidationError
		invalid *application.InvalidInputError
		maxErr  *http.MaxBytesError
	)
	switch {
	case errors.Is(err, identity.ErrUnauthenticated):
		return Problem{Type: "unauthenticated", Title: "Authentication required", Status: http.StatusUnauthorized}
	case errors.Is(err, identity.ErrForbidden):
		return Problem{Type: "forbidden", Title: "Permission denied", Status: http.StatusForbidden, Detail: err.Error()}
	case errors.Is(err, application.ErrNotFound):
		return Problem{Type: "not-found", Title: "Resource not found", Status: http.StatusNotFound}
	case errors.As(err, &verr):
		return Problem{Type: "validation-error", Title: "Workflow definition is invalid", Status: http.StatusUnprocessableEntity, Errors: verr.Problems}
	case errors.As(err, &invalid):
		return Problem{Type: "invalid-request", Title: "Invalid request", Status: http.StatusBadRequest, Detail: invalid.Msg}
	case errors.As(err, &maxErr):
		return Problem{Type: "payload-too-large", Title: "Request body too large", Status: http.StatusRequestEntityTooLarge}
	case errors.Is(err, execution.ErrInvalidTransition):
		return Problem{Type: "invalid-state-transition", Title: "Operation not allowed in the current state", Status: http.StatusConflict, Detail: err.Error()}
	case errors.Is(err, application.ErrConflict),
		errors.Is(err, workflow.ErrDraftExists), errors.Is(err, workflow.ErrNoDraft), errors.Is(err, workflow.ErrNothingToPublish):
		return Problem{Type: "conflict", Title: "Conflict with the current state", Status: http.StatusConflict, Detail: conflictDetail(err)}
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return Problem{Type: "unavailable", Title: "Request cancelled or timed out", Status: http.StatusServiceUnavailable}
	}
	return Problem{Type: "internal", Title: "Internal server error", Status: http.StatusInternalServerError}
}

func conflictDetail(err error) string {
	if errors.Is(err, application.ErrConflict) {
		return "a resource with the same unique attributes already exists"
	}
	return err.Error()
}

func writeError(w http.ResponseWriter, r *http.Request, log *slog.Logger, err error) {
	p := problemFor(err)
	p.CorrelationID = telemetry.CorrelationID(r.Context())
	if p.Status >= 500 {
		log.ErrorContext(r.Context(), "request failed", "error", err, "path", r.URL.Path)
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// decodeJSON decodes exactly one JSON object, rejecting unknown fields.
func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return err
		}
		return application.Invalid("malformed JSON body: %s", err.Error())
	}
	if dec.More() {
		return application.Invalid("body must contain a single JSON object")
	}
	return nil
}
