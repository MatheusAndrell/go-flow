package httpapi

import (
	"net/http"
	"strconv"

	"go.opentelemetry.io/otel/propagation"

	"github.com/MatheusAndrell/go-flow/internal/application"
	"github.com/MatheusAndrell/go-flow/internal/application/definitions"
	"github.com/MatheusAndrell/go-flow/internal/application/executions"
	"github.com/MatheusAndrell/go-flow/internal/domain/execution"
)

func pageFrom(r *http.Request) (application.Page, error) {
	q := r.URL.Query()
	p := application.Page{Cursor: q.Get("cursor")}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return p, application.Invalid("limit must be an integer")
		}
		p.Limit = n
	}
	return p, nil
}

func (s *Server) createWorkflow(w http.ResponseWriter, r *http.Request) {
	var req workflowRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	wf, err := s.Definitions.Create(r.Context(), principalFrom(r.Context()), definitions.CreateInput{
		Name: req.Name, Description: req.Description, Spec: req.Spec,
	})
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	w.Header().Set("Location", "/api/v1/workflows/"+wf.Definition.ID)
	writeJSON(w, http.StatusCreated, toWorkflow(wf))
}

func (s *Server) listWorkflows(w http.ResponseWriter, r *http.Request) {
	page, err := pageFrom(r)
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	defs, next, err := s.Definitions.List(r.Context(), principalFrom(r.Context()), page)
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	items := make([]definitionSummary, 0, len(defs))
	for _, d := range defs {
		items = append(items, definitionSummary{ID: d.ID, Name: d.Name, Description: d.Description, CreatedAt: d.CreatedAt, UpdatedAt: d.UpdatedAt})
	}
	writeJSON(w, http.StatusOK, listResponse[definitionSummary]{Items: items, NextCursor: next})
}

func (s *Server) getWorkflow(w http.ResponseWriter, r *http.Request) {
	wf, err := s.Definitions.Get(r.Context(), principalFrom(r.Context()), r.PathValue("id"))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, http.StatusOK, toWorkflow(wf))
}

func (s *Server) updateWorkflow(w http.ResponseWriter, r *http.Request) {
	var req updateWorkflowRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	wf, err := s.Definitions.Update(r.Context(), principalFrom(r.Context()), r.PathValue("id"), definitions.UpdateInput{
		Description: req.Description, Spec: req.Spec,
	})
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, http.StatusOK, toWorkflow(wf))
}

func (s *Server) createVersion(w http.ResponseWriter, r *http.Request) {
	var req versionRequest
	if r.ContentLength != 0 {
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, r, s.Log, err)
			return
		}
	}
	_, v, err := s.Definitions.CreateVersion(r.Context(), principalFrom(r.Context()), r.PathValue("id"), req.Spec)
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, http.StatusCreated, toVersion(v))
}

func (s *Server) publishWorkflow(w http.ResponseWriter, r *http.Request) {
	_, v, err := s.Definitions.Publish(r.Context(), principalFrom(r.Context()), r.PathValue("id"))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, http.StatusOK, toVersion(v))
}

func (s *Server) startExecution(w http.ResponseWriter, r *http.Request) {
	var req startRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(r.Context(), carrier)
	exec, created, err := s.Executions.Start(r.Context(), principalFrom(r.Context()), executions.StartInput{
		WorkflowID: req.WorkflowID, Version: req.Version, Input: req.Input, ScheduledAt: req.ScheduledAt,
		IdempotencyKey: r.Header.Get(headerIdempotency), TraceParent: carrier.Get("traceparent"),
	})
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	status := http.StatusCreated
	if !created {
		status = http.StatusOK
	}
	w.Header().Set("Location", "/api/v1/executions/"+exec.ID)
	writeJSON(w, status, toExecution(exec))
}

func (s *Server) listExecutions(w http.ResponseWriter, r *http.Request) {
	page, err := pageFrom(r)
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	q := r.URL.Query()
	list, next, err := s.Executions.List(r.Context(), principalFrom(r.Context()), executions.Filter{
		Status: execution.Status(q.Get("status")), DefinitionID: q.Get("workflow_id"),
	}, page)
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, http.StatusOK, listResponse[executionResponse]{Items: mapSlice(list, toExecution), NextCursor: next})
}

func (s *Server) getExecution(w http.ResponseWriter, r *http.Request) {
	exec, err := s.Executions.Get(r.Context(), principalFrom(r.Context()), r.PathValue("id"))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, http.StatusOK, toExecution(exec))
}

func (s *Server) executionHistory(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var after int64
	limit := 0
	var err error
	if v := q.Get("after"); v != "" {
		if after, err = strconv.ParseInt(v, 10, 64); err != nil || after < 0 {
			writeError(w, r, s.Log, application.Invalid("after must be a non-negative event id"))
			return
		}
	}
	if v := q.Get("limit"); v != "" {
		if limit, err = strconv.Atoi(v); err != nil {
			writeError(w, r, s.Log, application.Invalid("limit must be an integer"))
			return
		}
	}
	events, err := s.Executions.History(r.Context(), principalFrom(r.Context()), r.PathValue("id"), after, limit)
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	resp := listResponse[eventResponse]{Items: mapSlice(events, toEvent)}
	if limit == 0 {
		limit = 100
	}
	if len(events) == limit {
		resp.NextCursor = strconv.FormatInt(events[len(events)-1].ID, 10)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) executionTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := s.Executions.Tasks(r.Context(), principalFrom(r.Context()), r.PathValue("id"))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, http.StatusOK, listResponse[taskResponse]{Items: mapSlice(tasks, toTask)})
}

func (s *Server) cancelExecution(w http.ResponseWriter, r *http.Request) {
	var req cancelRequest
	if r.ContentLength != 0 {
		if err := decodeJSON(r, &req); err != nil {
			writeError(w, r, s.Log, err)
			return
		}
	}
	exec, err := s.Executions.Cancel(r.Context(), principalFrom(r.Context()), r.PathValue("id"), req.Reason, req.Compensate)
	s.respondExecution(w, r, exec, err)
}

func (s *Server) pauseExecution(w http.ResponseWriter, r *http.Request) {
	exec, err := s.Executions.Pause(r.Context(), principalFrom(r.Context()), r.PathValue("id"))
	s.respondExecution(w, r, exec, err)
}

func (s *Server) resumeExecution(w http.ResponseWriter, r *http.Request) {
	exec, err := s.Executions.Resume(r.Context(), principalFrom(r.Context()), r.PathValue("id"))
	s.respondExecution(w, r, exec, err)
}

func (s *Server) respondExecution(w http.ResponseWriter, r *http.Request, exec *execution.Execution, err error) {
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, http.StatusOK, toExecution(exec))
}

func (s *Server) getTask(w http.ResponseWriter, r *http.Request) {
	t, err := s.Executions.Task(r.Context(), principalFrom(r.Context()), r.PathValue("id"))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, http.StatusOK, toTask(t))
}

func (s *Server) taskAttempts(w http.ResponseWriter, r *http.Request) {
	attempts, err := s.Executions.Attempts(r.Context(), principalFrom(r.Context()), r.PathValue("id"))
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, http.StatusOK, listResponse[attemptResponse]{Items: mapSlice(attempts, toAttempt)})
}

func (s *Server) listDeadLetters(w http.ResponseWriter, r *http.Request) {
	page, err := pageFrom(r)
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	list, next, err := s.Executions.DeadLetters(r.Context(), principalFrom(r.Context()), page)
	if err != nil {
		writeError(w, r, s.Log, err)
		return
	}
	writeJSON(w, http.StatusOK, listResponse[deadLetterResponse]{Items: mapSlice(list, toDeadLetter), NextCursor: next})
}
