package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/domain/retry"
)

const (
	MaxSteps       = 100
	MaxStepTimeout = 24 * time.Hour
	DefaultTimeout = 5 * time.Minute
	maxParamsBytes = 64 << 10
)

var (
	stepKeyPattern     = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
	handlerNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)
)

// Spec is the executable part of a workflow version: a DAG of steps.
type Spec struct {
	Steps    []Step   `json:"steps"`
	Settings Settings `json:"settings"`
}

type Settings struct {
	// MaxParallelism limits how many steps of one execution may be in flight at once. Zero means unlimited.
	MaxParallelism int `json:"max_parallelism,omitempty"`
	// DefaultTimeout applies to steps without their own timeout.
	DefaultTimeout Duration `json:"default_timeout,omitempty"`
	// DefaultRetry applies to steps without their own retry policy.
	DefaultRetry *retry.Policy `json:"default_retry,omitempty"`
}

type Step struct {
	Key          string          `json:"key"`
	Name         string          `json:"name,omitempty"`
	Handler      string          `json:"handler"`
	DependsOn    []string        `json:"depends_on,omitempty"`
	Params       json.RawMessage `json:"params,omitempty"`
	Timeout      Duration        `json:"timeout,omitempty"`
	Retry        *retry.Policy   `json:"retry,omitempty"`
	Compensation *Compensation   `json:"compensation,omitempty"`
}

// Compensation is the business operation that semantically undoes a completed step.
type Compensation struct {
	Handler string        `json:"handler"`
	Timeout Duration      `json:"timeout,omitempty"`
	Retry   *retry.Policy `json:"retry,omitempty"`
}

// Duration marshals as a Go duration string such as "30s".
type Duration time.Duration

func (d Duration) Std() time.Duration { return time.Duration(d) }

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("duration must be a string such as \"30s\": %w", err)
	}
	if s == "" {
		*d = 0
		return nil
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// ValidationError lists every problem found in a spec so clients can fix them at once.
type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return "invalid workflow spec: " + strings.Join(e.Problems, "; ")
}

func (e *ValidationError) add(format string, args ...any) {
	e.Problems = append(e.Problems, fmt.Sprintf(format, args...))
}

// Validate checks structure, references and acyclicity of the step graph.
func (s Spec) Validate() error {
	verr := &ValidationError{}
	if len(s.Steps) == 0 {
		verr.add("at least one step is required")
	}
	if len(s.Steps) > MaxSteps {
		verr.add("a workflow may declare at most %d steps", MaxSteps)
	}
	if s.Settings.MaxParallelism < 0 {
		verr.add("settings.max_parallelism must not be negative")
	}
	validateTimeout(verr, "settings.default_timeout", s.Settings.DefaultTimeout)
	validatePolicy(verr, "settings.default_retry", s.Settings.DefaultRetry)

	keys := make(map[string]bool, len(s.Steps))
	for i, step := range s.Steps {
		where := fmt.Sprintf("steps[%d]", i)
		if !stepKeyPattern.MatchString(step.Key) {
			verr.add("%s.key %q must match %s", where, step.Key, stepKeyPattern)
		} else if keys[step.Key] {
			verr.add("%s.key %q is duplicated", where, step.Key)
		}
		keys[step.Key] = true
		if !handlerNamePattern.MatchString(step.Handler) {
			verr.add("%s.handler %q must match %s", where, step.Handler, handlerNamePattern)
		}
		if len(step.Params) > maxParamsBytes {
			verr.add("%s.params exceeds %d bytes", where, maxParamsBytes)
		}
		if len(step.Params) > 0 && !json.Valid(step.Params) {
			verr.add("%s.params is not valid JSON", where)
		}
		validateTimeout(verr, where+".timeout", step.Timeout)
		validatePolicy(verr, where+".retry", step.Retry)
		if c := step.Compensation; c != nil {
			if !handlerNamePattern.MatchString(c.Handler) {
				verr.add("%s.compensation.handler %q must match %s", where, c.Handler, handlerNamePattern)
			}
			validateTimeout(verr, where+".compensation.timeout", c.Timeout)
			validatePolicy(verr, where+".compensation.retry", c.Retry)
		}
	}
	for i, step := range s.Steps {
		seen := make(map[string]bool, len(step.DependsOn))
		for _, dep := range step.DependsOn {
			switch {
			case dep == step.Key:
				verr.add("steps[%d] %q depends on itself", i, step.Key)
			case !keys[dep]:
				verr.add("steps[%d] %q depends on unknown step %q", i, step.Key, dep)
			case seen[dep]:
				verr.add("steps[%d] %q declares dependency %q twice", i, step.Key, dep)
			}
			seen[dep] = true
		}
	}
	if len(verr.Problems) > 0 {
		return verr
	}
	if _, err := s.TopologicalOrder(); err != nil {
		verr.add("%s", err.Error())
		return verr
	}
	return nil
}

func validateTimeout(verr *ValidationError, field string, d Duration) {
	if d < 0 || d.Std() > MaxStepTimeout {
		verr.add("%s must be between 0 and %s", field, MaxStepTimeout)
	}
}

func validatePolicy(verr *ValidationError, field string, p *retry.Policy) {
	if p == nil {
		return
	}
	if err := p.Validate(); err != nil {
		verr.add("%s: %s", field, strings.ReplaceAll(err.Error(), "\n", ", "))
	}
}

// ErrCycle is returned when the dependency graph is not a DAG.
var ErrCycle = errors.New("dependency graph contains a cycle")

// TopologicalOrder returns step keys ordered so that every step appears after
// its dependencies (Kahn's algorithm, ties broken by declaration order).
func (s Spec) TopologicalOrder() ([]string, error) {
	indegree := make(map[string]int, len(s.Steps))
	dependents := make(map[string][]string, len(s.Steps))
	for _, step := range s.Steps {
		indegree[step.Key] += 0
		for _, dep := range step.DependsOn {
			indegree[step.Key]++
			dependents[dep] = append(dependents[dep], step.Key)
		}
	}
	var queue []string
	for _, step := range s.Steps {
		if indegree[step.Key] == 0 {
			queue = append(queue, step.Key)
		}
	}
	order := make([]string, 0, len(s.Steps))
	for len(queue) > 0 {
		key := queue[0]
		queue = queue[1:]
		order = append(order, key)
		for _, next := range dependents[key] {
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	if len(order) != len(s.Steps) {
		var stuck []string
		for _, step := range s.Steps {
			if !slices.Contains(order, step.Key) {
				stuck = append(stuck, step.Key)
			}
		}
		return nil, fmt.Errorf("%w involving steps %s", ErrCycle, strings.Join(stuck, ", "))
	}
	return order, nil
}

// Handlers returns every handler name referenced by the spec, including compensations.
func (s Spec) Handlers() []string {
	var names []string
	for _, step := range s.Steps {
		names = append(names, step.Handler)
		if step.Compensation != nil {
			names = append(names, step.Compensation.Handler)
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// TimeoutFor resolves the effective timeout of a step.
func (s Spec) TimeoutFor(step Step) time.Duration {
	switch {
	case step.Timeout > 0:
		return step.Timeout.Std()
	case s.Settings.DefaultTimeout > 0:
		return s.Settings.DefaultTimeout.Std()
	default:
		return DefaultTimeout
	}
}

// RetryFor resolves the effective retry policy of a step.
func (s Spec) RetryFor(step Step) retry.Policy {
	switch {
	case step.Retry != nil:
		return *step.Retry
	case s.Settings.DefaultRetry != nil:
		return *s.Settings.DefaultRetry
	default:
		return retry.Default()
	}
}
