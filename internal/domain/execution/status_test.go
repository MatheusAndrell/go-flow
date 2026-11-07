package execution

import "testing"

func TestExecutionStateMachine(t *testing.T) {
	allowed := map[Status][]Status{
		StatusPending:      {StatusRunning, StatusCancelled},
		StatusRunning:      {StatusWaiting, StatusPaused, StatusCompleted, StatusFailed, StatusCancelled, StatusCompensating},
		StatusWaiting:      {StatusRunning, StatusPaused, StatusFailed, StatusCancelled, StatusCompensating},
		StatusPaused:       {StatusRunning, StatusCancelled, StatusCompensating},
		StatusCompensating: {StatusCompensated, StatusFailed},
	}
	for _, from := range AllStatuses {
		for _, to := range AllStatuses {
			want := false
			for _, a := range allowed[from] {
				if a == to {
					want = true
				}
			}
			if got := from.CanTransitionTo(to); got != want {
				t.Errorf("%s -> %s: got %v want %v", from, to, got, want)
			}
		}
	}
	for _, s := range []Status{StatusCompleted, StatusFailed, StatusCancelled, StatusCompensated} {
		if !s.Terminal() {
			t.Errorf("%s must be terminal", s)
		}
	}
	if Status("BOGUS").Terminal() || Status("BOGUS").Valid() {
		t.Error("unknown status must be neither valid nor terminal")
	}
}

func TestTaskStateMachine(t *testing.T) {
	cases := []struct {
		from, to TaskStatus
		ok       bool
	}{
		{TaskPending, TaskReady, true},
		{TaskPending, TaskRunning, false},
		{TaskReady, TaskRunning, true},
		{TaskRunning, TaskRetryScheduled, true},
		{TaskRetryScheduled, TaskRunning, true},
		{TaskRunning, TaskReady, false},
		{TaskCompleted, TaskCompensated, true},
		{TaskCompleted, TaskFailed, false},
		{TaskFailed, TaskRunning, false},
		{TaskCancelled, TaskReady, false},
	}
	for _, c := range cases {
		if got := c.from.CanTransitionTo(c.to); got != c.ok {
			t.Errorf("%s -> %s: got %v", c.from, c.to, got)
		}
	}
}
