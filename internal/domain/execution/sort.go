package execution

import (
	"cmp"
	"slices"
)

func sortByPosition(tasks []*Task) {
	slices.SortStableFunc(tasks, func(a, b *Task) int { return cmp.Compare(a.Position, b.Position) })
}

// sortByCompletionDesc orders tasks from most to least recently completed.
// Ties (same timestamp) fall back to reverse topological position, which is
// still a valid reverse order for the DAG.
func sortByCompletionDesc(tasks []*Task) {
	slices.SortStableFunc(tasks, func(a, b *Task) int {
		if a.CompletedAt != nil && b.CompletedAt != nil {
			if c := b.CompletedAt.Compare(*a.CompletedAt); c != 0 {
				return c
			}
		}
		return cmp.Compare(b.Position, a.Position)
	})
}
