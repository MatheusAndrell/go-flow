// Package application contains types shared by the use-case packages.
package application

import (
	"errors"
	"fmt"
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")
)

// InvalidInputError carries a client-facing validation message.
type InvalidInputError struct{ Msg string }

func (e *InvalidInputError) Error() string { return e.Msg }

func Invalid(format string, args ...any) error {
	return &InvalidInputError{Msg: fmt.Sprintf(format, args...)}
}

const (
	DefaultPageSize = 20
	MaxPageSize     = 100
)

// Page requests a slice of a keyset-paginated list. Cursor is opaque to callers.
type Page struct {
	Limit  int
	Cursor string
}

func (p Page) Normalize() (Page, error) {
	switch {
	case p.Limit == 0:
		p.Limit = DefaultPageSize
	case p.Limit < 0 || p.Limit > MaxPageSize:
		return p, Invalid("limit must be between 1 and %d", MaxPageSize)
	}
	return p, nil
}
