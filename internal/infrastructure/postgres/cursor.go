package postgres

import (
	"encoding/base64"
	"strconv"
	"strings"
	"time"

	"github.com/MatheusAndrell/go-flow/internal/application"
)

// Keyset pagination cursor over (created_at DESC, id DESC). Unlike OFFSET it
// stays stable and cheap while rows are inserted.
type cursor struct {
	CreatedAt time.Time
	ID        string
}

func (c cursor) encode() string {
	raw := strconv.FormatInt(c.CreatedAt.UnixMicro(), 10) + "|" + c.ID
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeCursor(s string) (*cursor, error) {
	if s == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, application.Invalid("invalid cursor")
	}
	ts, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return nil, application.Invalid("invalid cursor")
	}
	micros, err := strconv.ParseInt(ts, 10, 64)
	if err != nil || len(id) != 36 {
		return nil, application.Invalid("invalid cursor")
	}
	return &cursor{CreatedAt: time.UnixMicro(micros).UTC(), ID: id}, nil
}

// pageArgs returns SQL fragments for keyset pagination. Arguments are
// appended starting at position next.
func pageArgs(c *cursor, next int) (string, []any) {
	if c == nil {
		return "", nil
	}
	return " AND (created_at, id) < ($" + strconv.Itoa(next) + ", $" + strconv.Itoa(next+1) + ")", []any{c.CreatedAt, c.ID}
}
