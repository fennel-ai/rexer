package store

import (
	"context"

	"fennel/lib/value"
)

type AggregateStore interface {
	Get(ctx context.Context, duration uint32, keys []string) ([]value.Value, error)
}
