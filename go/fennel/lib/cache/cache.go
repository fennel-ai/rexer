package cache

import (
	"context"
	"time"
)

type Cache interface {
	Get(ctx context.Context, k string) (interface{}, error)
	Delete(ctx context.Context, k ...string) error
	Set(ctx context.Context, k string, v interface{}, ttl time.Duration) error
	Init() error
	// Nil returns the error that the cache returns when the key isn't found
	Nil() error
}
