package cache

import (
	"context"
	"time"
)

type Cache interface {
	Init() error

	Get(ctx context.Context, k string) (interface{}, error)
	MGet(ctx context.Context, k ...string) ([]interface{}, error)

	Set(ctx context.Context, k string, v interface{}, ttl time.Duration) error
	MSet(ctx context.Context, ks []string, vs []interface{}, ttls []time.Duration) error

	Delete(ctx context.Context, k ...string) error
	// Nil returns the error that the cache returns when the key isn't found
	Nil() error
}
