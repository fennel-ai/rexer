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

	// RunAsTxn executes the given func `l` in a transaction
	//
	// the transaction will retry atmost `r` times if the value of any of the `keys` was
	// modified (written, updated, evicted) during its execution
	//
	// NOTE: the logic in func `l` must use the `txn` instance to modify the
	// state of the cache. In the failure mode (txn could not be committed after retries),
	// all the cache entries corresponding to `keys` are invalidated to NOT leave the
	// cache in a potentially inconsistent state
	RunAsTxn(ctx context.Context, l func(txn Txn, keys []string) error, keys []string, r int) error

	// Nil returns the error that the cache returns when the key isn't found
	Nil() error
}

// Txn is the interface which is used to modify the cache state atomically
//
// NOTE: An instance of this instance should not be created. This is usually provided
// as an argument to the function which is executed as part of `Cache.RunAsTxn`
type Txn interface {
	Get(ctx context.Context, k string) (interface{}, error)
	MGet(ctx context.Context, k ...string) ([]interface{}, error)

	Set(ctx context.Context, k string, v interface{}, ttl time.Duration) error
	MSet(ctx context.Context, ks []string, vs []interface{}, ttls []time.Duration) error
}
