package kvstore

import (
	"context"
	"errors"
)

var (
	ErrKeyNotFound = errors.New("Key not found")
	ErrEmptyKey    = errors.New("Key cannot be empty")
)

// Value stored in the KV store, alongwith metadata about the codec version used
// to encode the value.
type SerializedValue struct {
	Codec uint8
	Raw   []byte
}

type Reader interface {
	// Get returns the value associated with the given key.
	// ErrKeyNotFound is returned if the key is not found in the store.
	Get(ctx context.Context, key []byte) (*SerializedValue, error)
}

type Writer interface {
	// Set sets the value associated with the given key to the given value.
	Set(ctx context.Context, key []byte, value SerializedValue) error
}

type ReaderWriter interface {
	Reader
	Writer
}
