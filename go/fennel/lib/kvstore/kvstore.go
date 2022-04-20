package kvstore

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
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

func (v SerializedValue) String() string {
	return fmt.Sprintf("%d:%s", v.Codec, base64.StdEncoding.EncodeToString(v.Raw))
}

type Reader interface {
	// Get returns the value associated with the given key.
	// ErrKeyNotFound is returned if the key is not found in the store.
	Get(ctx context.Context, tablet TabletType, key []byte) (*SerializedValue, error)
	// GetAll returns all the keys and values in the store where the key starts with the given prefix.
	GetAll(ctx context.Context, tablet TabletType, prefix []byte) ([][]byte, []SerializedValue, error)
}

type Writer interface {
	// Set sets the value associated with the given key to the given value.
	Set(ctx context.Context, tablet TabletType, key []byte, value SerializedValue) error
}

type ReaderWriter interface {
	Reader
	Writer
}
