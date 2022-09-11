package gravel

import (
	"errors"

	"github.com/cespare/xxhash/v2"
)

var (
	ErrNotFound = errors.New("key not found")
)

const (
	SUFFIX = ".grvl"
)

type Timestamp uint32

type Value struct {
	data    []byte
	expires Timestamp
	deleted bool
}

type Entry struct {
	key []byte
	val Value
}

// Hash is the standardized hash function for all keys in Gravel
// We retry to do this hash computation once per request and
// pass the hash around instead of recomputing it
func Hash(k []byte) uint64 {
	return xxhash.Sum64(k)
}
