package gravel

import (
	"errors"

	"github.com/zeebo/xxh3"
)

var (
	ErrNotFound = errors.New("key not found")
)

const (
	FileExtension     = ".grvl"
	tempFileExtension = ".grvl.temp"
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

// Hash is the standardized hash function for all keys in each Gravel file
// We retry to do this hash computation once per request and
// pass the hash around instead of recomputing it
func Hash(k []byte) uint64 {
	return xxh3.Hash(k)
}

// Shard returns the shard this hash should go to.
// To calculate the Shard, we don't want to use the hash as it is to avoid the risk
// of unexpected distribution unevenness. Instead, we come up with a related hash
// by xoring the lower 32 bits with higher 32 bits and higher 32 bits with lower 32
// bits. This is significantly faster than taking an independent hash
func Shard(h uint64, numShards uint64) uint64 {
	sh := h ^ (h >> 32) ^ (h << 32)
	return sh & (numShards - 1)
}
