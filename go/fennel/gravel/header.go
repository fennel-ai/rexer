package gravel

import (
	"errors"
	"github.com/cespare/xxhash/v2"
)

var (
	ErrNotFound = errors.New("key not found")
)

const (
	SUFFIX     = ".grvl"
	tempSuffix = ".grvl.temp"
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
	return xxhash.Sum64(k)
}

// ShardHash is for deciding which shard to go, and it uses a different function
// with the one that is used inside the hash-table to avoid the risk of unexpected
// distribution unevenness per hash table file
func ShardHash(k []byte) uint64 {
	// the function needs not very fancy, this is good enough
	arbitraryBigPrime := uint64(21788233)
	ret := uint64(3)
	for _, v := range k {
		ret = ret*arbitraryBigPrime + uint64(v)
	}
	return ret
}

func Shard(k []byte, numShards uint64) uint64 {
	return ShardHash(k) & (numShards - 1)
}
