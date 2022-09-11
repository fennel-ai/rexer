package cuckoo

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCuckooMap(t *testing.T) {
	const N = 400
	f := NewFilter[byte](N)
	hashes := make([]uint64, N)
	vals := make([]byte, N)
	for i := range hashes {
		h := rand.Uint64()
		hashes[i] = h
		_, ok := f.Lookup(h)
		assert.False(t, ok)
		vals[i] = uint8(rand.Uint32() & 255)
	}

	// now add all the entries
	for i, h := range hashes {
		assert.True(t, f.Insert(h, vals[i]))
	}

	// now verify that all entries should work
	for i, h := range hashes {
		got, ok := f.Lookup(h)
		assert.True(t, ok)
		assert.Equal(t, vals[i], got)
	}
}
