package gravel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloom(t *testing.T) {
	b := NewBloomFilter(1000, 0.001)
	keys := [][]byte{[]byte("hi"), []byte("bye"), []byte("okay")}
	for _, k := range keys {
		assert.False(t, b.Has(k))
		b.Add(k)
		assert.True(t, b.Has(k))
	}
	data := b.Dump()
	b2 := LoadBloom(data)
	for _, k := range keys {
		assert.True(t, b2.Has(k))
	}
}
