package gravel

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemTable(t *testing.T) {
	mt := NewMemTable(16)
	var keys, vals [][]byte
	var entries []Entry
	sz := 0
	for i := 0; i < 100; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key-%d", i)))
		sz += len(keys[i])
		vals = append(vals, []byte(fmt.Sprintf("val-%d", i)))
		var v Value
		if i%2 == 0 {
			v.deleted = true
			sz += 1
		} else {
			v.data = vals[i]
			v.expires = 0
			sz += len(vals[i]) + 4 + 1
		}
		entries = append(entries, Entry{key: keys[i], val: v})
	}
	assert.Equal(t, uint64(0), mt.Size())
	assert.Equal(t, uint64(0), mt.Len())

	for _, k := range keys {
		_, err := mt.Get(k, ShardHash(k))
		assert.Equal(t, ErrNotFound, err)
	}

	// now add a few entries
	assert.NoError(t, mt.SetMany(entries, &Stats{}))
	assert.Equal(t, uint64(len(entries)), mt.Len())
	assert.Equal(t, uint64(sz), mt.Size())

	// and verify each made its way
	for i, k := range keys {
		v, err := mt.Get(k, ShardHash(k))
		assert.NoError(t, err)
		assert.Equal(t, entries[i].val, v)
	}
}
