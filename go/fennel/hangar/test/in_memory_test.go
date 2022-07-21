package test

import (
	"math/rand"
	"testing"
	"time"

	"fennel/hangar"
	"fennel/lib/ftypes"

	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
)

func TestBasic(t *testing.T) {
	planeId := ftypes.RealmID(1)
	h := NewInMemoryHangar(planeId)

	// Get before set.
	vgs, err := h.GetMany([]hangar.KeyGroup{
		{
			Prefix: hangar.Key{
				Data: []byte("foo"),
			},
			Fields: mo.None[hangar.Fields](),
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(vgs), "%+v", vgs)
	assert.Equal(t, 0, len(vgs[0].Fields), "%+v", vgs[0])
	assert.Equal(t, 0, len(vgs[0].Values), "%+v", vgs[0])

	// Now set some fields.
	fields := [][]byte{[]byte("a"), []byte("b")}
	values := [][]byte{[]byte("x"), []byte("y")}
	err = h.SetMany([]hangar.Key{
		{
			Data: []byte("foo"),
		},
	}, []hangar.ValGroup{
		{
			Fields: fields,
			Values: values,
		},
	})
	assert.NoError(t, err)

	// Get all values for "foo" prefix.
	vgs, err = h.GetMany([]hangar.KeyGroup{
		{
			Prefix: hangar.Key{
				Data: []byte("foo"),
			},
			Fields: mo.None[hangar.Fields](),
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(vgs), "%+v", vgs)
	assert.Equal(t, 2, len(vgs[0].Fields), "%+v", vgs[0])
	assert.Equal(t, 2, len(vgs[0].Values), "%+v", vgs[0])
	assert.ElementsMatch(t, fields, vgs[0].Fields)
	assert.ElementsMatch(t, values, vgs[0].Values)

	// Get a specific field for "foo" prefix.
	vgs, err = h.GetMany([]hangar.KeyGroup{
		{
			Prefix: hangar.Key{
				Data: []byte("foo"),
			},
			Fields: mo.Some[hangar.Fields](fields[:1]),
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(vgs), "%+v", vgs)
	assert.Equal(t, 1, len(vgs[0].Fields), "%+v", vgs[0])
	assert.Equal(t, 1, len(vgs[0].Values), "%+v", vgs[0])
	assert.ElementsMatch(t, fields[:1], vgs[0].Fields)
	assert.ElementsMatch(t, values[:1], vgs[0].Values)

	// Delete a field for "foo" prefix.
	err = h.DelMany([]hangar.KeyGroup{
		{
			Prefix: hangar.Key{
				Data: []byte("foo"),
			},
			Fields: mo.Some[hangar.Fields](fields[:1]),
		},
	})
	assert.NoError(t, err)

	// Now get all fields for "foo".
	vgs, err = h.GetMany([]hangar.KeyGroup{
		{
			Prefix: hangar.Key{
				Data: []byte("foo"),
			},
			Fields: mo.None[hangar.Fields](),
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(vgs), "%+v", vgs)
	assert.Equal(t, 1, len(vgs[0].Fields), "%+v", vgs[0])
	assert.Equal(t, 1, len(vgs[0].Values), "%+v", vgs[0])
	assert.ElementsMatch(t, fields[1:], vgs[0].Fields)
	assert.ElementsMatch(t, values[1:], vgs[0].Values)
}

func TestInMemoryFull(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	maker := func(_ *testing.T) hangar.Hangar {
		planeID := ftypes.RealmID(rand.Uint32())
		db := NewInMemoryHangar(planeID)
		return db
	}
	skipped := []string{"test_set_ttl"}
	hangar.TestStore(t, maker, skipped...)
}
