package hangar

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestValGroup_Valid(t *testing.T) {
	t.Parallel()
	scenarios := []struct {
		vg    ValGroup
		valid bool
	}{
		{ValGroup{
			Expiry: 0,
			Fields: nil,
			Values: nil,
		}, true},
		{ValGroup{
			Expiry: 0,
			Fields: Fields([][]byte{[]byte("foo")}),
			Values: Values([][]byte{[]byte("bar")}),
		}, true},
		{ValGroup{
			Expiry: -1,
			Fields: Fields([][]byte{[]byte("foo")}),
			Values: Values([][]byte{[]byte("bar")}),
		}, true},
		{ValGroup{
			Expiry: int64(time.Now().Add(time.Minute).Second()),
			Fields: Fields([][]byte{[]byte("foo"), []byte("bar")}),
			Values: Values([][]byte{[]byte("bar"), []byte("baz")}),
		}, true},
		{ValGroup{
			Expiry: 0,
			Fields: Fields([][]byte{}),
			Values: Values([][]byte{[]byte("bar")}),
		}, false},
		{ValGroup{
			Expiry: -1,
			Fields: Fields([][]byte{}),
			Values: Values([][]byte{[]byte("bar")}),
		}, false},
	}
	for _, scene := range scenarios {
		assert.Equal(t, scene.valid, scene.vg.Valid())
	}
}

func TestValGroup_Update(t *testing.T) {
	scenarios := []struct {
		old  ValGroup
		new  ValGroup
		want ValGroup
		err  bool
	}{
		{
			ValGroup{Expiry: 123, Fields: Fields([][]byte{[]byte("foo")}), Values: Values([][]byte{[]byte("bar")})},
			ValGroup{Expiry: 41, Fields: Fields([][]byte{[]byte("foo")}), Values: Values([][]byte{[]byte("baz")})},
			ValGroup{Expiry: 41, Fields: Fields([][]byte{[]byte("foo")}), Values: Values([][]byte{[]byte("baz")})},
			false,
		},
		{
			ValGroup{},
			ValGroup{Expiry: 41, Fields: Fields([][]byte{[]byte("foo")}), Values: Values([][]byte{[]byte("baz")})},
			ValGroup{Expiry: 41, Fields: Fields([][]byte{[]byte("foo")}), Values: Values([][]byte{[]byte("baz")})},
			false,
		},
		{
			ValGroup{Expiry: 41, Fields: Fields([][]byte{[]byte("foo"), []byte("hi")}), Values: Values([][]byte{[]byte("bar"), []byte("bye")})},
			ValGroup{Expiry: 41, Fields: Fields([][]byte{[]byte("foo"), []byte("one")}), Values: Values([][]byte{[]byte("baz"), []byte("two")})},
			ValGroup{Expiry: 41, Fields: Fields([][]byte{[]byte("foo"), []byte("hi"), []byte("one")}), Values: Values([][]byte{[]byte("baz"), []byte("bye"), []byte("two")})},
			false,
		},
		{
			ValGroup{Expiry: 123, Fields: Fields([][]byte{[]byte("foo"), []byte("hi")}), Values: Values([][]byte{[]byte("bar")})},
			ValGroup{Expiry: -1, Fields: Fields([][]byte{[]byte("foo")}), Values: Values([][]byte{[]byte("baz")})},
			ValGroup{Expiry: 41, Fields: Fields([][]byte{[]byte("foo")}), Values: Values([][]byte{[]byte("baz")})},
			true,
		},
		{
			ValGroup{Expiry: -1, Fields: Fields([][]byte{[]byte("foo")}), Values: Values([][]byte{[]byte("baz")})},
			ValGroup{Expiry: 123, Fields: Fields([][]byte{[]byte("foo"), []byte("hi")}), Values: Values([][]byte{[]byte("bar")})},
			ValGroup{Expiry: 41, Fields: Fields([][]byte{[]byte("foo")}), Values: Values([][]byte{[]byte("baz")})},
			true,
		},
	}
	for _, scene := range scenarios {
		err := scene.old.Update(scene.new)
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, scene.want, scene.old)
		}
	}
}

func TestValGroup_Select(t *testing.T) {
	scenarios := []struct {
		base   ValGroup
		fields Fields
		exp    ValGroup
	}{
		{
			ValGroup{Expiry: 0,
				Fields: Fields([][]byte{[]byte("foo"), []byte("hi")}),
				Values: Values([][]byte{[]byte("bar"), []byte("bye")}),
			},
			Fields([][]byte{[]byte("foo"), []byte("extra")}),
			ValGroup{Expiry: 0,
				Fields: Fields([][]byte{[]byte("foo")}),
				Values: Values([][]byte{[]byte("bar")}),
			},
		},
		{
			ValGroup{Expiry: 0,
				Fields: Fields([][]byte{[]byte("foo"), []byte("hi")}),
				Values: Values([][]byte{[]byte("bar"), []byte("bye")}),
			},
			Fields([][]byte{}),
			ValGroup{Expiry: 0,
				Fields: Fields([][]byte{}),
				Values: Values([][]byte{}),
			},
		},
		{
			ValGroup{Expiry: 0, Fields: nil, Values: nil},
			Fields([][]byte{[]byte("foo"), []byte("extra")}),
			ValGroup{Expiry: 0, Fields: nil, Values: nil},
		},
	}
	for _, scene := range scenarios {
		scene.base.Select(scene.fields)
		assert.Equal(t, scene.exp, scene.base)
	}
}

func TestValGroup_Del(t *testing.T) {
	scenarios := []struct {
		base   ValGroup
		fields Fields
		exp    ValGroup
	}{
		{
			ValGroup{Expiry: 0,
				Fields: Fields([][]byte{[]byte("foo"), []byte("hi")}),
				Values: Values([][]byte{[]byte("bar"), []byte("bye")}),
			},
			Fields([][]byte{[]byte("foo"), []byte("extra")}),
			ValGroup{Expiry: 0,
				Fields: Fields([][]byte{[]byte("hi")}),
				Values: Values([][]byte{[]byte("bye")}),
			},
		},
		{
			ValGroup{Expiry: 0,
				Fields: Fields([][]byte{[]byte("foo"), []byte("hi")}),
				Values: Values([][]byte{[]byte("bar"), []byte("bye")}),
			},
			Fields([][]byte{}),
			ValGroup{Expiry: 0,
				Fields: Fields([][]byte{[]byte("foo"), []byte("hi")}),
				Values: Values([][]byte{[]byte("bar"), []byte("bye")}),
			},
		},
		{
			ValGroup{Expiry: 0, Fields: nil, Values: nil},
			Fields([][]byte{[]byte("foo"), []byte("extra")}),
			ValGroup{Expiry: 0, Fields: nil, Values: nil},
		},
	}
	for _, scene := range scenarios {
		scene.base.Del(scene.fields)
		assert.Equal(t, scene.exp, scene.base)
	}
}
