package operators

import (
	"fennel/lib/value"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTypeCheckKwargs(t *testing.T) {
	t.Parallel()
	op := testOp{}
	scenarios := []struct {
		given   []value.Value
		static  bool
		matches bool
	}{
		{
			[]value.Value{value.Bool(true), value.String("abc"), value.Nil},
			true,
			true,
		},
		{
			[]value.Value{value.Bool(false), value.Double(4.0), value.Nil},
			true,
			true,
		},
		{
			[]value.Value{value.Bool(false), value.Nil, value.NewList()},
			true,
			true,
		},
		{
			[]value.Value{value.Bool(false)},
			true,
			false,
		},
		{
			[]value.Value{value.Int(1), value.Nil, value.Nil},
			true,
			false,
		},
		{
			[]value.Value{},
			false,
			false,
		},
		{
			[]value.Value{value.Double(2.0)},
			false,
			true,
		},
		{},
	}
	for _, scenario := range scenarios {
		_, err := NewKwargs(op.Signature(), scenario.given, scenario.static)
		if scenario.matches {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}
func TestKwargs_Get(t *testing.T) {
	op := testOp{}
	scenarios := []struct {
		sig    *Signature
		vals   []value.Value
		static bool
		key    string
		val    value.Value
		ok     bool
		err    bool
	}{
		{
			sig:    op.Signature(),
			static: true,
			vals:   []value.Value{value.Bool(true), value.String("hello"), value.Nil},
			key:    "p1",
			val:    value.Bool(true),
			ok:     true,
			err:    false,
		},
		{
			sig:    op.Signature(),
			static: true,
			vals:   []value.Value{value.Bool(false), value.String("hello"), value.Nil},
			key:    "p3",
			val:    value.String("hello"),
			ok:     true,
			err:    false,
		},
		{
			sig:    op.Signature(),
			static: true,
			key:    "p4",
			vals:   []value.Value{value.Bool(true), value.String("hello"), value.NewList()},
			val:    value.NewList(),
			ok:     true,
			err:    false,
		},
		{
			sig:    op.Signature(),
			static: true,
			vals:   []value.Value{value.Bool(true), value.String("hello"), value.Nil, value.Nil},
			err:    true,
		},
		{
			sig:    op.Signature(),
			static: true,
			vals:   []value.Value{value.Bool(false), value.String("hello"), value.Nil},
			key:    "p2",
			val:    nil,
			ok:     false,
			err:    false,
		},
		{
			sig:    op.Signature(),
			static: false,
			vals:   []value.Value{value.Double(1)},
			key:    "p3",
			ok:     false,
			err:    false,
		},
		{
			sig:    op.Signature(),
			static: false,
			vals:   []value.Value{value.Double(1)},
			key:    "p2",
			val:    value.Double(1),
			ok:     true,
			err:    false,
		},
	}
	for _, scene := range scenarios {
		kwarg, err := NewKwargs(scene.sig, scene.vals, scene.static)
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			found, ok := kwarg.Get(scene.key)
			if scene.ok {
				assert.True(t, ok)
				assert.Equal(t, scene.val, found)
				assert.Equal(t, scene.val, kwarg.GetUnsafe(scene.key))
			} else {
				assert.False(t, ok)
				assert.Equal(t, nil, kwarg.GetUnsafe(scene.key))
			}
		}
	}
}
