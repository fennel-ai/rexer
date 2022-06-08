package operators

import (
	"fennel/lib/value"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
			vals:   []value.Value{value.Int(1), value.String("hello")},
			key:    "p1",
			val:    value.Int(1),
			ok:     true,
			err:    false,
		},
		{
			sig:    op.Signature(),
			static: true,
			vals:   []value.Value{value.Int(1), value.String("hello")},
			key:    "p3",
			val:    value.String("hello"),
			ok:     true,
			err:    false,
		},
		{
			sig:    op.Signature(),
			static: true,
			vals:   []value.Value{value.Int(1), value.String("hello"), value.NewList()},
			err:    true,
		},
		{
			sig:    op.Signature(),
			static: true,
			vals:   []value.Value{value.Int(1), value.String("hello")},
			key:    "p2",
			val:    nil,
			ok:     false,
			err:    false,
		},
		{
			sig:    op.Signature(),
			static: false,
			vals:   []value.Value{value.Int(1)},
			key:    "p3",
			ok:     false,
		},
		{
			sig:    op.Signature(),
			static: false,
			vals:   []value.Value{value.Int(1)},
			key:    "p2",
			val:    value.Int(1),
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
