package zip

import (
	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
	"testing"
)

func TestZipper_Apply(t *testing.T) {
	t.Parallel()
	op := zipper{}
	scenarios := []struct {
		inputs   [][]value.Value
		static   value.Dict
		context  []value.Dict
		err      bool
		expected []value.Value
	}{
		{
			[][]value.Value{
				{value.Int(1), value.Int(2), value.Int(3)},
				{value.String("1"), value.String("2"), value.String("3")},
			},
			value.NewDict(nil),

			[]value.Dict{value.NewDict(nil), value.NewDict(nil), value.NewDict(nil)},
			false,
			[]value.Value{value.NewList(value.Int(1), value.String("1")), value.NewList(value.Int(2), value.String("2")), value.NewList(value.Int(3), value.String("3"))},
		},
		{
			[][]value.Value{
				{value.Int(1), value.Int(2), value.Int(3)},
				{value.NewList(value.String("1")), value.String("2"), value.String("3")},
			},
			value.NewDict(nil),

			[]value.Dict{value.NewDict(nil), value.NewDict(nil), value.NewDict(nil)},
			false,
			[]value.Value{value.NewList(value.Int(1), value.NewList(value.String("1"))), value.NewList(value.Int(2), value.String("2")), value.NewList(value.Int(3), value.String("3"))},
		},
		{
			[][]value.Value{
				{value.Int(1), value.Int(2), value.Int(3)},
			},
			value.NewDict(nil),

			[]value.Dict{value.NewDict(nil), value.NewDict(nil), value.NewDict(nil)},
			false,
			[]value.Value{value.NewList(value.Int(1)), value.NewList(value.Int(2)), value.NewList(value.Int(3))},
		},
	}
	for _, scene := range scenarios {
		if scene.err {
			optest.AssertError(t, tier.Tier{}, op, scene.static, scene.inputs, scene.context)
		} else {
			optest.AssertEqual(t, tier.Tier{}, op, scene.static, scene.inputs, scene.context, scene.expected)
		}
	}
}
