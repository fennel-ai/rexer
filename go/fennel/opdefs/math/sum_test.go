package math

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestAdder_Apply(t *testing.T) {
	t.Parallel()
	scenarios := []struct {
		inputs   []value.Value
		static   value.Dict
		kwargs   []value.Dict
		err      bool
		expected []value.Value
	}{
		{
			[]value.Value{
				value.NewList(value.Int(1), value.Int(2), value.Int(-1)),
				value.NewList(value.Int(-1), value.Int(0), value.Int(1)),
			},
			value.NewDict(nil),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"zero": value.Int(3)}),
				value.NewDict(map[string]value.Value{"zero": value.Int(0)}),
			},
			false,
			[]value.Value{value.Int(5), value.Int(0)},
		},
		// Sum of values coming from "of" context kwarg.
		{
			[]value.Value{value.Int(1), value.Int(2)},
			value.NewDict(nil),
			[]value.Dict{
				value.NewDict(map[string]value.Value{
					"zero": value.Int(3),
					"of":   value.NewList(value.Int(1), value.Int(2), value.Int(1)),
				}),
				value.NewDict(map[string]value.Value{
					"zero": value.Int(0),
					"of":   value.NewList(value.Int(-1), value.Int(0), value.Int(1)),
				}),
			},
			false,
			[]value.Value{value.Int(7), value.Int(0)},
		},
		{
			// output is double even if only zero is double
			// and even if only one element is double
			[]value.Value{
				value.NewList(value.Int(1), value.Int(2), value.Int(-1)),
				value.NewList(value.Int(1), value.Double(2), value.Int(-1)),
			},
			value.NewDict(nil),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"zero": value.Double(3)}),
				value.NewDict(map[string]value.Value{"zero": value.Int(0)}),
			},
			false,
			[]value.Value{value.Double(5), value.Double(2)},
		},
		{
			// works on big numbers
			[]value.Value{value.NewList(value.Int(1e15), value.Int(2e15), value.Int(-1e15))},
			value.NewDict(nil),
			[]value.Dict{value.NewDict(map[string]value.Value{})},
			false,
			[]value.Value{value.Double(2e15)},
		},
		{
			// only numbers allowed
			[]value.Value{value.NewList(value.String("hi"), value.Int(2), value.Int(3))},
			value.NewDict(nil),
			[]value.Dict{value.NewDict(map[string]value.Value{"zero": value.Double(0)})},
			true,
			nil,
		},
	}
	for _, scene := range scenarios {
		if scene.err {
			optest.AssertError(t, tier.Tier{}, adder{}, scene.static, [][]value.Value{scene.inputs}, scene.kwargs)
		} else {
			optest.AssertEqual(t, tier.Tier{}, adder{}, scene.static, [][]value.Value{scene.inputs}, scene.kwargs, scene.expected)
		}
	}
}
