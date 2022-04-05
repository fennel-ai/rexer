package number

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
			[]value.Value{value.Int(1), value.Int(2), value.Int(-1)},
			value.NewDict(map[string]value.Value{"start": value.Int(3)}),
			[]value.Dict{value.NewDict(nil), value.NewDict(nil), value.NewDict(nil)},
			false,
			[]value.Value{value.Int(5)},
		},
		{
			// output is double even if only start is double
			[]value.Value{value.Int(1), value.Int(2), value.Double(-1)},
			value.NewDict(map[string]value.Value{"start": value.Int(3)}),
			[]value.Dict{value.NewDict(nil), value.NewDict(nil), value.NewDict(nil)},
			false,
			[]value.Value{value.Double(5)},
		},
		{
			// output is double even if one element is double
			[]value.Value{value.Double(1), value.Int(2), value.Int(-1)},
			value.NewDict(nil),
			[]value.Dict{value.NewDict(nil), value.NewDict(nil), value.NewDict(nil)},
			false,
			[]value.Value{value.Double(2)},
		},
		{
			// works on big numbers
			[]value.Value{value.Int(1e15), value.Int(2e15), value.Int(-1e15)},
			value.NewDict(map[string]value.Value{"start": value.Double(0)}),
			[]value.Dict{value.NewDict(nil), value.NewDict(nil), value.NewDict(nil)},
			false,
			[]value.Value{value.Double(2e15)},
		},
		{
			// only numbers allowed
			[]value.Value{value.String("hi"), value.Int(2), value.Int(3)},
			value.NewDict(map[string]value.Value{"start": value.Double(0)}),
			[]value.Dict{value.NewDict(nil), value.NewDict(nil), value.NewDict(nil)},
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
