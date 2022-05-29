package math

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestMeanop_Apply(t *testing.T) {
	t.Parallel()
	scenarios := []struct {
		inputs   []value.Value
		kwargs   []*value.Dict
		err      bool
		expected []value.Value
	}{
		{
			[]value.Value{
				value.NewList(value.Int(1), value.Int(2), value.Int(-1)),
				value.NewList(value.Int(1), value.Int(2), value.Int(0)),
				value.NewList(value.Double(1), value.Int(2), value.Int(-1)),
				value.NewList(value.Int(1e15), value.Int(2e15), value.Int(-1e15)),
				value.NewList(),
			},
			[]*value.Dict{value.NewDict(nil), value.NewDict(nil), value.NewDict(nil), value.NewDict(nil), value.NewDict(nil)},
			false,
			[]value.Value{
				value.Double(2.0 / 3.0),
				value.Double(1.0),
				value.Double(2.0 / 3.0),
				value.Double(2e15 / 3.0),
				// Empty list should give a "null" mean.
				value.Nil,
			},
		},
		// Mean of values from "of" field in context.
		{
			[]value.Value{
				value.Int(1),
				value.Int(2),
				value.Int(3),
			},
			[]*value.Dict{
				value.NewDict(map[string]value.Value{
					"of": value.NewList(value.Int(1), value.Int(2), value.Int(-1)),
				}),
				value.NewDict(map[string]value.Value{
					"of": value.NewList(value.Int(1), value.Int(2), value.Int(0)),
				}),
				value.NewDict(map[string]value.Value{
					"of": value.NewList(value.Double(1), value.Int(2), value.Int(-1)),
				}),
			},
			false,
			[]value.Value{
				value.Double(2.0 / 3.0),
				value.Double(1.0),
				value.Double(2.0 / 3.0),
			},
		},
		{
			// only numbers allowed
			[]value.Value{value.NewList(value.String("hi"), value.Int(2), value.Int(3))},
			[]*value.Dict{value.NewDict(nil), value.NewDict(nil), value.NewDict(nil)},
			true,
			nil,
		},
	}
	for _, scene := range scenarios {
		if scene.err {
			optest.AssertError(t, tier.Tier{}, meanop{}, value.NewDict(nil), [][]value.Value{scene.inputs}, scene.kwargs)
		} else {
			optest.AssertEqual(t, tier.Tier{}, meanop{}, value.NewDict(nil), [][]value.Value{scene.inputs}, scene.kwargs, scene.expected)
		}
	}
}
