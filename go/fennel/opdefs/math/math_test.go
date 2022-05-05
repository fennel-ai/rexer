package math

import (
	"math"
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestMath(t *testing.T) {
	scenarios := []struct {
		inputs      []value.Value
		static      value.Dict
		kwargs      []value.Dict
		err         bool
		expectedMax []value.Value
		expectedMin []value.Value
	}{
		{
			[]value.Value{
				value.NewList(value.Int(-1), value.Int(2), value.Int(0)),
				value.NewList(value.Int(-25), value.Double(0), value.Int(5)),
				value.NewList(value.Double(-4), value.Int(2), value.Double(3)),
			},
			value.NewDict(nil),
			[]value.Dict{value.NewDict(nil), value.NewDict(nil), value.NewDict(nil)},
			false,
			[]value.Value{value.Int(2), value.Int(5), value.Double(3)},
			[]value.Value{value.Int(-1), value.Int(-25), value.Double(-4)},
		},
		{
			[]value.Value{
				value.Int(1),
				value.Int(2),
				value.Int(3),
			},
			value.NewDict(nil),
			[]value.Dict{
				value.NewDict(map[string]value.Value{
					"of": value.NewList(value.Int(-1), value.Int(2), value.Int(0)),
				}),
				value.NewDict(map[string]value.Value{
					"of": value.NewList(value.Int(-25), value.Double(0), value.Int(5)),
				}),
				value.NewDict(map[string]value.Value{
					"of": value.NewList(value.Double(-4), value.Int(2), value.Double(3)),
				}),
			},
			false,
			[]value.Value{value.Int(2), value.Int(5), value.Double(3)},
			[]value.Value{value.Int(-1), value.Int(-25), value.Double(-4)},
		},
		{
			[]value.Value{
				value.NewList(),
				value.NewList(value.Int(math.MaxInt64), value.Int(math.MinInt64), value.Int(0)),
			},
			value.NewDict(nil),
			[]value.Dict{value.NewDict(nil), value.NewDict(nil)},
			false,
			[]value.Value{value.Nil, value.Int(math.MaxInt64)},
			[]value.Value{value.Nil, value.Int(math.MinInt64)},
		},
		{
			[]value.Value{
				value.NewList(value.String("x")),
			},
			value.NewDict(nil),
			[]value.Dict{value.NewDict(nil)},
			true,
			nil,
			nil,
		},
	}
	for _, scene := range scenarios {
		if scene.err {
			optest.AssertError(t, tier.Tier{}, maxOp{}, scene.static, [][]value.Value{scene.inputs}, scene.kwargs)
			optest.AssertError(t, tier.Tier{}, minOp{}, scene.static, [][]value.Value{scene.inputs}, scene.kwargs)
		} else {
			optest.AssertEqual(t, tier.Tier{}, maxOp{}, scene.static, [][]value.Value{scene.inputs}, scene.kwargs, scene.expectedMax)
			optest.AssertEqual(t, tier.Tier{}, minOp{}, scene.static, [][]value.Value{scene.inputs}, scene.kwargs, scene.expectedMin)
		}
	}
}
