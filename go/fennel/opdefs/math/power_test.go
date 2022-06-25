package math

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestPower(t *testing.T) {
	scenarios := []struct {
		inputs        []value.Value
		static        value.Dict
		kwargs        []value.Dict
		err           bool
		expectedPower []value.Value
	}{
		{
			[]value.Value{
				value.Int(-1), value.Int(2), value.Int(0),
			},
			value.NewDict(nil),
			[]value.Dict{value.NewDict(map[string]value.Value{"power": value.Double(2)}), value.NewDict(map[string]value.Value{"power": value.Double(2)}), value.NewDict(map[string]value.Value{"power": value.Double(2)})},
			false,
			[]value.Value{value.Int(1), value.Int(4), value.Double(0)},
		},
		{
			[]value.Value{
				value.Int(1),
				value.Int(2),
				value.Int(3),
				value.Int(4),
				value.Int(5),
			},
			value.NewDict(nil),
			[]value.Dict{
				value.NewDict(map[string]value.Value{
					"of":    value.Int(-1),
					"power": value.Double(2),
				}),
				value.NewDict(map[string]value.Value{
					"of":    value.Int(-25),
					"power": value.Double(2),
				}),
				value.NewDict(map[string]value.Value{
					"of":    value.Double(-4),
					"power": value.Double(2),
				}),
				value.NewDict(map[string]value.Value{
					"of":    value.Int(4),
					"power": value.Double(0.5),
				}),
				value.NewDict(map[string]value.Value{
					"of":    value.Int(81),
					"power": value.Double(0.5),
				}),
			},
			false,
			[]value.Value{value.Int(1), value.Int(625), value.Double(16), value.Int(2), value.Int(9)},
		},
	}
	for _, scene := range scenarios {
		if scene.err {
			optest.AssertError(t, tier.Tier{}, powerOp{}, scene.static, [][]value.Value{scene.inputs}, scene.kwargs)
		} else {
			optest.AssertEqual(t, tier.Tier{}, powerOp{}, scene.static, [][]value.Value{scene.inputs}, scene.kwargs, scene.expectedPower)
		}
	}
}
