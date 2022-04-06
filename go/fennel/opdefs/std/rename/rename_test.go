package rename

import (
	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
	"testing"
)

func TestRenamer_Apply(t *testing.T) {
	scenarios := []struct {
		input   []value.Value
		context []value.Dict
		err     bool
		output  []value.Value
	}{
		{
			[]value.Value{
				value.NewDict(map[string]value.Value{
					"x": value.Int(1), "y": value.Int(2), "z": value.Int(3),
				}),
				value.NewDict(map[string]value.Value{
					"x": value.Int(4), "y": value.Int(5), "z": value.Int(6),
				}),
			},
			[]value.Dict{
				value.NewDict(map[string]value.Value{
					"from": value.String("x"), "to": value.String("a"),
				}),
				value.NewDict(map[string]value.Value{
					"from": value.String("y"), "to": value.String("b"),
				}),
			},
			false,
			[]value.Value{
				value.NewDict(map[string]value.Value{
					"a": value.Int(1), "y": value.Int(2), "z": value.Int(3),
				}),
				value.NewDict(map[string]value.Value{
					"x": value.Int(4), "b": value.Int(5), "z": value.Int(6),
				}),
			},
		},
		{
			[]value.Value{value.NewDict(map[string]value.Value{"x": value.Int(1), "y": value.Int(2), "z": value.Int(3)})},
			[]value.Dict{value.NewDict(map[string]value.Value{"from": value.NewList(value.String("x")), "to": value.String("a")})},
			true, nil,
		},
		{
			[]value.Value{value.NewDict(map[string]value.Value{"x": value.Int(1), "y": value.Int(2), "z": value.Int(3)})},
			[]value.Dict{value.NewDict(map[string]value.Value{"to": value.String("a")})},
			true, nil,
		},
	}
	for _, scene := range scenarios {
		if scene.err {
			optest.AssertError(t, tier.Tier{}, renamer{}, value.NewDict(nil), [][]value.Value{scene.input}, scene.context)
		} else {
			optest.AssertEqual(t, tier.Tier{}, renamer{}, value.NewDict(nil), [][]value.Value{scene.input}, scene.context, scene.output)
		}
	}
}
