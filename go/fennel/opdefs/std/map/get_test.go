package _map

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestGetp_Apply(t *testing.T) {
	t.Parallel()
	op := get{}
	scenarios := []struct {
		inputs   []value.Value
		static   value.Dict
		context  []value.Dict
		err      bool
		expected []value.Value
	}{
		{
			[]value.Value{
				value.NewDict(map[string]value.Value{"x": value.Int(1), "y": value.Int(2), "z": value.Int(3)}),
				value.NewDict(map[string]value.Value{"x": value.Int(1), "y": value.Int(2), "z": value.Int(4)}),
				value.NewDict(map[string]value.Value{"x": value.Int(1), "y": value.Int(3), "z": value.Int(3) + value.Int(4)}),
			},
			value.NewDict(nil),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"fields": value.NewList(value.String("x"), value.String("y"), value.String("z"))}),
				value.NewDict(map[string]value.Value{"fields": value.NewList(value.String("x"), value.String("x"), value.String("x"))}),
				value.NewDict(map[string]value.Value{"fields": value.NewList(value.String("z"), value.String("y"), value.String("z"))}),
			},
			false,
			[]value.Value{value.NewList(value.Int(1), value.Int(2), value.Int(3)),
				value.NewList(value.Int(1), value.Int(1), value.Int(1)),
				value.NewList(value.Int(7), value.Int(3), value.Int(7)),
			},
		},
	}
	for _, scene := range scenarios {
		if scene.err {
			optest.AssertError(t, tier.Tier{}, op, scene.static, [][]value.Value{scene.inputs}, scene.context)
		} else {
			optest.AssertEqual(t, tier.Tier{}, op, scene.static, [][]value.Value{scene.inputs}, scene.context, scene.expected)
		}
	}
}
