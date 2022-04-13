package _map

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestMapLookup_Apply(t *testing.T) {
	t.Parallel()
	op := map_lookup{}
	scenarios := []struct {
		inputs   []value.Value
		static   value.Dict
		context  []value.Dict
		err      bool
		expected []value.Value
	}{
		{
			[]value.Value{
				value.NewDict(map[string]value.Value{"x": value.Int(1)}),
				value.NewDict(map[string]value.Value{"x": value.Int(2)}),
				value.NewDict(map[string]value.Value{"x": value.Int(3)}),
			},
			value.NewDict(nil),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"keys": value.List(["k1", "k2", "k3"])}),
				value.NewDict(map[string]value.Value{"to": value.String("2")}),
				value.NewDict(map[string]value.Value{"to": value.Int(3)}),
			},
			false,
			[]value.Value{value.String("1"), value.String("2"), value.Int(3)},
		},
		{
			[]value.Value{
				value.NewDict(map[string]value.Value{"x": value.Int(1)}),
				value.NewDict(map[string]value.Value{"x": value.Int(3)}),
			},
			value.NewDict(nil),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"to": value.String("1")}),
				value.NewDict(map[string]value.Value{"from": value.String("2")}),
			},
			true,
			nil,
		},
		{
			[]value.Value{
				value.Int(1), value.Int(2), value.Int(3),
				value.String("1"), value.String("2"), value.String("3"),
			},
			value.NewDict(nil),

			[]value.Dict{
				value.NewDict(map[string]value.Value{"to": value.NewList(value.Int(1), value.String("1"))}),
				value.NewDict(map[string]value.Value{"to": value.NewList(value.Int(2), value.String("2"))}),
				value.NewDict(map[string]value.Value{"to": value.NewList(value.Int(3), value.String("3"))}),
			},
			false,
			[]value.Value{value.NewList(value.Int(1), value.String("1")), value.NewList(value.Int(2), value.String("2")), value.NewList(value.Int(3), value.String("3"))},
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
