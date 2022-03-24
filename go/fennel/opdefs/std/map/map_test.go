package _map

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestMapper_Apply(t *testing.T) {
	t.Parallel()
	op := mapper{}
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
				value.NewDict(map[string]value.Value{"to": value.String("1")}),
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
	}
	for _, scene := range scenarios {
		if scene.err {
			optest.AssertError(t, tier.Tier{}, op, scene.static, scene.inputs, scene.context)
		} else {
			optest.AssertEqual(t, tier.Tier{}, op, scene.static, scene.inputs, scene.context, scene.expected)
		}
	}
}
