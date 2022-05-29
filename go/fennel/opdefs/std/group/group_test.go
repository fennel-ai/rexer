package group

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestGrouper_Apply(t *testing.T) {
	t.Parallel()
	op := grouper{}
	scenarios := []struct {
		inputs   []value.Value
		static   *value.Dict
		context  []*value.Dict
		err      bool
		expected []value.Value
	}{
		{
			[]value.Value{
				value.NewDict(map[string]value.Value{"x": value.Int(1)}),
				value.NewDict(map[string]value.Value{"x": value.Int(2)}),
				value.NewDict(map[string]value.Value{"x": value.Int(3)}),
				value.NewDict(map[string]value.Value{"x": value.Int(4)}),
			},
			value.NewDict(nil),
			[]*value.Dict{
				value.NewDict(map[string]value.Value{"by": value.String("1")}),
				value.NewDict(map[string]value.Value{"by": value.String("2")}),
				value.NewDict(map[string]value.Value{"by": value.Int(3)}),
				value.NewDict(map[string]value.Value{"by": value.String("2")}),
			},
			false,
			[]value.Value{
				value.NewDict(map[string]value.Value{
					"group":    value.String("1"),
					"elements": value.NewList(value.NewDict(map[string]value.Value{"x": value.Int(1)})),
				}),
				value.NewDict(map[string]value.Value{
					"group": value.String("2"),
					"elements": value.NewList(
						value.NewDict(map[string]value.Value{"x": value.Int(2)}),
						value.NewDict(map[string]value.Value{"x": value.Int(4)})),
				}),
				value.NewDict(map[string]value.Value{
					"group":    value.Int(3),
					"elements": value.NewList(value.NewDict(map[string]value.Value{"x": value.Int(3)})),
				}),
			},
		},
		{
			[]value.Value{
				value.NewDict(map[string]value.Value{"x": value.Int(1)}),
				value.NewDict(map[string]value.Value{"x": value.Int(2)}),
			},
			value.NewDict(nil),
			[]*value.Dict{
				value.NewDict(map[string]value.Value{"ok": value.String("1")}),
				value.NewDict(map[string]value.Value{"not": value.String("2")}),
			},
			true,
			nil,
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
