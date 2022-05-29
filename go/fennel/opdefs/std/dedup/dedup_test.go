package dedup

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestDeduper_Apply(t *testing.T) {
	t.Parallel()
	op := deduper{}
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
			},
			value.NewDict(nil),
			[]*value.Dict{
				value.NewDict(map[string]value.Value{"by": value.String("1")}),
				value.NewDict(map[string]value.Value{"by": value.String("1")}),
				value.NewDict(map[string]value.Value{"by": value.Int(3)}),
			},
			false,
			[]value.Value{
				value.NewDict(map[string]value.Value{"x": value.Int(1)}),
				value.NewDict(map[string]value.Value{"x": value.Int(3)}),
			},
		},
		{
			[]value.Value{
				value.NewDict(map[string]value.Value{"x": value.Int(1)}),
				value.NewDict(map[string]value.Value{"x": value.Int(2)}),
				value.NewDict(map[string]value.Value{"x": value.Int(1)}),
			},
			value.NewDict(nil),
			[]*value.Dict{
				value.NewDict(nil),
				value.NewDict(nil),
				value.NewDict(nil),
			},
			false,
			[]value.Value{
				value.NewDict(map[string]value.Value{"x": value.Int(1)}),
				value.NewDict(map[string]value.Value{"x": value.Int(2)}),
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
