package repeat

import (
	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
	"testing"
)

func TestRepeater_Apply(t *testing.T) {
	scenarios := []struct {
		input    []value.Value
		context  []*value.Dict
		err      bool
		expected []value.Value
	}{
		{
			[]value.Value{value.Int(1), value.Int(2)},
			[]*value.Dict{value.NewDict(map[string]value.Value{"count": value.Int(3)}), value.NewDict(map[string]value.Value{"count": value.Int(2)})},
			false,
			[]value.Value{value.Int(1), value.Int(1), value.Int(1), value.Int(2), value.Int(2)},
		},
		{
			[]value.Value{value.Int(1), value.Int(2)},
			[]*value.Dict{value.NewDict(map[string]value.Value{"count": value.Int(0)}), value.NewDict(map[string]value.Value{"count": value.Int(2)})},
			false,
			[]value.Value{value.Int(2), value.Int(2)},
		},
		{
			[]value.Value{value.Int(1), value.Int(2)},
			[]*value.Dict{value.NewDict(map[string]value.Value{"count": value.Int(2)}), value.NewDict(map[string]value.Value{"count": value.Int(-1)})},
			true, nil,
		},
		{
			[]value.Value{value.Int(1), value.Int(2)},
			[]*value.Dict{value.NewDict(map[string]value.Value{"count": value.Double(2)}), value.NewDict(map[string]value.Value{"count": value.Int(1)})},
			true, nil,
		},
		{
			[]value.Value{value.Int(1), value.Int(2)},
			[]*value.Dict{value.NewDict(map[string]value.Value{"not_count": value.Double(2)}), value.NewDict(map[string]value.Value{"count": value.Int(1)})},
			true, nil,
		},
	}
	for _, s := range scenarios {
		if s.err {
			optest.AssertError(t, tier.Tier{}, repeater{}, value.NewDict(nil), [][]value.Value{s.input}, s.context)
		} else {
			optest.AssertEqual(t, tier.Tier{}, repeater{}, value.NewDict(nil), [][]value.Value{s.input}, s.context, s.expected)
		}
	}
}
