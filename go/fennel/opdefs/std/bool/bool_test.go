package bool

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestBool(t *testing.T) {
	scenarios := []struct {
		inputs       []value.Value
		static       value.Dict
		kwargs       []value.Dict
		err          bool
		expected_any []value.Value
		expected_all []value.Value
	}{
		{
			[]value.Value{value.Bool(true), value.Bool(false), value.Bool(true)},
			value.NewDict(nil),
			[]value.Dict{value.NewDict(nil), value.NewDict(nil), value.NewDict(nil)},
			false,
			[]value.Value{value.Bool(true)},
			[]value.Value{value.Bool(false)},
		},
		{
			[]value.Value{value.Bool(true), value.Bool(true), value.Bool(true)},
			value.NewDict(nil),
			[]value.Dict{value.NewDict(nil), value.NewDict(nil), value.NewDict(nil)},
			false,
			[]value.Value{value.Bool(true)},
			[]value.Value{value.Bool(true)},
		},
		{
			[]value.Value{value.Bool(false), value.Bool(false), value.Bool(false)},
			value.NewDict(nil),
			[]value.Dict{value.NewDict(nil), value.NewDict(nil), value.NewDict(nil)},
			false,
			[]value.Value{value.Bool(false)},
			[]value.Value{value.Bool(false)},
		},
	}
	for _, scene := range scenarios {
		if scene.err {
			optest.AssertError(t, tier.Tier{}, anyop{}, scene.static, [][]value.Value{scene.inputs}, scene.kwargs)
			optest.AssertError(t, tier.Tier{}, allop{}, scene.static, [][]value.Value{scene.inputs}, scene.kwargs)
		} else {
			optest.AssertEqual(t, tier.Tier{}, anyop{}, scene.static, [][]value.Value{scene.inputs}, scene.kwargs, scene.expected_any)
			optest.AssertEqual(t, tier.Tier{}, allop{}, scene.static, [][]value.Value{scene.inputs}, scene.kwargs, scene.expected_all)
		}
	}
}
