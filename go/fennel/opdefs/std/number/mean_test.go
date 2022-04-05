package number

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestMeanop_Apply(t *testing.T) {
	t.Parallel()
	scenarios := []struct {
		inputs   []value.Value
		err      bool
		expected []value.Value
	}{
		{
			[]value.Value{value.Int(1), value.Int(2), value.Int(-1)},
			false,
			[]value.Value{value.Double(2.0 / 3.0)},
		},
		{
			// output is double even if only start is double
			[]value.Value{value.Int(1), value.Int(2), value.Double(-1)},
			false,
			[]value.Value{value.Double(2. / 3.)},
		},
		{
			// output is double even if one element is double
			[]value.Value{value.Double(1), value.Int(2), value.Int(-1)},
			false,
			[]value.Value{value.Double(2. / 3.)},
		},
		{
			// works on big numbers
			[]value.Value{value.Int(1e15), value.Int(2e15), value.Int(-1e15)},
			false,
			[]value.Value{value.Double((2e15) / 3.)},
		},
		{
			// only numbers allowed
			[]value.Value{value.String("hi"), value.Int(2), value.Int(3)},
			true,
			nil,
		},
	}
	for _, scene := range scenarios {
		empty := []value.Dict{}
		for _ = range scene.inputs {
			empty = append(empty, value.NewDict(nil))
		}
		if scene.err {
			optest.AssertError(t, tier.Tier{}, meanop{}, value.NewDict(nil), [][]value.Value{scene.inputs}, empty)
		} else {
			optest.AssertEqual(t, tier.Tier{}, meanop{}, value.NewDict(nil), [][]value.Value{scene.inputs}, empty, scene.expected)
		}
	}
}
