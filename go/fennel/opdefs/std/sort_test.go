package std

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestSortOperator_Apply(t *testing.T) {
	intable := []value.Value{
		value.NewDict(map[string]value.Value{"name": value.String("first")}),
		value.NewDict(map[string]value.Value{"name": value.String("second")}),
		value.NewDict(map[string]value.Value{"name": value.String("third")}),
	}
	contextKwargs := []value.Dict{
		value.NewDict(map[string]value.Value{"by": value.Int(2)}),
		value.NewDict(map[string]value.Value{"by": value.Double(1.0)}),
		value.NewDict(map[string]value.Value{"by": value.Double(3.0)}),
	}

	orders := []value.Dict{
		value.NewDict(map[string]value.Value{"reverse": value.Bool(false)}),
		value.NewDict(map[string]value.Value{"reverse": value.Bool(true)}),
	}

	expected := [][]value.Value{
		{
			value.NewDict(map[string]value.Value{"name": value.String("second")}),
			value.NewDict(map[string]value.Value{"name": value.String("first")}),
			value.NewDict(map[string]value.Value{"name": value.String("third")}),
		},
		{
			value.NewDict(map[string]value.Value{"name": value.String("third")}),
			value.NewDict(map[string]value.Value{"name": value.String("first")}),
			value.NewDict(map[string]value.Value{"name": value.String("second")}),
		},
	}

	tr := tier.Tier{}
	optest.AssertEqual(t, tr, &SortOperator{}, orders[0], [][]value.Value{intable}, contextKwargs, expected[0])
	optest.AssertEqual(t, tr, &SortOperator{}, orders[1], [][]value.Value{intable}, contextKwargs, expected[1])

	// Sort a list of numbers.
	optest.AssertEqual(t, tr, &SortOperator{}, value.NewDict(nil),
		[][]value.Value{{value.Int(2), value.Int(1), value.Double(3.0)}},
		[]value.Dict{value.NewDict(nil), value.NewDict(nil), value.NewDict(nil)},
		[]value.Value{value.Int(1), value.Int(2), value.Double(3.0)},
	)
}
