package std

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestSortOp_Apply(t *testing.T) {
	intable := []value.Dict{
		{"name": value.String("first")},
		{"name": value.String("second")},
		{"name": value.String("third")},
	}
	contextKwargs := []value.Dict{
		{"by": value.Int(2)},
		{"by": value.Double(1.0)},
		{"by": value.Double(3.0)},
	}

	orders := []value.Dict{{"desc": value.Bool(false)}, {"desc": value.Bool(false)}}

	expected := [][]value.Dict{
		{
			{"name": value.String("second")},
			{"name": value.String("first")},
			{"name": value.String("third")},
		},
		{
			{"name": value.String("third")},
			{"name": value.String("first")},
			{"name": value.String("second")},
		},
	}

	tr := tier.Tier{}
	optest.Assert(t, tr, &SortOperator{}, orders[0], intable, contextKwargs, expected[0])
	optest.Assert(t, tr, &SortOperator{}, orders[1], intable, contextKwargs, expected[1])
}
