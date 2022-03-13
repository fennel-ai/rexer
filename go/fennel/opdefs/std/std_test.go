package std

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestFilterOperator_Apply(t *testing.T) {
	intable := []value.Dict{
		{"a.inner": value.Int(1), "b": value.String("hi")},
		{"a.inner": value.Int(1), "b": value.String("bye")},
		{"a.inner": value.Int(7), "b": value.String("hello")},
	}

	// passing where true works
	whereTrue := value.Dict{"where": value.Bool(true)}
	whereFalse := value.Dict{"where": value.Bool(false)}

	contextKwargTable := []value.Dict{whereTrue, whereFalse, whereTrue}
	expected := []value.Dict{
		{"a.inner": value.Int(1), "b": value.String("hi")},
		{"a.inner": value.Int(7), "b": value.String("hello")},
	}

	tr := tier.Tier{}
	optest.Assert(t, tr, &FilterOperator{}, whereTrue, intable, contextKwargTable, expected)

	// and when we filter everything, we should get empty table
	contextKwargTable = []value.Dict{whereFalse, whereFalse, whereFalse}
	optest.Assert(t, tr, &FilterOperator{}, whereTrue, intable, contextKwargTable, []value.Dict{})
}

func TestTakeOperator_Apply(t *testing.T) {
	intable := []value.Dict{
		{"a.inner": value.Int(1), "b": value.String("hi")},
		{"a.inner": value.Int(1), "b": value.String("bye")},
		{"a.inner": value.Int(7), "b": value.String("hello")},
	}

	// passing limit 2 works
	expected := []value.Dict{
		{"a.inner": value.Int(1), "b": value.String("hi")},
		{"a.inner": value.Int(1), "b": value.String("bye")},
	}
	contextKwargTable := []value.Dict{{}, {}, {}}
	tr := tier.Tier{}
	optest.Assert(t, tr, &TakeOperator{}, value.Dict{"limit": value.Int(2)}, intable, contextKwargTable, expected)

	// and when the limit is very large, it only returns intable as it is
	optest.Assert(t, tr, &TakeOperator{}, value.Dict{"limit": value.Int(10000)}, intable, contextKwargTable, intable)
}
