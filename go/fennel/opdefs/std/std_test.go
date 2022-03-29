package std

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestFilterOperator_Apply(t *testing.T) {
	intable := []value.Value{
		value.NewDict(map[string]value.Value{"a.inner": value.Int(1), "b": value.String("hi")}),
		value.NewDict(map[string]value.Value{"a.inner": value.Int(1), "b": value.String("bye")}),
		value.NewDict(map[string]value.Value{"a.inner": value.Int(7), "b": value.String("hello")}),
	}

	// passing where true works
	whereTrue := value.NewDict(map[string]value.Value{"where": value.Bool(true)})
	whereFalse := value.NewDict(map[string]value.Value{"where": value.Bool(false)})

	contextKwargTable := []value.Dict{whereTrue, whereFalse, whereTrue}
	expected := []value.Value{
		value.NewDict(map[string]value.Value{"a.inner": value.Int(1), "b": value.String("hi")}),
		value.NewDict(map[string]value.Value{"a.inner": value.Int(7), "b": value.String("hello")}),
	}

	tr := tier.Tier{}
	optest.AssertElementsMatch(t, tr, &FilterOperator{}, whereTrue, intable, contextKwargTable, expected)

	// and when we filter everything, we should get empty table
	contextKwargTable = []value.Dict{whereFalse, whereFalse, whereFalse}
	optest.AssertElementsMatch(t, tr, &FilterOperator{}, whereTrue, intable, contextKwargTable, []value.Value{})
}

func TestTakeOperator_Apply(t *testing.T) {
	intable := []value.Value{
		value.NewDict(map[string]value.Value{"a.inner": value.Int(1), "b": value.String("hi")}),
		value.NewDict(map[string]value.Value{"a.inner": value.Int(1), "b": value.String("bye")}),
		value.NewDict(map[string]value.Value{"a.inner": value.Int(7), "b": value.String("hello")}),
	}
	// and when the limit is very large, it only returns intable as it is
	outtable := make([]value.Value, len(intable))
	for i, input := range intable {
		outtable[i] = input
	}

	// passing limit 2 works
	expected := []value.Value{
		value.NewDict(map[string]value.Value{"a.inner": value.Int(1), "b": value.String("hi")}),
		value.NewDict(map[string]value.Value{"a.inner": value.Int(1), "b": value.String("bye")}),
	}
	contextKwargTable := []value.Dict{{}, {}, {}}
	tr := tier.Tier{}
	optest.AssertElementsMatch(t, tr, &TakeOperator{}, value.NewDict(map[string]value.Value{"limit": value.Int(2)}), intable, contextKwargTable, expected)

	optest.AssertEqual(t, tr, &TakeOperator{}, value.NewDict(map[string]value.Value{"limit": value.Int(10000)}), intable, contextKwargTable, outtable)
}
