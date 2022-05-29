package std

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestExplodeOperator_KeyNotString(t *testing.T) {
	intable := []value.Value{
		value.NewDict(map[string]value.Value{"a.list": value.NewList(value.Int(1), value.Int(5)), "b": value.String("hi")}),
	}

	tr := tier.Tier{}
	skwargs := value.NewDict(map[string]value.Value{"field": value.Int(2)})
	optest.AssertError(t, tr, &ExplodeOperator{}, skwargs, [][]value.Value{intable}, []*value.Dict{{}})
	skwargs = value.NewDict(map[string]value.Value{"field": value.NewList(value.Int(2))})
	optest.AssertError(t, tr, &ExplodeOperator{}, skwargs, [][]value.Value{intable}, []*value.Dict{{}})
	skwargs = value.NewDict(map[string]value.Value{"field": value.NewList(value.String("a.list"), value.Int(2))})
	optest.AssertError(t, tr, &ExplodeOperator{}, skwargs, [][]value.Value{intable}, []*value.Dict{{}})
}

func TestExplodeOperator_KeyNotPresent(t *testing.T) {
	intable := []value.Value{
		value.NewDict(map[string]value.Value{"a.list": value.NewList(value.Int(1), value.Int(5)), "b": value.String("hi")}),
	}

	tr := tier.Tier{}
	skwargs := value.NewDict(map[string]value.Value{"field": value.String("c")})
	optest.AssertError(t, tr, &ExplodeOperator{}, skwargs, [][]value.Value{intable}, []*value.Dict{{}})
}

func TestExplodeOperator_ListScalarKeys(t *testing.T) {
	intable := []value.Value{
		value.NewDict(map[string]value.Value{"a.list": value.NewList(value.Int(1), value.Int(5)), "b": value.String("hi")}),
	}

	tr := tier.Tier{}
	skwargs := value.NewDict(map[string]value.Value{"field": value.NewList(value.String("a.list"), value.String("b"))})
	optest.AssertError(t, tr, &ExplodeOperator{}, skwargs, [][]value.Value{intable}, []*value.Dict{{}})
}

func TestExplodeOperator_ScalarKeys(t *testing.T) {
	intable := []value.Value{
		value.NewDict(map[string]value.Value{"a": value.Int(1), "b": value.String("hi")}),
	}
	outtable := []value.Value{intable[0]}

	tr := tier.Tier{}
	skwargs := value.NewDict(map[string]value.Value{"field": value.NewList(value.String("a"), value.String("b"))})
	optest.AssertElementsMatch(t, tr, &ExplodeOperator{}, skwargs, [][]value.Value{intable}, []*value.Dict{{}}, outtable)
}

func TestExplodeOperator_NonMatchingRowWiseElements(t *testing.T) {
	intable := []value.Value{
		value.NewDict(map[string]value.Value{"a.list": value.NewList(value.Int(1), value.Int(5)), "b": value.NewList(value.String("hi"))}),
	}

	tr := tier.Tier{}
	skwargs := value.NewDict(map[string]value.Value{"field": value.NewList(value.String("a.list"), value.String("b"))})
	optest.AssertError(t, tr, &ExplodeOperator{}, skwargs, [][]value.Value{intable}, []*value.Dict{value.NewDict(nil)})
}

func TestExplodeOperator_Apply(t *testing.T) {
	intable := []value.Value{
		value.NewDict(map[string]value.Value{"a.list": value.NewList(value.Int(1), value.Int(5)), "b": value.String("hi")}),
		value.NewDict(map[string]value.Value{"a.list": value.NewList(value.Int(10), value.Int(15)), "b": value.String("bye")}),
		value.NewDict(map[string]value.Value{"a.list": value.NewList(value.Int(3), value.Int(8)), "b": value.String("hello")}),
	}

	contextKwargTable := []*value.Dict{{}, {}, {}}

	expected := []value.Value{
		value.NewDict(map[string]value.Value{"a.list": value.Int(1), "b": value.String("hi")}),
		value.NewDict(map[string]value.Value{"a.list": value.Int(5), "b": value.String("hi")}),
		value.NewDict(map[string]value.Value{"a.list": value.Int(10), "b": value.String("bye")}),
		value.NewDict(map[string]value.Value{"a.list": value.Int(15), "b": value.String("bye")}),
		value.NewDict(map[string]value.Value{"a.list": value.Int(3), "b": value.String("hello")}),
		value.NewDict(map[string]value.Value{"a.list": value.Int(8), "b": value.String("hello")}),
	}

	tr := tier.Tier{}
	skwargs := value.NewDict(map[string]value.Value{"field": value.String("a.list")})
	optest.AssertElementsMatch(t, tr, &ExplodeOperator{}, skwargs, [][]value.Value{intable}, contextKwargTable, expected)
	skwargs = value.NewDict(map[string]value.Value{"field": value.NewList(value.String("a.list"))})
	optest.AssertElementsMatch(t, tr, &ExplodeOperator{}, skwargs, [][]value.Value{intable}, contextKwargTable, expected)
}

func TestExplodeOperator_ApplyListKeys(t *testing.T) {
	intable := []value.Value{
		value.NewDict(map[string]value.Value{"a.list": value.NewList(value.Int(1), value.Int(5)), "b.list": value.NewList(value.String("hi"), value.String("hello"))}),
		value.NewDict(map[string]value.Value{"a.list": value.NewList(value.Int(10)), "b.list": value.NewList(value.String("hi"))}),
		value.NewDict(map[string]value.Value{"a.list": value.NewList(), "b.list": value.NewList()}),
	}

	contextKwargTable := []*value.Dict{{}, {}, {}}

	expected := []value.Value{
		value.NewDict(map[string]value.Value{"a.list": value.Int(1), "b.list": value.String("hi")}),
		value.NewDict(map[string]value.Value{"a.list": value.Int(5), "b.list": value.String("hello")}),
		value.NewDict(map[string]value.Value{"a.list": value.Int(10), "b.list": value.String("hi")}),
		value.NewDict(map[string]value.Value{"a.list": value.Nil, "b.list": value.Nil}),
	}

	tr := tier.Tier{}
	skwargs := value.NewDict(map[string]value.Value{"field": value.NewList(value.String("a.list"), value.String("b.list"))})
	// should work with a list of strings
	optest.AssertElementsMatch(t, tr, &ExplodeOperator{}, skwargs, [][]value.Value{intable}, contextKwargTable, expected)
}
