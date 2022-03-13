package std

import (
	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
	"testing"
)

func TestExplodeOperator_KeyNotString(t *testing.T) {
	intable := []value.Dict{
		{"a.list": value.List{value.Int(1), value.Int(5)}, "b": value.String("hi")},
	}

	tr := tier.Tier{}
	skwargs := value.Dict{"keys": value.Int(2)}
	optest.AssertError(t, tr, &ExplodeOperator{}, skwargs, intable, []value.Dict{{}})
	skwargs = value.Dict{"keys": value.List{value.Int(2)}}
	optest.AssertError(t, tr, &ExplodeOperator{}, skwargs, intable, []value.Dict{{}})
	skwargs = value.Dict{"keys": value.List{value.String("a.list"), value.Int(2)}}
	optest.AssertError(t, tr, &ExplodeOperator{}, skwargs, intable, []value.Dict{{}})
}

func TestExplodeOperator_KeyNotPresent(t *testing.T) {
	intable := []value.Dict{
		{"a.list": value.List{value.Int(1), value.Int(5)}, "b": value.String("hi")},
	}

	tr := tier.Tier{}
	skwargs := value.Dict{"keys": value.String("c")}
	optest.AssertError(t, tr, &ExplodeOperator{}, skwargs, intable, []value.Dict{{}})
}

func TestExplodeOperator_ListScalarKeys(t *testing.T) {
	intable := []value.Dict{
		{"a.list": value.List{value.Int(1), value.Int(5)}, "b": value.String("hi")},
	}

	tr := tier.Tier{}
	skwargs := value.Dict{"keys": value.List{value.String("a.list"), value.String("b")}}
	optest.AssertError(t, tr, &ExplodeOperator{}, skwargs, intable, []value.Dict{{}})
}

func TestExplodeOperator_ScalarKeys(t *testing.T) {
	intable := []value.Dict{
		{"a": value.Int(1), "b": value.String("hi")},
	}

	tr := tier.Tier{}
	skwargs := value.Dict{"keys": value.List{value.String("a"), value.String("b")}}
	optest.Assert(t, tr, &ExplodeOperator{}, skwargs, intable, []value.Dict{{}}, intable)
}

func TestExplodeOperator_NonMatchingRowWiseElements(t *testing.T) {
	intable := []value.Dict{
		{"a.list": value.List{value.Int(1), value.Int(5)}, "b": value.List{value.String("hi")}},
	}

	tr := tier.Tier{}
	skwargs := value.Dict{"keys": value.List{value.String("a.list"), value.String("b")}}
	optest.AssertError(t, tr, &ExplodeOperator{}, skwargs, intable, []value.Dict{{}})
}

func TestExplodeOperator_Apply(t *testing.T) {
	intable := []value.Dict{
		{"a.list": value.List{value.Int(1), value.Int(5)}, "b": value.String("hi")},
		{"a.list": value.List{value.Int(10), value.Int(15)}, "b": value.String("bye")},
		{"a.list": value.List{value.Int(3), value.Int(8)}, "b": value.String("hello")},
	}

	contextKwargTable := []value.Dict{{}, {}, {}}

	expected := []value.Dict{
		{"a.list": value.Int(1), "b": value.String("hi")},
		{"a.list": value.Int(5), "b": value.String("hi")},
		{"a.list": value.Int(10), "b": value.String("bye")},
		{"a.list": value.Int(15), "b": value.String("bye")},
		{"a.list": value.Int(3), "b": value.String("hello")},
		{"a.list": value.Int(8), "b": value.String("hello")},
	}

	tr := tier.Tier{}
	skwargs := value.Dict{"keys": value.String("a.list")}
	optest.Assert(t, tr, &ExplodeOperator{}, skwargs, intable, contextKwargTable, expected)
	skwargs = value.Dict{"keys": value.List{value.String("a.list")}}
	optest.Assert(t, tr, &ExplodeOperator{}, skwargs, intable, contextKwargTable, expected)
}

func TestExplodeOperator_ApplyListKeys(t *testing.T) {
	intable := []value.Dict{
		{"a.list": value.List{value.Int(1), value.Int(5)}, "b.list": value.List{value.String("hi"), value.String("hello")}},
		{"a.list": value.List{value.Int(10)}, "b.list": value.List{value.String("hi")}},
		{"a.list": value.List{}, "b.list": value.List{}},
	}

	contextKwargTable := []value.Dict{{}, {}, {}}

	expected := []value.Dict{
		{"a.list": value.Int(1), "b.list": value.String("hi")},
		{"a.list": value.Int(5), "b.list": value.String("hello")},
		{"a.list": value.Int(10), "b.list": value.String("hi")},
		{"a.list": value.Nil, "b.list": value.Nil},
	}

	tr := tier.Tier{}
	skwargs := value.Dict{"keys": value.List{value.String("a.list"), value.String("b.list")}}
	// should work with a list of strings
	optest.Assert(t, tr, &ExplodeOperator{}, skwargs, intable, contextKwargTable, expected)
}
