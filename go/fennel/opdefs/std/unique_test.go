package std

import (
	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
	"testing"
)

func TestUniqueOperator_Apply(t *testing.T) {
	intable := []value.Dict{
		{"foo": value.Int(2)},
		{"foo": value.Dict{
			"col1": value.String("age"),
		}},
		{"bar": value.List{value.String("bar"), value.String("bar")}},
		{"foo": value.List{value.Int(1), value.Int(4), value.Int(3), value.Int(1)}},
		{"foo": value.List{value.Double(1.0), value.Double(1.0)}},
		{"foo": value.List{value.String("val1"), value.String("val2"), value.String("val1")}},
		{"foo": value.List{value.Double(1.0), value.Int(4), value.Int(3), value.Int(1)}},
		{"foo": value.List{value.Int(1), value.Double(1.0), value.Dict{"foo": value.Double(1.0)}, value.List{value.Int(1)}}},
	}

	contextKwargTable := []value.Dict{{}, {}, {}, {}, {}, {}, {}, {}}

	expected := []value.Dict{
		{"foo": value.Int(2)},
		{"foo": value.Dict{
			"col1": value.String("age"),
		}},
		{"bar": value.List{value.String("bar"), value.String("bar")}},
		{"foo": value.List{value.Int(1), value.Int(4), value.Int(3)}},
		{"foo": value.List{value.Double(1.0)}},
		{"foo": value.List{value.String("val1"), value.String("val2")}},
		{"foo": value.List{value.Double(1.0), value.Int(4), value.Int(3), value.Int(1)}},
		{"foo": value.List{value.Int(1), value.Double(1.0), value.Dict{"foo": value.Double(1.0)}, value.List{value.Int(1)}}},
	}

	tr := tier.Tier{}
	skwargs := value.Dict{"name": value.String("foo")}
	optest.Assert(t, tr, &UniqueOperator{}, skwargs, intable, contextKwargTable, expected)
}
