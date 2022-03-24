package std

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestUniqueOperator_Apply(t *testing.T) {
	intable := []value.Dict{
		value.NewDict(map[string]value.Value{"foo": value.Int(2)}),
		value.NewDict(map[string]value.Value{"foo": value.NewDict(map[string]value.Value{"col1": value.String("age")})}),
		value.NewDict(map[string]value.Value{"bar": value.NewList(value.String("bar"), value.String("bar"))}),
		value.NewDict(map[string]value.Value{"foo": value.NewList(value.Int(1), value.Int(4), value.Int(3), value.Int(1))}),
		value.NewDict(map[string]value.Value{"foo": value.NewList(value.Double(1.0), value.Double(1.0))}),
		value.NewDict(map[string]value.Value{"foo": value.NewList(value.String("val1"), value.String("val2"), value.String("val1"))}),
		value.NewDict(map[string]value.Value{"foo": value.NewList(value.Double(1.0), value.Int(4), value.Int(3), value.Int(1))}),
		value.NewDict(map[string]value.Value{"foo": value.NewList(value.Int(1), value.Double(1.0), value.NewDict(map[string]value.Value{"foo": value.Double(1.0)}), value.NewList(value.Int(1)))}),
	}

	contextKwargTable := []value.Dict{{}, {}, {}, {}, {}, {}, {}, {}}

	expected := []value.Value{
		value.NewDict(map[string]value.Value{"foo": value.Int(2)}),
		value.NewDict(map[string]value.Value{"foo": value.NewDict(map[string]value.Value{"col1": value.String("age")})}),
		value.NewDict(map[string]value.Value{"bar": value.NewList(value.String("bar"), value.String("bar"))}),
		value.NewDict(map[string]value.Value{"foo": value.NewList(value.Int(1), value.Int(4), value.Int(3))}),
		value.NewDict(map[string]value.Value{"foo": value.NewList(value.Double(1.0))}),
		value.NewDict(map[string]value.Value{"foo": value.NewList(value.String("val1"), value.String("val2"))}),
		value.NewDict(map[string]value.Value{"foo": value.NewList(value.Double(1.0), value.Int(4), value.Int(3), value.Int(1))}),
		value.NewDict(map[string]value.Value{"foo": value.NewList(value.Int(1), value.Double(1.0), value.NewDict(map[string]value.Value{"foo": value.Double(1.0)}))}),
	}

	tr := tier.Tier{}
	skwargs := value.NewDict(map[string]value.Value{"name": value.String("foo")})
	optest.Assert(t, tr, &UniqueOperator{}, skwargs, intable, contextKwargTable, expected)
}
