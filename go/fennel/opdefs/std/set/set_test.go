package set

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestSet(t *testing.T) {
	intable := []value.Value{
		value.NewDict(map[string]value.Value{"orig": value.Int(1)}),
	}
	contextKwargTable := []value.Dict{
		value.NewDict(map[string]value.Value{
			"value": value.Int(42),
		}),
	}
	expected := []value.Value{
		value.NewDict(map[string]value.Value{"orig": value.Int(1), "new_field": value.Int(42)}),
	}
	tr := tier.Tier{}
	optest.AssertEqual(t, tr, &setOperator{}, value.NewDict(map[string]value.Value{
		"field": value.String("new_field"),
	}), intable, contextKwargTable, expected)
}

func TestSetNameContextual(t *testing.T) {
	intable := []value.Value{
		value.NewDict(map[string]value.Value{"orig": value.Int(1)}),
		value.NewDict(map[string]value.Value{"orig": value.Int(2)}),
	}
	contextKwargTable := []value.Dict{
		value.NewDict(map[string]value.Value{
			"field": value.String("new_field_first"),
			"value": value.Int(42),
		}),
		value.NewDict(map[string]value.Value{
			"field": value.String("new_field_second"),
			"value": value.Int(21),
		}),
	}
	expected := []value.Value{
		value.NewDict(map[string]value.Value{"orig": value.Int(1), "new_field_first": value.Int(42)}),
		value.NewDict(map[string]value.Value{"orig": value.Int(2), "new_field_second": value.Int(21)}),
	}
	tr := tier.Tier{}
	optest.AssertEqual(t, tr, &setOperator{}, value.Dict{}, intable, contextKwargTable, expected)
}

func TestSetError(t *testing.T) {
	intable := []value.Value{
		value.NewDict(map[string]value.Value{"orig": value.Int(1)}),
	}
	contextKwargTable := []value.Dict{
		value.NewDict(map[string]value.Value{
			"value": value.Int(42),
		}),
	}
	tr := tier.Tier{}
	// "name" kwarg is not provided.
	optest.AssertError(t, tr, &setOperator{}, value.Dict{}, intable, contextKwargTable)
}

func TestSignatureError(t *testing.T) {
	intable := []value.Value{
		value.NewDict(map[string]value.Value{"orig": value.Int(1)}),
	}
	contextKwargTable := []value.Dict{
		value.NewDict(map[string]value.Value{
			"field": value.Int(2),
			"value": value.Int(42),
		}),
	}
	tr := tier.Tier{}
	// "name" is int instead of string.
	optest.AssertError(t, tr, &setOperator{}, value.Dict{}, intable, contextKwargTable)
}
