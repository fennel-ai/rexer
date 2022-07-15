package model

import (
	"testing"

	"fennel/lib/value"
	"fennel/test"
	"fennel/test/optest"

	"fennel/engine/interpreter"
)

func TestPredictErrorNoModelParam(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	intable := []value.Value{
		value.Nil,
	}
	contextKwargTable := []value.Dict{
		value.NewDict(map[string]value.Value{
			"input": value.NewList(value.String("1:1 9:1 19:1 21:1 24:1 34:1 36:1 39:1 42:1 53:1 56:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 117:1 122:1")),
		}),
	}

	optest.AssertErrorIs(t, tier, &predictOperator{}, value.Dict{} /* no static kwargs */, [][]value.Value{intable}, contextKwargTable, &interpreter.RequiredKwargNotProvidedError{ParamName: "model", OpModule: "model", OpName: "predict"})
}

func TestPredictErrorNoModelStore(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	intable := []value.Value{
		value.NewList(value.String("1:1 9:1 19:1 21:1 24:1 34:1 36:1 39:1 42:1 53:1 56:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 117:1 122:1")),
	}
	staticKwargsTable := value.NewDict(map[string]value.Value{
		"model": value.String("no model"),
	})
	contextKwargTable := []value.Dict{
		value.NewDict(map[string]value.Value{}),
	}

	optest.AssertErrorContains(t, tier, &predictOperator{}, staticKwargsTable, [][]value.Value{intable}, contextKwargTable, "could not get model from db")
}
