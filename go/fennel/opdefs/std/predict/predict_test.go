//go:build sagemaker

// TODO: remove the sagemaker build tag once we have enabled AWS Sagemaker access from CI.
package predict

import (
	"testing"

	"fennel/lib/value"
	"fennel/test"
	"fennel/test/optest"
	"fennel/test/sagemaker"

	"github.com/stretchr/testify/assert"
)

func TestPredict(t *testing.T) {
	intable := []value.Value{
		value.NewDict(map[string]value.Value{"foo": value.Nil}),
		value.NewDict(map[string]value.Value{"bar": value.Nil}),
	}
	contextKwargTable := []value.Dict{
		value.NewDict(map[string]value.Value{
			"features": value.NewList(value.String("1:1 9:1 19:1 21:1 24:1 34:1 36:1 39:1 42:1 53:1 56:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 117:1 122:1")),
		}),
		value.NewDict(map[string]value.Value{
			"features": value.NewList(value.String("3:1 9:1 19:1 21:1 30:1 34:1 36:1 40:1 48:1 53:1 58:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 118:1 120:1")),
		}),
	}
	expected := []value.Value{
		value.Double(0.28583016991615295),
		value.Double(0.923923909664154),
	}

	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	err = sagemaker.AddSagemakerClientToTier(&tier)
	assert.NoError(t, err)
	optest.AssertEqual(t, tier, &predictOperator{}, value.NewDict(map[string]value.Value{
		"model_name":    value.String("integration-test-xgboost-model"),
		"model_version": value.String("v1"),
	}), [][]value.Value{intable}, contextKwargTable, expected)
}

func TestPredictError(t *testing.T) {
	intable := []value.Value{
		value.Nil,
	}
	contextKwargTable := []value.Dict{
		value.NewDict(map[string]value.Value{
			"features": value.String("not a feature list"),
		}),
	}

	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	err = sagemaker.AddSagemakerClientToTier(&tier)
	assert.NoError(t, err)
	optest.AssertError(t, tier, &predictOperator{}, value.NewDict(map[string]value.Value{
		"model_name":    value.String("integration-test-xgboost-model"),
		"model_version": value.String("v1"),
	}), [][]value.Value{intable}, contextKwargTable)
}

func TestPredictErrorNoModel(t *testing.T) {
	intable := []value.Value{
		value.Nil,
	}
	contextKwargTable := []value.Dict{
		value.NewDict(map[string]value.Value{
			"features": value.NewList(value.String("1:1 9:1 19:1 21:1 24:1 34:1 36:1 39:1 42:1 53:1 56:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 117:1 122:1")),
		}),
	}

	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	err = sagemaker.AddSagemakerClientToTier(&tier)
	assert.NoError(t, err)
	optest.AssertError(t, tier, &predictOperator{}, value.Dict{} /* no static kwargs */, [][]value.Value{intable}, contextKwargTable)
}
