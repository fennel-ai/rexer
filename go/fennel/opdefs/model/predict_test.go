//go:build sagemaker

// TODO: remove the sagemaker build tag once we have enabled AWS Sagemaker access from CI.
package model

import (
	"testing"

	lib "fennel/lib/sagemaker"
	"fennel/lib/value"
	"fennel/model/sagemaker"
	"fennel/test"
	"fennel/test/optest"

	"github.com/stretchr/testify/assert"
)

func TestPredict(t *testing.T) {
	t.Skip("Skipping test, till smclient-test-endpoint is fixed")
	intable := []value.Value{
		value.NewDict(map[string]value.Value{"foo": value.Nil}),
		value.NewDict(map[string]value.Value{"bar": value.Nil}),
	}
	contextKwargTable := []value.Dict{
		value.NewDict(map[string]value.Value{
			"input": value.NewList(value.String("1:1 9:1 19:1 21:1 24:1 34:1 36:1 39:1 42:1 53:1 56:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 117:1 122:1")),
		}),
		value.NewDict(map[string]value.Value{
			"input": value.NewList(value.String("3:1 9:1 19:1 21:1 30:1 34:1 36:1 40:1 48:1 53:1 58:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 118:1 120:1")),
		}),
	}
	expected := []value.Value{
		value.NewList(value.Double(0.28583016991615295)),
		value.NewList(value.Double(0.923923909664154)),
	}

	tier := test.Tier(t)
	defer test.Teardown(tier)
	err := test.AddSagemakerClientToTier(&tier)
	assert.NoError(t, err)
	_, err = sagemaker.InsertModel(tier, lib.Model{
		Name:             "smclient-test-xgboost-model",
		Version:          "v1",
		Framework:        "xgboost",
		FrameworkVersion: "1.3-1",
		ArtifactPath:     "s3://",
	})
	assert.NoError(t, err)
	tier.ModelStore.TestSetEndpointName("smclient-test-endpoint")
	optest.AssertEqual(t, tier, &predictOperator{}, value.NewDict(map[string]value.Value{
		"model_name":    value.String("smclient-test-xgboost-model"),
		"model_version": value.String("v1"),
	}), [][]value.Value{intable}, contextKwargTable, expected)
}

func TestPredictError(t *testing.T) {
	t.Skip("Skipping test, till smclient-test-endpoint is fixed")
	intable := []value.Value{
		value.Nil,
	}
	contextKwargTable := []value.Dict{
		value.NewDict(map[string]value.Value{
			"input": value.String("not a feature list"),
		}),
	}

	tier := test.Tier(t)
	defer test.Teardown(tier)
	err := test.AddSagemakerClientToTier(&tier)
	assert.NoError(t, err)
	tier.ModelStore.TestSetEndpointName("smclient-test-endpoint")
	optest.AssertError(t, tier, &predictOperator{}, value.NewDict(map[string]value.Value{
		"model_name":    value.String("smclient-test-xgboost-model"),
		"model_version": value.String("v1"),
	}), [][]value.Value{intable}, contextKwargTable)
}

func TestPredictErrorNoModel(t *testing.T) {
	intable := []value.Value{
		value.Nil,
	}
	contextKwargTable := []value.Dict{
		value.NewDict(map[string]value.Value{
			"input": value.NewList(value.String("1:1 9:1 19:1 21:1 24:1 34:1 36:1 39:1 42:1 53:1 56:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 117:1 122:1")),
		}),
	}

	tier := test.Tier(t)
	defer test.Teardown(tier)
	err := test.AddSagemakerClientToTier(&tier)
	assert.NoError(t, err)
	optest.AssertError(t, tier, &predictOperator{}, value.Dict{} /* no static kwargs */, [][]value.Value{intable}, contextKwargTable)
}
