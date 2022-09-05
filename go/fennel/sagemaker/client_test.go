//go:build sagemaker

package sagemaker

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	lib "fennel/lib/sagemaker"
	"fennel/lib/value"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestCreateDeleteExists(t *testing.T) {
	c, err := getTestClient()
	assert.NoError(t, err)

	sagemakerModelName := "integration-test-model"

	exists, err := c.ModelExists(context.Background(), "my-test-model")
	assert.NoError(t, err)
	assert.False(t, exists)

	err = c.CreateModel(context.Background(), []lib.Model{
		{
			Name:             "my-test-model",
			Version:          "v1",
			Framework:        "xgboost",
			FrameworkVersion: "1.3-1",
			ArtifactPath:     "s3://my-xgboost-test-bucket-2/model.tar.gz",
			ContainerName:    "Container-my-test-model-v1",
		},
	}, sagemakerModelName)
	assert.NoError(t, err)

	exists, err = c.ModelExists(context.Background(), sagemakerModelName)
	assert.NoError(t, err)
	assert.True(t, exists)

	err = c.DeleteModel(context.Background(), sagemakerModelName)
	assert.NoError(t, err)

	exists, err = c.ModelExists(context.Background(), sagemakerModelName)
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestEndpointConfigExists(t *testing.T) {
	c, err := getTestClient()
	assert.NoError(t, err)

	configName := "my-endpoint-config"
	exists, err := c.EndpointConfigExists(context.Background(), configName)
	assert.NoError(t, err)
	assert.False(t, exists)

	sagemakerModelName := "smclient-test-model"

	// Create endpoint configuration.
	endpointCfg := lib.SagemakerEndpointConfig{
		Name:          configName,
		ModelName:     sagemakerModelName,
		VariantName:   sagemakerModelName,
		InstanceType:  "ml.t2.medium",
		InstanceCount: 1,
	}

	err = c.CreateEndpointConfig(context.Background(), endpointCfg)
	assert.NoError(t, err)
	exists, err = c.EndpointConfigExists(context.Background(), configName)
	assert.NoError(t, err)
	assert.True(t, exists)

	err = c.DeleteEndpointConfig(context.Background(), configName)
	assert.NoError(t, err)
	exists, err = c.EndpointConfigExists(context.Background(), configName)
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestEndpointExists(t *testing.T) {
	c, err := getTestClient()
	assert.NoError(t, err)

	endpointName := "my-non-existing-endpoint"
	exists, err := c.EndpointExists(context.Background(), endpointName)
	assert.NoError(t, err)
	if exists {
		status, err := c.GetEndpointStatus(context.Background(), endpointName)
		assert.NoError(t, err)
		if status == "Creating" {
			log.Printf("endpoint can't be deleted while it is in Creating state")
			return
		}
		err = c.DeleteEndpoint(context.Background(), endpointName)
		assert.NoError(t, err)
		exists, err = c.EndpointExists(context.Background(), endpointName)
		assert.NoError(t, err)
		assert.False(t, exists)
	} else {
		err = c.CreateEndpoint(context.Background(), lib.SagemakerEndpoint{
			Name:               endpointName,
			EndpointConfigName: "smclient-test-endpoint-config",
		})
		assert.NoError(t, err)
		exists, err = c.EndpointExists(context.Background(), endpointName)
		assert.NoError(t, err)
		assert.True(t, exists)
	}
}

func TestGetEndpointConfigName(t *testing.T) {
	c, err := getTestClient()
	assert.NoError(t, err)

	name, err := c.GetEndpointConfigName(context.Background(), "smclient-test-endpoint")
	assert.NoError(t, err)
	assert.Equal(t, "smclient-test-endpoint-config", name)
}

func TestUpdateEndpoint(t *testing.T) {
	c, err := getTestClient()
	assert.NoError(t, err)

	endpointName := "smclient-test-endpoint"
	endpointCfgName := "smclient-test-endpoint-config"
	// Updating endpoint with the same endpoint configuration should fail.
	err = c.UpdateEndpoint(context.Background(), lib.SagemakerEndpoint{
		Name:               endpointName,
		EndpointConfigName: endpointCfgName,
	})
	assert.Error(t, err)
}

func TestScoreSvm(t *testing.T) {
	c, err := getTestClient()
	assert.NoError(t, err)
	featureVectors := []value.Value{
		value.NewDict(map[string]value.Value{"1": value.Int(1), "9": value.Int(1), "19": value.Int(1), "21": value.Int(1), "24": value.Int(1), "34": value.Int(1), "36": value.Int(1), "39": value.Int(1), "42": value.Int(1), "53": value.Int(1), "56": value.Int(1), "65": value.Int(1), "69": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "102": value.Int(1), "106": value.Int(1), "117": value.Int(1), "122": value.Int(1)}),
		value.NewDict(map[string]value.Value{"3": value.Int(1), "9": value.Int(1), "19": value.Int(1), "21": value.Int(1), "30": value.Int(1), "34": value.Int(1), "36": value.Int(1), "40": value.Int(1), "41": value.Int(1), "53": value.Int(1), "58": value.Int(1), "65": value.Int(1), "69": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "102": value.Int(1), "106": value.Int(1), "118": value.Int(1), "124": value.Int(1)}),
		value.NewDict(map[string]value.Value{"1": value.Int(1), "9": value.Int(1), "20": value.Int(1), "21": value.Int(1), "24": value.Int(1), "34": value.Int(1), "36": value.Int(1), "39": value.Int(1), "41": value.Int(1), "53": value.Int(1), "56": value.Int(1), "65": value.Int(1), "69": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "102": value.Int(1), "106": value.Int(1), "117": value.Int(1), "122": value.Int(1)}),
		value.NewDict(map[string]value.Value{"3": value.Int(1), "9": value.Int(1), "19": value.Int(1), "21": value.Int(1), "24": value.Int(1), "34": value.Int(1), "36": value.Int(1), "39": value.Int(1), "51": value.Int(1), "53": value.Int(1), "56": value.Int(1), "65": value.Int(1), "69": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "102": value.Int(1), "106": value.Int(1), "116": value.Int(1), "122": value.Int(1)}),
		value.NewDict(map[string]value.Value{"4": value.Int(1), "7": value.Int(1), "11": value.Int(1), "22": value.Int(1), "29": value.Int(1), "34": value.Int(1), "36": value.Int(1), "40": value.Int(1), "41": value.Int(1), "53": value.Int(1), "58": value.Int(1), "65": value.Int(1), "69": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "102": value.Int(1), "105": value.Int(1), "119": value.Int(1), "124": value.Int(1)}),
		value.NewDict(map[string]value.Value{"3": value.Int(1), "10": value.Int(1), "20": value.Int(1), "21": value.Int(1), "23": value.Int(1), "34": value.Int(1), "37": value.Int(1), "40": value.Int(1), "42": value.Int(1), "54": value.Int(1), "55": value.Int(1), "65": value.Int(1), "69": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "102": value.Int(1), "106": value.Int(1), "118": value.Int(1), "126": value.Int(1)}),
		value.NewDict(map[string]value.Value{"3": value.Int(1), "9": value.Int(1), "11": value.Int(1), "21": value.Int(1), "30": value.Int(1), "34": value.Int(1), "36": value.Int(1), "40": value.Int(1), "51": value.Int(1), "53": value.Int(1), "58": value.Int(1), "65": value.Int(1), "69": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "102": value.Int(1), "106": value.Int(1), "117": value.Int(1), "124": value.Int(1)}),
		value.NewDict(map[string]value.Value{"1": value.Int(1), "9": value.Int(1), "20": value.Int(1), "21": value.Int(1), "23": value.Int(1), "34": value.Int(1), "36": value.Int(1), "39": value.Int(1), "42": value.Int(1), "53": value.Int(1), "56": value.Int(1), "65": value.Int(1), "69": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "102": value.Int(1), "106": value.Int(1), "117": value.Int(1), "120": value.Int(1)}),
		value.NewDict(map[string]value.Value{"3": value.Int(1), "9": value.Int(1), "19": value.Int(1), "21": value.Int(1), "30": value.Int(1), "34": value.Int(1), "36": value.Int(1), "40": value.Int(1), "48": value.Int(1), "53": value.Int(1), "58": value.Int(1), "65": value.Int(1), "69": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "102": value.Int(1), "106": value.Int(1), "118": value.Int(1), "120": value.Int(1)}),
		value.NewDict(map[string]value.Value{"4": value.Int(1), "9": value.Int(1), "20": value.Int(1), "21": value.Int(1), "24": value.Int(1), "34": value.Int(1), "36": value.Int(1), "39": value.Int(1), "51": value.Int(1), "53": value.Int(1), "60": value.Int(1), "65": value.Int(1), "67": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "102": value.Int(1), "105": value.Int(1), "117": value.Int(1), "123": value.Int(1)}),
		value.NewDict(map[string]value.Value{"3": value.Int(1), "9": value.Int(1), "11": value.Int(1), "21": value.Int(1), "30": value.Int(1), "34": value.Int(1), "36": value.Int(1), "40": value.Int(1), "41": value.Int(1), "53": value.Int(1), "58": value.Int(1), "65": value.Int(1), "69": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "102": value.Int(1), "106": value.Int(1), "118": value.Int(1), "124": value.Int(1)}),
		value.NewDict(map[string]value.Value{"1": value.Int(1), "9": value.Int(1), "20": value.Int(1), "21": value.Int(1), "23": value.Int(1), "34": value.Int(1), "36": value.Int(1), "39": value.Int(1), "51": value.Int(1), "53": value.Int(1), "56": value.Int(1), "65": value.Int(1), "69": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "102": value.Int(1), "105": value.Int(1), "117": value.Int(1), "122": value.Int(1)}),
		value.NewDict(map[string]value.Value{"4": value.Int(1), "7": value.Int(1), "14": value.Int(1), "22": value.Int(1), "29": value.Int(1), "34": value.Int(1), "37": value.Int(1), "39": value.Int(1), "42": value.Int(1), "54": value.Int(1), "58": value.Int(1), "65": value.Int(1), "69": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "98": value.Int(1), "106": value.Int(1), "114": value.Int(1), "120": value.Int(1)}),
		value.NewDict(map[string]value.Value{"1": value.Int(1), "9": value.Int(1), "19": value.Int(1), "21": value.Int(1), "24": value.Int(1), "34": value.Int(1), "36": value.Int(1), "39": value.Int(1), "42": value.Int(1), "53": value.Int(1), "56": value.Int(1), "65": value.Int(1), "69": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "102": value.Int(1), "106": value.Int(1), "117": value.Int(1), "120": value.Int(1)}),
		value.NewDict(map[string]value.Value{"4": value.Int(1), "10": value.Int(1), "11": value.Int(1), "22": value.Int(1), "29": value.Int(1), "34": value.Int(1), "37": value.Int(1), "39": value.Int(1), "41": value.Int(1), "54": value.Int(1), "58": value.Int(1), "65": value.Int(1), "69": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "98": value.Int(1), "105": value.Int(1), "114": value.Int(1), "120": value.Int(1)}),
		value.NewDict(map[string]value.Value{"4": value.Int(1), "9": value.Int(1), "20": value.Int(1), "21": value.Int(1), "23": value.Int(1), "34": value.Int(1), "36": value.Int(1), "39": value.Int(1), "51": value.Int(1), "53": value.Int(1), "60": value.Int(1), "65": value.Int(1), "67": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "102": value.Int(1), "106": value.Int(1), "117": value.Int(1), "120": value.Int(1)}),
		value.NewDict(map[string]value.Value{"1": value.Int(1), "10": value.Int(1), "20": value.Int(1), "21": value.Int(1), "23": value.Int(1), "34": value.Int(1), "36": value.Int(1), "39": value.Int(1), "41": value.Int(1), "53": value.Int(1), "56": value.Int(1), "65": value.Int(1), "69": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "102": value.Int(1), "105": value.Int(1), "117": value.Int(1), "120": value.Int(1)}),
		value.NewDict(map[string]value.Value{"3": value.Int(1), "9": value.Int(1), "11": value.Int(1), "21": value.Int(1), "30": value.Int(1), "34": value.Int(1), "36": value.Int(1), "40": value.Int(1), "51": value.Int(1), "53": value.Int(1), "58": value.Int(1), "65": value.Int(1), "69": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "102": value.Int(1), "106": value.Int(1), "118": value.Int(1), "124": value.Int(1)}),
		value.NewDict(map[string]value.Value{"6": value.Int(1), "7": value.Int(1), "11": value.Int(1), "22": value.Int(1), "29": value.Int(1), "34": value.Int(1), "36": value.Int(1), "40": value.Int(1), "42": value.Int(1), "53": value.Int(1), "58": value.Int(1), "65": value.Int(1), "69": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "102": value.Int(1), "106": value.Int(1), "118": value.Int(1), "124": value.Int(1)}),
		value.NewDict(map[string]value.Value{"3": value.Int(1), "10": value.Int(1), "20": value.Int(1), "21": value.Int(1), "23": value.Int(1), "34": value.Int(1), "36": value.Int(1), "39": value.Int(1), "42": value.Int(1), "53": value.Int(1), "56": value.Int(1), "65": value.Int(1), "69": value.Int(1), "77": value.Int(1), "86": value.Int(1), "88": value.Int(1), "92": value.Int(1), "95": value.Int(1), "102": value.Int(1), "105": value.Int(1), "116": value.Int(1), "120": value.Int(1)}),
	}
	response, err := c.Score(context.Background(), &lib.ScoreRequest{
		Framework:     "xgboost",
		EndpointName:  "smclient-test-endpoint",
		ContainerName: "Container-smclient-model-v1",
		ModelInput:    value.NewList(featureVectors...),
	})
	assert.NoError(t, err)
	assert.Equal(t, len(featureVectors), len(response.Scores))
}

func TestScoreCsv(t *testing.T) {
	c, err := getTestClient()
	assert.NoError(t, err)
	csv, err := value.FromJSON([]byte("[0,0,0,0,0,0,0,1,0,1,0,1,0,0,1,0,0,0,1,0,0,0,0,0,0,0,0,0,1,0,1,0,0,1,0,0,0,0,0,0,0,0,1,1,0,1,0,0,1,0,0,0,0,0,0,0,0,0,0,1,0,0,1,0,0,0,0,0,0,1,0,0,0,1,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,1,0,1,0,0,1,0,0,1,0,0,0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,1,0,0,0,0]"))
	assert.NoError(t, err)
	featureVectors := []value.Value{
		csv.(value.List),
	}
	response, err := c.Score(context.Background(), &lib.ScoreRequest{
		Framework:     "xgboost",
		EndpointName:  "smclient-test-endpoint",
		ContainerName: "Container-smclient-model-v1",
		ModelInput:    value.NewList(featureVectors...),
	})
	assert.NoError(t, err)
	assert.Equal(t, len(featureVectors), len(response.Scores))
}

func TestGetProductionVariantName(t *testing.T) {
	c, err := getTestClient()
	assert.NoError(t, err)

	endpoint := "autoscaling-unittest-endpoint"
	// model - autoscaling-unittest-model
	expected := "variant-name-1"
	ctx := context.Background()

	actual, err := c.GetProductionVariantName(ctx, endpoint)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestIsAutoscalingConfigured(t *testing.T) {
	c, err := getTestClient()
	assert.NoError(t, err)

	endpoint := "autoscaling-unittest-endpoint"
	// model - autoscaling-unittest-model
	variantName := "variant-name-1"
	ctx := context.Background()

	// autoscaling by default is not configured
	yes, err := c.IsAutoscalingConfigured(ctx, endpoint, variantName)
	assert.NoError(t, err)
	assert.False(t, yes)

	// configure autoscaling on this instance and assert
	err = c.EnableAutoscaling(ctx, endpoint, variantName, lib.ScalingConfiguration{
		Cpu: lib.CpuScalingPolicy{
			CpuTargetValue:         20,
			ScaleInCoolDownPeriod:  100,
			ScaleOutCoolDownPeriod: 200,
		},
		BaseConfig: &lib.BaseConfig{MinCapacity: 1, MaxCapacity: 2},
	})
	assert.NoError(t, err)
	yes, err = c.IsAutoscalingConfigured(ctx, endpoint, variantName)
	assert.NoError(t, err)
	assert.True(t, yes)

	// now disable autoscaling and verify again
	err = c.DisableAutoscaling(ctx, endpoint, variantName)
	assert.NoError(t, err)
	yes, err = c.IsAutoscalingConfigured(ctx, endpoint, variantName)
	assert.NoError(t, err)
	assert.False(t, yes)
}

func TestEnableAutoscalingMisConfigs(t *testing.T) {
	c, err := getTestClient()
	assert.NoError(t, err)

	endpoint := "autoscaling-unittest-endpoint"
	// model - autoscaling-unittest-model
	variantName := "variant-name-1"
	ctx := context.Background()

	{
		err := c.EnableAutoscaling(ctx, endpoint, variantName, lib.ScalingConfiguration{
			Cpu: lib.CpuScalingPolicy{
				CpuTargetValue:         -1,
				ScaleInCoolDownPeriod:  100,
				ScaleOutCoolDownPeriod: 200,
			},
			BaseConfig: &lib.BaseConfig{MinCapacity: 1, MaxCapacity: 2},
		})
		assert.Error(t, err)
	}
	{
		err := c.EnableAutoscaling(ctx, endpoint, variantName, lib.ScalingConfiguration{
			Cpu: lib.CpuScalingPolicy{
				CpuTargetValue:         20,
				ScaleInCoolDownPeriod:  100,
				ScaleOutCoolDownPeriod: 200,
			},
			BaseConfig: &lib.BaseConfig{MinCapacity: 0, MaxCapacity: 2},
		})
		assert.Error(t, err)
	}
	{
		err := c.EnableAutoscaling(ctx, endpoint, variantName, lib.ScalingConfiguration{
			Cpu: lib.CpuScalingPolicy{
				CpuTargetValue:         20,
				ScaleInCoolDownPeriod:  100,
				ScaleOutCoolDownPeriod: 200,
			},
			BaseConfig: &lib.BaseConfig{MinCapacity: 1, MaxCapacity: 0},
		})
		assert.Error(t, err)
	}
	{
		err := c.EnableAutoscaling(ctx, endpoint, variantName, lib.ScalingConfiguration{
			Cpu: lib.CpuScalingPolicy{
				CpuTargetValue:         20,
				ScaleInCoolDownPeriod:  100,
				ScaleOutCoolDownPeriod: 200,
			},
			BaseConfig: &lib.BaseConfig{MinCapacity: 2, MaxCapacity: 1},
		})
		assert.Error(t, err)
	}
	{
		err := c.EnableAutoscaling(ctx, endpoint, variantName, lib.ScalingConfiguration{
			Cpu: lib.CpuScalingPolicy{
				CpuTargetValue:         20,
				ScaleInCoolDownPeriod:  -1,
				ScaleOutCoolDownPeriod: -1,
			},
			BaseConfig: &lib.BaseConfig{MinCapacity: 2, MaxCapacity: 1},
		})
		assert.Error(t, err)
	}
}

func TestDisablingAutoscalingOnUnconfiguredVariant(t *testing.T) {
	c, err := getTestClient()
	assert.NoError(t, err)

	endpoint := "autoscaling-unittest-endpoint"
	// model - autoscaling-unittest-model
	variantName := "variant-name-1"
	ctx := context.Background()

	// check that autoscaling is not configured
	yes, err := c.IsAutoscalingConfigured(ctx, endpoint, variantName)
	assert.NoError(t, err)
	assert.False(t, yes)

	// disable on this, should not return error
	err = c.DisableAutoscaling(ctx, endpoint, variantName)
	assert.NoError(t, err)
}

func getTestClient() (SMClient, error) {
	// Set the environment variables to enable access the test sagemaker endpoint.
	os.Setenv("AWS_PROFILE", "admin")
	os.Setenv("AWS_SDK_LOAD_CONFIG", "1")

	logger, err := zap.NewDevelopment()
	if err != nil {
		return SMClient{}, fmt.Errorf("failed to construct logger: %v", err)
	}

	return NewClient(SagemakerArgs{
		Region:                 "ap-south-1",
		SagemakerExecutionRole: "arn:aws:iam::030813887342:role/service-role/AmazonSageMaker-ExecutionRole-20220315T123828",
		SagemakerInstanceType:  "ml.c5.large",
		SagemakerInstanceCount: 1,
	}, logger)
}
