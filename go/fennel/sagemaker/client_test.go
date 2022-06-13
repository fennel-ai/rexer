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
	t.Skip("Skipping test, till smclient-test-endpoint is fixed")
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
	t.Skip("Skipping test, till smclient-test-endpoint is fixed")
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
	t.Skip("Skipping test, till smclient-test-endpoint is fixed")
	c, err := getTestClient()
	assert.NoError(t, err)
	featureVectors := []value.List{
		value.NewList(value.String("1:1 9:1 19:1 21:1 24:1 34:1 36:1 39:1 42:1 53:1 56:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 117:1 122:1")),
		value.NewList(value.String("3:1 9:1 19:1 21:1 30:1 34:1 36:1 40:1 41:1 53:1 58:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 118:1 124:1")),
		value.NewList(value.String("1:1 9:1 20:1 21:1 24:1 34:1 36:1 39:1 41:1 53:1 56:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 117:1 122:1")),
		value.NewList(value.String("3:1 9:1 19:1 21:1 24:1 34:1 36:1 39:1 51:1 53:1 56:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 116:1 122:1")),
		value.NewList(value.String("4:1 7:1 11:1 22:1 29:1 34:1 36:1 40:1 41:1 53:1 58:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 105:1 119:1 124:1")),
		value.NewList(value.String("3:1 10:1 20:1 21:1 23:1 34:1 37:1 40:1 42:1 54:1 55:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 118:1 126:1")),
		value.NewList(value.String("3:1 9:1 11:1 21:1 30:1 34:1 36:1 40:1 51:1 53:1 58:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 117:1 124:1")),
		value.NewList(value.String("1:1 9:1 20:1 21:1 23:1 34:1 36:1 39:1 42:1 53:1 56:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 117:1 120:1")),
		value.NewList(value.String("3:1 9:1 19:1 21:1 30:1 34:1 36:1 40:1 48:1 53:1 58:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 118:1 120:1")),
		value.NewList(value.String("4:1 9:1 20:1 21:1 24:1 34:1 36:1 39:1 51:1 53:1 60:1 65:1 67:1 77:1 86:1 88:1 92:1 95:1 102:1 105:1 117:1 123:1")),
		value.NewList(value.String("3:1 9:1 11:1 21:1 30:1 34:1 36:1 40:1 41:1 53:1 58:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 118:1 124:1")),
		value.NewList(value.String("1:1 9:1 20:1 21:1 23:1 34:1 36:1 39:1 51:1 53:1 56:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 105:1 117:1 122:1")),
		value.NewList(value.String("4:1 7:1 14:1 22:1 29:1 34:1 37:1 39:1 42:1 54:1 58:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 98:1 106:1 114:1 120:1")),
		value.NewList(value.String("1:1 9:1 19:1 21:1 24:1 34:1 36:1 39:1 42:1 53:1 56:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 117:1 120:1")),
		value.NewList(value.String("4:1 10:1 11:1 22:1 29:1 34:1 37:1 39:1 41:1 54:1 58:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 98:1 105:1 114:1 120:1")),
		value.NewList(value.String("4:1 9:1 20:1 21:1 23:1 34:1 36:1 39:1 51:1 53:1 60:1 65:1 67:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 117:1 120:1")),
		value.NewList(value.String("1:1 10:1 20:1 21:1 23:1 34:1 36:1 39:1 41:1 53:1 56:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 105:1 117:1 120:1")),
		value.NewList(value.String("3:1 9:1 11:1 21:1 30:1 34:1 36:1 40:1 51:1 53:1 58:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 118:1 124:1")),
		value.NewList(value.String("6:1 7:1 11:1 22:1 29:1 34:1 36:1 40:1 42:1 53:1 58:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 106:1 118:1 124:1")),
		value.NewList(value.String("3:1 10:1 20:1 21:1 23:1 34:1 36:1 39:1 42:1 53:1 56:1 65:1 69:1 77:1 86:1 88:1 92:1 95:1 102:1 105:1 116:1 120:1")),
	}
	response, err := c.Score(context.Background(), &lib.ScoreRequest{
		Framework:     "xgboost",
		EndpointName:  "smclient-test-endpoint",
		ContainerName: lib.GetContainerName("smclient-test-xgboost-model", "v1"),
		FeatureLists:  featureVectors,
	})
	assert.NoError(t, err)
	assert.Equal(t, len(featureVectors), len(response.Scores))
}

func TestScoreCsv(t *testing.T) {
	t.Skip("Skipping test, till smclient-test-endpoint is fixed")
	c, err := getTestClient()
	assert.NoError(t, err)
	csv, err := value.FromJSON([]byte("[0,0,0,0,0,0,0,1,0,1,0,1,0,0,1,0,0,0,1,0,0,0,0,0,0,0,0,0,1,0,1,0,0,1,0,0,0,0,0,0,0,0,1,1,0,1,0,0,1,0,0,0,0,0,0,0,0,0,0,1,0,0,1,0,0,0,0,0,0,1,0,0,0,1,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,1,0,1,0,0,1,0,0,1,0,0,0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,1,0,0,0,0]"))
	assert.NoError(t, err)
	featureVectors := []value.List{
		csv.(value.List),
	}
	response, err := c.Score(context.Background(), &lib.ScoreRequest{
		Framework:     "xgboost",
		EndpointName:  "smclient-test-endpoint",
		ContainerName: lib.GetContainerName("smclient-test-xgboost-model", "v1"),
		FeatureLists:  featureVectors,
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
	err = c.EnableAutoscaling(ctx, endpoint, variantName)
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
	}, logger)
}
