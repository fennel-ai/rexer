package sagemaker

import (
	"testing"
	"time"

	lib "fennel/lib/sagemaker"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func TestInsertModel(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	id, err := InsertModel(tier, lib.Model{
		Name:             "test-model",
		Version:          "v1",
		Framework:        "xgboost",
		FrameworkVersion: "1.31.0",
		ArtifactPath:     "s3://fennel-test-bucket/test-model/model.tar.gz",
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), id)

	id, err = InsertModel(tier, lib.Model{
		Name:             "test-model",
		Version:          "v2",
		Framework:        "xgboost",
		FrameworkVersion: "another-version",
		ArtifactPath:     "another-path",
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(2), id)

	id, err = InsertModel(tier, lib.Model{
		Name:             "test-model-2",
		Version:          "v1",
		Framework:        "xgboost",
		FrameworkVersion: "1.31.0",
		ArtifactPath:     "s3://fennel-test-bucket/test-model/model.tar.gz",
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(3), id)
}

func TestGetActiveModels(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	curr := time.Now()
	id, err := InsertModel(tier, lib.Model{
		Name:             "test-model",
		Version:          "v1",
		Framework:        "xgboost",
		FrameworkVersion: "1.31.0",
		ArtifactPath:     "s3://fennel-test-bucket/test-model/model.tar.gz",
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), id)
	activeModels, err := GetActiveModels(tier)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(activeModels))
	assert.Equal(t, "test-model", activeModels[0].Name)
	assert.Equal(t, "v1", activeModels[0].Version)
	assert.GreaterOrEqual(t, activeModels[0].LastModified, curr.Unix())

	err = MakeModelInactive(tier, "test-model", "v1")
	assert.NoError(t, err)
	activeModels, err = GetActiveModels(tier)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(activeModels))
}

func TestInsertHostedModels(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	id, err := InsertModel(tier, lib.Model{
		Name:             "test-model",
		Version:          "v1",
		Framework:        "xgboost",
		FrameworkVersion: "1.31.0",
		ArtifactPath:     "s3://fennel-test-bucket/test-model/model.tar.gz",
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), id)

	err = InsertHostedModels(tier, lib.SagemakerHostedModel{
		SagemakerModelName: "aws-test-model",
		ModelId:            1,
		ContainerHostname:  "test-hostname",
	})
	assert.NoError(t, err)
}

func TestGetCoveringModels(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	id, err := InsertModel(tier, lib.Model{
		Name:             "test-model",
		Version:          "v1",
		Framework:        "xgboost",
		FrameworkVersion: "1.31.0",
		ArtifactPath:     "s3://fennel-test-bucket/test-model/model.tar.gz",
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), id)

	err = InsertHostedModels(tier, lib.SagemakerHostedModel{
		SagemakerModelName: "aws-test-model",
		ModelId:            1,
		ContainerHostname:  "test-hostname",
	})
	assert.NoError(t, err)

	hostedModels, err := GetCoveringHostedModels(tier)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(hostedModels))
	assert.Equal(t, "aws-test-model", hostedModels[0])

	id2, err := InsertModel(tier, lib.Model{
		Name:             "test-model2",
		Version:          "v1",
		Framework:        "xgboost",
		FrameworkVersion: "1.31.0",
		ArtifactPath:     "s3://fennel-test-bucket/test-model/model.tar.gz",
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(2), id2)

	hostedModels, err = GetCoveringHostedModels(tier)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(hostedModels))

	err = InsertHostedModels(tier, lib.SagemakerHostedModel{
		SagemakerModelName: "aws-test-model",
		ModelId:            2,
		ContainerHostname:  "test-hostname-2",
	})
	assert.NoError(t, err)
	hostedModels, err = GetCoveringHostedModels(tier)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(hostedModels))
	assert.Equal(t, "aws-test-model", hostedModels[0])
}

func TestInsertEndpointConfig(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	err := InsertEndpointConfig(tier, lib.SagemakerEndpointConfig{
		Name:          "test-endpoint-config",
		VariantName:   "test-variant",
		ModelName:     "test-model",
		InstanceType:  "ml.m4.xlarge",
		InstanceCount: 1,
	})
	assert.NoError(t, err)

	cfg, err := GetEndpointConfigWithModel(tier, "test-model")
	assert.NoError(t, err)
	assert.Equal(t, "test-endpoint-config", cfg.Name)
}

func TestInsertEndpoint(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	cfgName := "test-endpoint-config"
	endpoints, err := GetEndpointsWithCfg(tier, cfgName)
	assert.NoError(t, err)
	assert.Empty(t, endpoints)

	err = InsertEndpoint(tier, lib.SagemakerEndpoint{
		Name:               "test-endpoint",
		EndpointConfigName: cfgName,
	})
	assert.NoError(t, err)

	endpoints, err = GetEndpointsWithCfg(tier, cfgName)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(endpoints))
	assert.Equal(t, "test-endpoint", endpoints[0].Name)

	endpoint, err := GetEndpoint(tier, "test-endpoint")
	assert.NoError(t, err)
	assert.Equal(t, "test-endpoint", endpoint.Name)

	// Update endpoint to use new config.
	err = InsertEndpoint(tier, lib.SagemakerEndpoint{
		Name:               "test-endpoint",
		EndpointConfigName: cfgName + "v2",
	})
	assert.NoError(t, err)
	endpoints, err = GetEndpointsWithCfg(tier, cfgName)
	assert.NoError(t, err)
	assert.Empty(t, endpoints)

	err = MakeEndpointInactive(tier, "test-endpoint")
	assert.NoError(t, err)

	inactive, err := GetInactiveEndpoints(tier)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(inactive))
	assert.Equal(t, "test-endpoint", inactive[0])
}

func TestGetFramework(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)

	id, err := InsertModel(tier, lib.Model{
		Name:             "test-model",
		Version:          "v1",
		Framework:        "xgboost",
		FrameworkVersion: "1.31.0",
		ArtifactPath:     "s3://fennel-test-bucket/test-model/model.tar.gz",
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), id)

	framework, err := GetFramework(tier, "test-model", "v1")
	assert.NoError(t, err)
	assert.Equal(t, "xgboost", framework)
}
