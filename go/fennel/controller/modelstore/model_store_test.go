//go:build sagemaker

package modelstore

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"testing"
	"time"

	lib "fennel/lib/sagemaker"
	"fennel/lib/value"
	db "fennel/model/sagemaker"
	"fennel/modelstore"
	"fennel/s3"
	"fennel/sagemaker"
	"fennel/test"
	"fennel/tier"

	"github.com/stretchr/testify/assert"
)

func TestStoreScoreRemoveModel(t *testing.T) {
	if os.Getenv("long") == "" {
		t.Skip("Skipping long test")
	}

	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	err = test.AddSagemakerClientToTier(&tier)
	assert.NoError(t, err)
	defer cleanup(t, tier)

	data, err := tier.S3Client.Download("model.tar.gz", "my-xgboost-test-bucket-2")
	assert.NoError(t, err)
	req := lib.ModelUploadRequest{
		Name:             "name",
		Version:          "v1",
		Framework:        "xgboost",
		FrameworkVersion: "1.3-1",
		ModelFile:        bytes.NewReader(data),
	}

	for {
		err = Store(context.Background(), tier, req)
		var retry RetryError
		if errors.As(err, &retry) {
			break
		}
		log.Print("Waiting one minute before retrying to store")
		time.Sleep(time.Minute)
	}
	assert.NoError(t, err)

	csv, err := value.FromJSON([]byte("[0,0,0,0,0,0,0,1,0,1,0,1,0,0,1,0,0,0,1,0,0,0,0,0,0,0,0,0,1,0,1,0,0,1,0,0,0,0,0,0,0,0,1,1,0,1,0,0,1,0,0,0,0,0,0,0,0,0,0,1,0,0,1,0,0,0,0,0,0,1,0,0,0,1,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,1,0,1,0,0,1,0,0,1,0,0,0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,1,0,0,0,0]"))
	assert.NoError(t, err)
	featureVecs := []value.List{csv.(value.List)}
	var scores []value.Value
	for {
		scores, err = Score(context.Background(), tier, "name", "v1", featureVecs)
		var retry RetryError
		if errors.As(err, &retry) {
			break
		}
		log.Print("Waiting one minute before retrying to score")
		time.Sleep(time.Minute)
	}
	assert.NoError(t, err)
	assert.Equal(t, len(featureVecs), len(scores))

	for {
		err = Remove(context.Background(), tier, req.Name, req.Version)
		var retry RetryError
		if errors.As(err, &retry) {
			break
		}
		log.Print("Waiting one minute before retrying to remove")
		time.Sleep(time.Minute)
	}
	assert.NoError(t, err)
}

func TestPretrainedModelEndPoint(t *testing.T) {
	if os.Getenv("long") == "" {
		t.Skip("Skipping long test")
	}
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	os.Setenv("AWS_PROFILE", "admin")
	os.Setenv("AWS_SDK_LOAD_CONFIG", "1")
	c, err := sagemaker.NewClient(sagemaker.SagemakerArgs{
		Region:                 "us-west-2",
		SagemakerExecutionRole: "arn:aws:iam::030813887342:role/service-role/AmazonSageMaker-ExecutionRole-20220315T123828",
	})
	assert.NoError(t, err)
	tier.SagemakerClient = c
	tier.S3Client = s3.NewClient(s3.S3Args{Region: "us-west-2"})
	model := "sbert"
	defer cleanupPreTrainedModelTest(t, tier, model)

	err = EnableModel(context.Background(), tier, model)
	assert.NoError(t, err)

	// It takes a couple of minutes for the model to be ready
	time.Sleep(3 * time.Minute)

	endpointName := PreTrainedModelId(model, tier.ID)

	// assert that resources are created in sagemaker.
	exists, err := tier.SagemakerClient.EndpointExists(context.Background(), endpointName)
	assert.NoError(t, err)
	assert.True(t, exists)

	featureInput := value.NewList(value.String("Recommendation systems is the way to go"))

	req := lib.ScoreRequest{
		Framework:    "huggingface",
		EndpointName: endpointName,
		FeatureLists: []value.List{featureInput, featureInput, featureInput},
	}
	response, err := tier.SagemakerClient.Score(context.Background(), &req)

	assert.NoError(t, err)
	assert.Equal(t, 3, len(response.Scores))
}

func cleanupPreTrainedModelTest(t *testing.T, tier tier.Tier, model string) {
	pretrainedId := PreTrainedModelId(model, tier.ID)
	err := tier.SagemakerClient.DeleteModel(context.Background(), pretrainedId)
	assert.NoError(t, err)
	err = tier.SagemakerClient.DeleteEndpointConfig(context.Background(), pretrainedId)
	assert.NoError(t, err)
	err = tier.SagemakerClient.DeleteEndpoint(context.Background(), pretrainedId)
	assert.NoError(t, err)
}

func TestEnsureEndpoint(t *testing.T) {
	if os.Getenv("long") == "" {
		t.Skip("Skipping long test")
	}
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	c, err := sagemaker.NewClient(sagemaker.SagemakerArgs{
		Region:                 "ap-south-1",
		SagemakerExecutionRole: "arn:aws:iam::030813887342:role/service-role/AmazonSageMaker-ExecutionRole-20220315T123828",
	})
	assert.NoError(t, err)
	tier.SagemakerClient = c

	model := lib.Model{
		Name:             "my-test-model",
		Version:          "v1",
		Framework:        "xgboost",
		FrameworkVersion: "1.3-1",
		ArtifactPath:     "s3://my-xgboost-test-bucket-2/model.tar.gz",
	}
	endpointName := "unit-test-endpoint"
	tier.ModelStore = modelstore.NewModelStore(modelstore.ModelStoreArgs{
		ModelStoreS3Bucket:     "my-xgboost-test-bucket-2",
		ModelStoreEndpointName: endpointName,
	}, tier.ID)

	// Insert an active model into db.
	modelId, err := db.InsertModel(tier, model)
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), modelId)

	// Ensure model is served on sagemaker.
	err = EnsureEndpointExists(context.Background(), tier)
	assert.NoError(t, err)

	// assert that registry resources are created in db.
	sagemakerModels, err := db.GetCoveringHostedModels(tier)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(sagemakerModels))
	endpointCfg, err := db.GetEndpointConfigWithModel(tier, sagemakerModels[0])
	assert.NoError(t, err)
	assert.NotEqual(t, "", endpointCfg.Name)
	endpoints, err := db.GetEndpointsWithCfg(tier, endpointCfg.Name)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(endpoints))
	assert.Equal(t, endpointName, endpoints[0].Name)

	// assert that resources are created in sagemaker.
	exists, err := tier.SagemakerClient.EndpointExists(context.Background(), endpointName)
	assert.NoError(t, err)
	assert.True(t, exists)
	defer func() {
		err := tier.SagemakerClient.DeleteEndpoint(context.Background(), endpointName)
		assert.NoError(t, err)
	}()

	exists, err = tier.SagemakerClient.EndpointConfigExists(context.Background(), endpointCfg.Name)
	assert.NoError(t, err)
	assert.True(t, exists)
	defer func() {
		err := tier.SagemakerClient.DeleteEndpointConfig(context.Background(), endpointCfg.Name)
		assert.NoError(t, err)
	}()

	exists, err = tier.SagemakerClient.ModelExists(context.Background(), sagemakerModels[0])
	assert.NoError(t, err)
	assert.True(t, exists)
	defer func() {
		// wait for endpoint to be in-service before it can be deleted.
		var status string
		for status != "InService" {
			log.Printf("Waiting for endpoint [%s] to be in service...", endpointName)
			time.Sleep(time.Second * 10)
			status, err = tier.SagemakerClient.GetEndpointStatus(context.Background(), endpointName)
			assert.NoError(t, err)
		}
		err := tier.SagemakerClient.DeleteModel(context.Background(), sagemakerModels[0])
		assert.NoError(t, err)
	}()
}

func cleanup(t *testing.T, tier tier.Tier) {
	hostedModels, err := db.GetAllHostedModels(tier)
	assert.NoError(t, err)
	for _, m := range hostedModels {
		endpointCfg, err := db.GetEndpointConfigWithModel(tier, m.SagemakerModelName)
		assert.NoError(t, err)
		err = tier.SagemakerClient.DeleteModel(context.Background(), m.SagemakerModelName)
		assert.NoError(t, err)
		err = tier.SagemakerClient.DeleteEndpointConfig(context.Background(), endpointCfg.Name)
		assert.NoError(t, err)
	}
}
