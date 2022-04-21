//go:build sagemaker

package modelstore

import (
	"context"
	"log"
	"testing"
	"time"

	lib "fennel/lib/sagemaker"
	db "fennel/model/sagemaker"
	"fennel/modelstore"
	"fennel/sagemaker"
	"fennel/test"
	"github.com/stretchr/testify/assert"
)

/* TODO - fix test (doesn't work due to 10 min timeout)
func TestStoreScoreRemoveModel(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	err = testsagemaker.AddSagemakerDataAndClientToTier(&tier)
	assert.NoError(t, err)

	data, err := tier.S3Client.DownloadFromRoot("model.tar.gz", "my-xgboost-test-bucket-2")
	assert.NoError(t, err)
	req := lib.ModelUploadRequest{
		Name:             "some-model",
		Version:          "v1",
		Framework:        "xgboost",
		FrameworkVersion: "1.3.1",
		ModelFile:        bytes.NewReader(data),
	}

	var retry bool
	for {
		err, retry = Store(context.Background(), tier, req)
		log.Print(err, retry)
		if !retry {
			break
		}
		log.Print("Waiting two minutes before retrying to store")
		time.Sleep(2 * time.Minute)
	}
	assert.NoError(t, err)

	csv, err := value.FromJSON([]byte("[0,0,0,0,0,0,0,1,0,1,0,1,0,0,1,0,0,0,1,0,0,0,0,0,0,0,0,0,1,0,1,0,0,1,0,0,0,0,0,0,0,0,1,1,0,1,0,0,1,0,0,0,0,0,0,0,0,0,0,1,0,0,1,0,0,0,0,0,0,1,0,0,0,1,0,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,1,0,1,0,0,1,0,0,1,0,0,0,0,0,1,0,1,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,1,0,0,0,0]"))
	assert.NoError(t, err)
	featureVecs := []value.List{csv.(value.List)}
	var scores []value.Value
	for {
		scores, err, retry = Score(context.Background(), tier, "some-model", "v1", featureVecs)
		log.Print(err, retry)
		if !retry {
			break
		}
		log.Print("Waiting two minutes before retrying to score")
		time.Sleep(2 * time.Minute)
	}
	assert.Equal(t, len(featureVecs), len(scores))

	for {
		err, retry = Remove(context.Background(), tier, req.Name, req.Version)
		log.Print(err, retry)
		if !retry {
			break
		}
		log.Print("Waiting two minutes before retrying to remove")
		time.Sleep(2 * time.Minute)
	}
	assert.NoError(t, err)
}
*/

func TestEnsureEndpoint(t *testing.T) {
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
		FrameworkVersion: "1.3.1",
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
