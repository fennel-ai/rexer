package modelstore

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	lib "fennel/lib/sagemaker"
	db "fennel/model/sagemaker"
	"fennel/tier"
)

type ModelStore struct {
	s3Bucket     string
	storeMutex   sync.Mutex
	endpointName string
}

// NewModelStore creates a new ModelStore. There should not be more than one ModelStore.
func NewModelStore(s3Bucket string, endpointName string) *ModelStore {
	ms := ModelStore{
		s3Bucket:     s3Bucket,
		storeMutex:   sync.Mutex{},
		endpointName: endpointName,
	}
	return &ms
}

func (ms *ModelStore) StoreModel(ctx context.Context, tier tier.Tier, req lib.ModelInsertRequest) error {
	// lock to avoid race condition when two models are being attempted to stored with room only for one more model
	ms.storeMutex.Lock()
	defer ms.storeMutex.Unlock()

	// check there are no more than 15 active models
	// sagemaker does not allow more than 15 models with different containers on one endpoint
	activeModels, err := db.GetActiveModels(tier)
	if err != nil {
		return fmt.Errorf("failed to get active models from db: %v", err)
	}
	if len(activeModels) >= 15 {
		return fmt.Errorf("cannot have more than 15 active models: %v", err)
	}

	// upload to s3
	err = tier.S3Client.UploadModelToS3(req.ModelFile, req.ModelFileName, ms.s3Bucket)
	if err != nil {
		return fmt.Errorf("failed to upload model to s3: %v", err)
	}

	// now insert into db
	artifactPath := ms.getArtifactPath(req.ModelFileName)
	model := lib.Model{
		Name:             req.Name,
		Version:          req.Version,
		Framework:        req.Framework,
		FrameworkVersion: req.FrameworkVersion,
		ArtifactPath:     artifactPath,
	}
	_, err = db.InsertModel(tier, model)
	if err != nil {
		return fmt.Errorf("failed to insert model in db: %v", err)
	}
	return ms.EnsureEndpointExists(ctx, tier)
}

func (ms *ModelStore) EnsureEndpointExists(ctx context.Context, tier tier.Tier) error {
	// Get all active models.
	activeModels, err := db.GetActiveModels(tier)
	if err != nil {
		return fmt.Errorf("failed to get active models from db: %v", err)
	}
	if len(activeModels) == 0 {
		return nil
	}
	coveringModels, err := db.GetCoveringHostedModels(tier)
	if err != nil {
		return fmt.Errorf("failed to check if any sagemaker model covers all active models: %v", err)
	}
	var sagemakerModelName string
	if len(coveringModels) == 0 {
		sagemakerModelName = fmt.Sprintf("t-%d-model-%s", tier.ID, time.Now().Format("20060102150405"))
		hostedModels := make([]lib.SagemakerHostedModel, len(activeModels))
		for i, model := range activeModels {
			hostedModels[i] = lib.SagemakerHostedModel{
				SagemakerModelName: sagemakerModelName,
				ModelId:            model.Id,
				ContainerHostname:  lib.GetContainerName(model.Name, model.Version),
			}
		}
		err = db.InsertHostedModels(tier, hostedModels...)
		if err != nil {
			return fmt.Errorf("failed to insert hosted models into db: %v", err)
		}
	} else {
		// Just use the first covering model for now.
		sagemakerModelName = coveringModels[0]
	}

	// Create the model on sagemaker if it doesn't already exist.
	exists, err := tier.SagemakerClient.ModelExists(ctx, sagemakerModelName)
	if err != nil {
		return fmt.Errorf("failed to check if model exists on sagemaker: %v", err)
	}
	if !exists {
		err = tier.SagemakerClient.CreateModel(ctx, activeModels, sagemakerModelName)
		if err != nil {
			return fmt.Errorf("failed to create model on sagemaker: %v", err)
		}
	}

	// Ensure endpoint config exists in db and sagemaker.
	endpointCfg, err := db.GetEndpointConfigWithModel(tier, sagemakerModelName)
	if err != nil {
		return fmt.Errorf("failed to get endpoint config with name [%s] from db: %v", sagemakerModelName, err)
	}
	if endpointCfg.Name == "" {
		endpointCfg = lib.SagemakerEndpointConfig{
			Name:          fmt.Sprintf("%s-config-%d", sagemakerModelName, rand.Int63()),
			ModelName:     sagemakerModelName,
			VariantName:   sagemakerModelName,
			InstanceType:  "ml.t2.medium",
			InstanceCount: 1,
		}
		err = db.InsertEndpointConfig(tier, endpointCfg)
		if err != nil {
			return fmt.Errorf("failed to create endpoint config for model [%s] in db: %v", sagemakerModelName, err)
		}
	}
	exists, err = tier.SagemakerClient.EndpointConfigExists(ctx, endpointCfg.Name)
	if err != nil {
		return fmt.Errorf("failed to check if endpoint config exists on sagemaker: %v", err)
	}
	if !exists {
		err = tier.SagemakerClient.CreateEndpointConfig(ctx, endpointCfg)
		if err != nil {
			return fmt.Errorf("failed to create endpoint config on sagemaker: %v", err)
		}
	}

	// Ensure endpoint exists in db and sagemaker.
	endpoint, err := db.GetEndpoint(tier, ms.endpointName)
	if err != nil {
		return fmt.Errorf("failed to get endpoint with name [%s] from db: %v", ms.endpointName, err)
	}
	if endpoint.Name == "" || endpoint.EndpointConfigName != endpointCfg.Name {
		endpoint = lib.SagemakerEndpoint{
			Name:               ms.endpointName,
			EndpointConfigName: endpointCfg.Name,
		}
		err = db.InsertEndpoint(tier, endpoint)
		if err != nil {
			return fmt.Errorf("failed to insert endpoint with name [%s] into db: %v", ms.endpointName, err)
		}
	}
	exists, err = tier.SagemakerClient.EndpointExists(ctx, endpoint.Name)
	if err != nil {
		return fmt.Errorf("failed to check if endpoint exists on sagemaker: %v", err)
	}
	if !exists {
		err = tier.SagemakerClient.CreateEndpoint(ctx, endpoint)
		if err != nil {
			return fmt.Errorf("failed to create endpoint on sagemaker: %v", err)
		}
	} else if endpoint.EndpointConfigName != endpointCfg.Name {
		err = tier.SagemakerClient.UpdateEndpoint(ctx, lib.SagemakerEndpoint{
			Name:               ms.endpointName,
			EndpointConfigName: endpointCfg.Name,
		})
		if err != nil {
			return fmt.Errorf("failed to update endpoint on sagemaker: %v", err)
		}
	}
	return nil
}

func (ms *ModelStore) getArtifactPath(fileName string) string {
	return fmt.Sprintf("s3://%s/%s", ms.s3Bucket, fileName)
}
