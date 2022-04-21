package modelstore

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	lib "fennel/lib/sagemaker"
	"fennel/lib/value"
	db "fennel/model/sagemaker"
	"fennel/tier"
)

func Store(ctx context.Context, tier tier.Tier, req lib.ModelUploadRequest) (error, bool) {
	// lock to avoid race condition when two models are being attempted to stored with room only for one more model
	// TODO - does not work across servers, so should use distributed lock
	tier.ModelStore.Lock()
	defer tier.ModelStore.Unlock()
	err, retry := EnsureEndpointInService(ctx, tier)
	if err != nil {
		return err, retry
	}

	// check there are no more than 15 active models
	// sagemaker does not allow more than 15 models with different containers on one endpoint
	activeModels, err := db.GetActiveModels(tier)
	if err != nil {
		return fmt.Errorf("failed to get active models from db: %v", err), false
	}
	if len(activeModels) >= 15 {
		return fmt.Errorf("cannot have more than 15 active models: %v", err), false
	}

	// upload to s3
	fileName := lib.GetContainerName(req.Name, req.Version)
	err = tier.S3Client.Upload(req.ModelFile, fileName, tier.ModelStore.S3Bucket())
	if err != nil {
		return fmt.Errorf("failed to upload model to s3: %v", err), false
	}

	// now insert into db
	artifactPath := tier.ModelStore.GetArtifactPath(fileName)
	model := lib.Model{
		Name:             req.Name,
		Version:          req.Version,
		Framework:        req.Framework,
		FrameworkVersion: req.FrameworkVersion,
		ArtifactPath:     artifactPath,
	}
	_, err = db.InsertModel(tier, model)
	if err != nil {
		return fmt.Errorf("failed to insert model in db: %v", err), false
	}
	return EnsureEndpointExists(ctx, tier), false
}

func Remove(ctx context.Context, tier tier.Tier, name, version string) (error, bool) {
	tier.ModelStore.Lock()
	defer tier.ModelStore.Unlock()
	err, retry := EnsureEndpointInService(ctx, tier)
	if err != nil {
		return err, retry
	}

	// delete from s3
	err = tier.S3Client.Delete(lib.GetContainerName(name, version), tier.ModelStore.S3Bucket())
	if err != nil {
		return fmt.Errorf("failed to delete model from s3: %v", err), false
	}
	err = db.MakeModelInactive(tier, name, version)
	if err != nil {
		return fmt.Errorf("failed to deactivate model in db: %v", err), false
	}
	return EnsureEndpointExists(ctx, tier), false
}

func Score(ctx context.Context, tier tier.Tier, name, version string, featureVecs []value.List) ([]value.Value, error, bool) {
	tier.ModelStore.Lock()
	defer tier.ModelStore.Unlock()

	req := lib.ScoreRequest{
		EndpointName: tier.ModelStore.EndpointName(),
		ModelName:    name,
		ModelVersion: version,
		FeatureLists: featureVecs,
	}
	response, err := tier.SagemakerClient.Score(ctx, &req)
	if err != nil {
		status, err2 := tier.SagemakerClient.GetEndpointStatus(ctx, tier.ModelStore.EndpointName())
		if err2 != nil {
			return nil, fmt.Errorf("failed to score the model: %v; %v", err, err2), false
		}
		if status == "Updating" {
			cover, err2 := db.GetCoveringHostedModels(tier)
			if err2 != nil {
				return nil, fmt.Errorf("failed to score the model: %v; %v", err, err2), false
			}
			ok, err2 := tier.SagemakerClient.ModelExists(ctx, cover[0])
			if err2 != nil {
				return nil, fmt.Errorf("failed to score the model: %v; %v", err, err2), false
			}
			if ok {
				return nil, fmt.Errorf("failed to score the model: endpoint not updated with new model yet"), true
			}
		}
		return nil, fmt.Errorf("failed to score the model: %v", err), false
	}
	return response.Scores, nil, false
}

func EnsureEndpointExists(ctx context.Context, tier tier.Tier) error {
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
	endpointName := tier.ModelStore.EndpointName()
	endpoint, err := db.GetEndpoint(tier, endpointName)
	if err != nil {
		return fmt.Errorf("failed to get endpoint with name [%s] from db: %v", endpointName, err)
	}
	if endpoint.Name == "" || endpoint.EndpointConfigName != endpointCfg.Name {
		endpoint = lib.SagemakerEndpoint{
			Name:               endpointName,
			EndpointConfigName: endpointCfg.Name,
		}
		err = db.InsertEndpoint(tier, endpoint)
		if err != nil {
			return fmt.Errorf("failed to insert endpoint with name [%s] into db: %v", endpointName, err)
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
	} else {
		curEndpointCfgName, err := tier.SagemakerClient.GetCurrentEndpointConfigName(ctx, endpointName)
		if err != nil {
			return fmt.Errorf("couldn't get current endpoint config's name from sagemaker: %v", err)
		}
		if curEndpointCfgName != endpointCfg.Name {
			err = tier.SagemakerClient.UpdateEndpoint(ctx, lib.SagemakerEndpoint{
				Name:               endpointName,
				EndpointConfigName: endpointCfg.Name,
			})
			if err != nil {
				return fmt.Errorf("failed to update endpoint on sagemaker: %v", err)
			}
		}
	}
	return nil
}

func EnsureEndpointInService(ctx context.Context, tier tier.Tier) (error, bool) {
	endpointName := tier.ModelStore.EndpointName()
	status, err := tier.SagemakerClient.GetEndpointStatus(ctx, endpointName)
	if err != nil {
		return fmt.Errorf("failed to get endpoint status: %v", err), false
	}
	if status == "Updating" || status == "SystemUpdating" {
		return fmt.Errorf("endpoint is updating, please wait for a few minutes"), true
	}
	return nil, false
}
