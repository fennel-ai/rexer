package modelstore

import (
	"context"
	"fmt"
	"time"

	lib "fennel/lib/sagemaker"
	db "fennel/model/sagemaker"
	"fennel/tier"
)

func EnsureEndpointExists(ctx context.Context, tier tier.Tier, endpointName string) error {
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
			Name:          fmt.Sprintf("%s-config", sagemakerModelName),
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
	} else if endpoint.EndpointConfigName != endpointCfg.Name {
		err = tier.SagemakerClient.UpdateEndpoint(ctx, lib.SagemakerEndpoint{
			Name:               endpointName,
			EndpointConfigName: endpointCfg.Name,
		})
		if err != nil {
			return fmt.Errorf("failed to update endpoint on sagemaker: %v", err)
		}
	}
	return nil
}
