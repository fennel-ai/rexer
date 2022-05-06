package modelstore

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"fennel/lib/ftypes"
	lib "fennel/lib/sagemaker"
	"fennel/lib/value"
	db "fennel/model/sagemaker"
	"fennel/tier"
	"github.com/pkg/errors"
)

// Store attempts to store a model in the DB and SageMaker. When it fails, returns an error along with a bool
// which is true when retrying after a few minutes is recommended.
func Store(ctx context.Context, tier tier.Tier, req lib.ModelUploadRequest) (err error, retry bool) {
	// lock to avoid race condition when two models are being attempted to stored with room only for one more model
	// TODO - does not work across servers, so should use distributed lock
	tier.ModelStore.Lock()
	defer tier.ModelStore.Unlock()
	ok, err := tier.SagemakerClient.EndpointExists(ctx, tier.ModelStore.EndpointName())
	if err != nil {
		return errors.Wrapf(err, "failed to check if endpoint [%s] exists", tier.ModelStore.EndpointName()), false
	}
	if ok {
		// If endpoint exists, wait for it to be in service as we cannot make changes to the endpoint
		// when it is not in service.
		err, retry = EnsureEndpointInService(ctx, tier)
		if err != nil {
			return err, retry
		}
	}
	// If it does not exist, it will be created later

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
	err = tier.S3Client.Upload(req.ModelFile, getPath(tier.ID, fileName), tier.ModelStore.S3Bucket())
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

// Remove attempts to remove a model from the DB and SageMaker. When it fails, returns an error along with a bool
// which is true when retrying after a few minutes is recommended.
func Remove(ctx context.Context, tier tier.Tier, name, version string) (err error, retry bool) {
	tier.ModelStore.Lock()
	defer tier.ModelStore.Unlock()
	ok, err := tier.SagemakerClient.EndpointExists(ctx, tier.ModelStore.EndpointName())
	if err != nil {
		return fmt.Errorf("failed to delete model: %v", err), false
	}
	if ok {
		// If endpoint exists, wait for it to be in service as we cannot make changes to the endpoint
		// when it is not in service.
		err, retry = EnsureEndpointInService(ctx, tier)
		if err != nil {
			return err, retry
		}
	}
	// If it does not exist, it will be created later

	// delete from s3
	err = tier.S3Client.Delete(getPath(tier.ID, lib.GetContainerName(name, version)), tier.ModelStore.S3Bucket())
	if err != nil {
		return fmt.Errorf("failed to delete model from s3: %v", err), false
	}
	err = db.MakeModelInactive(tier, name, version)
	if err != nil {
		return fmt.Errorf("failed to deactivate model in db: %v", err), false
	}
	return EnsureEndpointExists(ctx, tier), false
}

// Score calls SageMaker to score the model with provided list of inputs and returns a corresponding list of outputs
// on a successful run. When there is an error, it returns the error and a bool which is true when the error is only
// temporary and sending the request again after a few minutes is recommended.
func Score(
	ctx context.Context, tier tier.Tier, name, version string, featureVecs []value.List,
) (res []value.Value, err error, retry bool) {
	framework, err := db.GetFramework(tier, name, version)
	if err != nil {
		return nil, fmt.Errorf("could not get framework from db: %v", err), false
	}
	req := lib.ScoreRequest{
		Framework:     framework,
		EndpointName:  tier.ModelStore.EndpointName(),
		ContainerName: lib.GetContainerName(name, version),
		FeatureLists:  featureVecs,
	}
	response, err := tier.SagemakerClient.Score(ctx, &req)
	if err != nil {
		/*
			Updating the endpoint on sagemaker takes about 11 minutes during which it works with the
			previous endpoint configuration. Attempting to score a newly uploaded model would return
			a not found error. We check if the endpoint is updating, and if the model to be scored
			is active, and if the corresponding covering model is hosted. In that case, we return
			an error asking to wait for the endpoint to be updated.
		*/
		status, err2 := tier.SagemakerClient.GetEndpointStatus(ctx, tier.ModelStore.EndpointName())
		if err2 != nil {
			return nil, fmt.Errorf("failed to score the model: %v; %v", err, err2), false
		}
		if status == "Creating" || status == "Updating" || status == "Deleting" || status == "RollingBack" {
			activeModels, err2 := db.GetActiveModels(tier)
			if err2 != nil {
				return nil, fmt.Errorf("failed to score the model: %v; %v", err, err2), false
			}
			found := false
			for _, m := range activeModels {
				if name == m.Name && version == m.Version {
					found = true
					break
				}
			}
			if !found {
				return nil, fmt.Errorf("failed to score the model: model is absent/inactive"), false
			}
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
			} else {
				return nil, fmt.Errorf("failed to score the model: covering model not hosted"), false
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
		// if there are no active models, delete endpoint if it exists to clean up
		ok, err := tier.SagemakerClient.EndpointExists(ctx, tier.ModelStore.EndpointName())
		if err != nil {
			return fmt.Errorf("failed to check endpoint exists: %v", err)
		}
		if ok {
			err := tier.SagemakerClient.DeleteEndpoint(ctx, tier.ModelStore.EndpointName())
			if err != nil {
				return fmt.Errorf("failed to delete endpoint '%s': %v", tier.ModelStore.EndpointName(), err)
			}
		}
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
		// The type of endpoint we use requires at least two models.
		// So, we add a dummy model when there is only one model.
		if len(activeModels) == 1 {
			activeModels = append(activeModels, getDummyModel(activeModels[0]))
		}
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
		curEndpointCfgName, err := tier.SagemakerClient.GetEndpointConfigName(ctx, endpointName)
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

// EnsureEndpointInService checks if the endpoint is in service. Returns an error if it is not in service and a bool
// which is true when the endpoint is only temporarily out of service and will soon be available again.
func EnsureEndpointInService(ctx context.Context, tier tier.Tier) (err error, tmp bool) {
	endpointName := tier.ModelStore.EndpointName()
	status, err := tier.SagemakerClient.GetEndpointStatus(ctx, endpointName)
	if err != nil {
		return fmt.Errorf("failed to get endpoint status: %v", err), false
	}
	switch status {
	case "InService":
		return nil, false
	case "Updating", "SystemUpdating", "RollingBack":
		return fmt.Errorf("endpoint is updating, please wait for a few minutes"), true
	case "Creating":
		return fmt.Errorf("endpoint is being created, please wait for a few minutes"), true
	case "Deleting":
		return fmt.Errorf("endpoint is being deleted, please wait for a few minutes"), true
	case "OutOfService":
		return fmt.Errorf("endpoint out of service and not available to take incoming requests"), false
	case "Failed":
		return fmt.Errorf("endpoint failed and must be deleted"), false
	default:
		return fmt.Errorf("endpoint in unknown status"), false
	}
}

func getPath(tierID ftypes.RealmID, fileName string) string {
	return fmt.Sprintf("t-%d/%s", tierID, fileName)
}

func getDummyModel(model lib.Model) lib.Model {
	return lib.Model{
		Name:             model.Name,
		Version:          strconv.Itoa(rand.Int()),
		Framework:        model.Framework,
		FrameworkVersion: model.FrameworkVersion,
		ArtifactPath:     model.ArtifactPath,
	}
}
