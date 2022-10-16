package modelstore

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"fennel/lib/ftypes"
	lib "fennel/lib/sagemaker"
	"fennel/lib/value"
	db "fennel/model/sagemaker"
	"fennel/tier"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

// Note: errorStatus here should not be verbose or the "cardinality" should not be high
var scalingConfigErrors = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "sagemaker_scaling_config_errors",
	Help: "Errors corresponding to sagemaker scaling configuration",
}, []string{"errorStatus"})

type RetryError struct {
	err string
}

func (re RetryError) Error() string {
	return re.err + ", please retry after a few minutes"
}

// local cache of model name, version to model.
// key type is string, value type is string.
var modelCache = sync.Map{}

func genCacheKey(name, version string) string {
	return name + "-" + version
}

type ModelInfo struct {
	ModelName        string
	ModelStorage     string
	Framework        string
	FrameworkVersion string
	Info             string
}

type ModelRegistry = map[string]ModelInfo

var SupportedPretrainedModels = ModelRegistry{
	"sbert": ModelInfo{
		ModelName:        "sbert",
		ModelStorage:     "s3://sagemaker-us-west-2-pretrained/custom_inference/all-MiniLM-L6-v2/model.tar.gz",
		Framework:        "huggingface",
		FrameworkVersion: "4.12",
		Info:             "https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2",
	},
}

func GetSupportedModels() []string {
	keys := make([]string, 0, len(SupportedPretrainedModels))
	for k := range SupportedPretrainedModels {
		keys = append(keys, k)
	}
	return keys
}

// For pretrained models we will use this Id for everthing (model, endpoint, endpoint config)
// This enables us to directly check if end point exists and call it, without havings to maintain info in the db.
func PreTrainedModelId(model string, tierId ftypes.RealmID) string {
	return fmt.Sprintf("Model-%s-%d", model, tierId)
}

// The model file and the sagemaker region should be in the same region.
func ensureModelFileInRegion(tier tier.Tier, modelFile string) (string, error) {
	region := tier.SagemakerClient.GetSMCRegion()
	parts := strings.Split(modelFile, "/")

	s3Bucket := "sagemaker-" + region + "-pretrained"
	// parts is broken in [s3, "", <region_specific_prefix>, custom_inference, <model_name>, model.tar.gz]
	if len(parts) != 6 {
		return "", fmt.Errorf("model file path is not in the expected format: %v", modelFile)
	}

	prefix := parts[3] + "/" + parts[4]
	files, err := tier.S3Client.ListFiles(s3Bucket, prefix, "")
	if err != nil {
		return "", fmt.Errorf("failed to list files in s3: %w", err)
	}
	if len(files) != 0 {
		for _, f := range files {
			fileNameParts := strings.Split(f, "/")
			fileName := fileNameParts[len(fileNameParts)-1]
			if fileName == "model.tar.gz" {
				return "s3://" + s3Bucket + "/" + strings.Join(parts[3:], "/"), nil
			}
		}
	}
	err = tier.S3Client.CopyFile(strings.Join(parts[2:], "/"), strings.Join(parts[3:], "/"), s3Bucket)
	if err != nil {
		return "", fmt.Errorf("failed to copy files in s3: %w", err)
	}
	return "s3://" + s3Bucket + "/" + strings.Join(parts[3:], "/"), nil
}

// Creates an endpoint if it does not exist.
func EnableModel(ctx context.Context, tier tier.Tier, model string) error {
	modelConfig, ok := SupportedPretrainedModels[model]
	if !ok {
		return fmt.Errorf("model %s is not supported, currently supported models are: %s", model, strings.Join(GetSupportedModels(), ", "))
	}
	sagemakerModelId := PreTrainedModelId(model, tier.ID)

	// Check if endpoint exists
	exists, err := tier.SagemakerClient.EndpointExists(ctx, sagemakerModelId)
	if err != nil {
		return fmt.Errorf("failed to check if endpoint exists on sagemaker: %v", err)
	}
	if exists {
		return nil
	}

	// If not, create endpoint, which consists of 3 steps.

	// 1. Create SageMaker model.
	exists, err = tier.SagemakerClient.ModelExists(ctx, sagemakerModelId)
	if err != nil {
		return fmt.Errorf("failed to check if model exists on sagemaker: %v", err)
	}
	if !exists {
		modelStorage, err := ensureModelFileInRegion(tier, modelConfig.ModelStorage)
		if err != nil {
			return fmt.Errorf("failed to ensure if model file exists in region: %w", err)
		}

		model := lib.Model{
			Name:             model,
			Version:          "1",
			Framework:        modelConfig.Framework,
			FrameworkVersion: modelConfig.FrameworkVersion,
			ArtifactPath:     modelStorage,
			ContainerName:    "Container-" + model + "-1",
		}
		err = tier.SagemakerClient.CreateModel(ctx, []lib.Model{model}, sagemakerModelId)
		if err != nil {
			return fmt.Errorf("failed to create model on sagemaker: %v", err)
		}
	}

	// 2. Create SageMaker endpoint config

	exists, err = tier.SagemakerClient.EndpointConfigExists(ctx, sagemakerModelId)
	if err != nil {
		return fmt.Errorf("failed to check if endpoint config exists on sagemaker: %v", err)
	}
	if !exists {
		endpointCfg := lib.SagemakerEndpointConfig{
			Name:                     sagemakerModelId,
			ModelName:                sagemakerModelId,
			VariantName:              sagemakerModelId,
			ServerlessMaxConcurrency: 75,
			ServerlessMemory:         4096,
		}
		err = tier.SagemakerClient.CreateEndpointConfig(ctx, endpointCfg)
		if err != nil {
			return fmt.Errorf("failed to create endpoint config on sagemaker: %v", err)
		}
	}

	// 3. Create SageMaker endpoint

	endpoint := lib.SagemakerEndpoint{
		Name:               sagemakerModelId,
		EndpointConfigName: sagemakerModelId,
	}
	return tier.SagemakerClient.CreateEndpoint(ctx, endpoint)
}

// Store attempts to store a model in the DB and SageMaker. Returns an error
// of type modelstore.RetryError when retrying after a few minutes is recommended.
func Store(ctx context.Context, tier tier.Tier, req lib.ModelUploadRequest) error {
	// lock to avoid race condition when two models are being attempted to stored with room only for one more model
	// TODO - does not work across servers, so should use distributed lock
	tier.ModelStore.Lock()
	defer tier.ModelStore.Unlock()
	ok, err := tier.SagemakerClient.EndpointExists(ctx, tier.ModelStore.EndpointName())
	if err != nil {
		return fmt.Errorf("failed to check if endpoint [%v] exists: %w", tier.ModelStore.EndpointName(), err)
	}
	if ok {
		// If endpoint exists, wait for it to be in service as we cannot make changes to the endpoint
		// when it is not in service.
		err = EnsureEndpointInService(ctx, tier)
		if err != nil {
			return fmt.Errorf("failed to store model; endpoint not in service: %w", err)
		}
	}
	// If it does not exist, it will be created later

	// check there are no more than 15 active models
	// sagemaker does not allow more than 15 models with different containers on one endpoint
	activeModels, err := db.GetActiveModels(tier)
	if err != nil {
		return fmt.Errorf("failed to get active models from db: %v", err)
	}
	if len(activeModels) >= 15 {
		return fmt.Errorf("cannot have more than 15 active models: %v", err)
	}

	containerName := lib.GenContainerName()
	// upload to s3
	err = tier.S3Client.Upload(req.ModelFile, containerName, tier.ModelStore.S3Bucket())
	if err != nil {
		return fmt.Errorf("failed to upload model to s3: %v", err)
	}

	// now insert into db
	artifactPath := tier.ModelStore.GetArtifactPath(containerName)
	model := lib.Model{
		Name:             req.Name,
		Version:          req.Version,
		Framework:        req.Framework,
		FrameworkVersion: req.FrameworkVersion,
		ArtifactPath:     artifactPath,
		ContainerName:    containerName,
	}
	_, err = db.InsertModel(tier, model)
	if err != nil {
		return fmt.Errorf("failed to insert model in db: %v", err)
	}
	err = EnsureEndpointExists(ctx, tier)
	// revert changes to db and s3 if failed to update endpoint
	if err != nil {
		err1 := db.DeleteModel(tier, model.Name, model.Version)
		err2 := tier.S3Client.Delete(containerName, tier.ModelStore.S3Bucket())
		if err1 != nil || err2 != nil {
			return fmt.Errorf("failed to upload model to sagemaker: [%v] and failed to revert db change: [%v] [%v]", err, err1, err2)
		}
	}
	return err
}

// Remove attempts to remove a model from the DB and SageMaker. Returns an error
// of type modelstore.RetryError when retrying after a few minutes is recommended.
func Remove(ctx context.Context, tier tier.Tier, name, version string) error {
	tier.ModelStore.Lock()
	defer tier.ModelStore.Unlock()
	ok, err := tier.SagemakerClient.EndpointExists(ctx, tier.ModelStore.EndpointName())
	if err != nil {
		return fmt.Errorf("failed to delete model: %v", err)
	}
	if ok {
		// If endpoint exists, wait for it to be in service as we cannot make changes to the endpoint
		// when it is not in service.
		err = EnsureEndpointInService(ctx, tier)
		if err != nil {
			return fmt.Errorf("failed to remove model; endpoint not in service: %w", err)
		}
	}
	// If it does not exist, it will be created later

	model, err := db.GetModel(tier, name, version)
	if err != nil {
		return fmt.Errorf("failed to load model from db: %w", err)
	}
	err = db.MakeModelInactive(tier, name, version)
	if err != nil {
		return fmt.Errorf("failed to deactivate model in db: %v", err)
	}
	err = EnsureEndpointExists(ctx, tier)
	if err != nil {
		_, err2 := db.InsertModel(tier, model)
		if err2 != nil {
			return fmt.Errorf("failed to upload model to sagemaker: [%v] and failed to revert db change: [%v]", err, err2)
		}
	}
	// delete from s3 only if delete on sagemaker endpoint succeeded
	if err == nil {
		err2 := tier.S3Client.Delete(model.ContainerName, tier.ModelStore.S3Bucket())
		if err2 != nil {
			return fmt.Errorf("failed to delete model from s3: %v", err2)
		}
	}
	return err
}

func PreTrainedScore(ctx context.Context, tier tier.Tier, modelName string, inputs []value.Value) ([]value.Value, error) {
	modelConfig, ok := SupportedPretrainedModels[modelName]
	if !ok {
		return nil, fmt.Errorf("model %s is not supported, currently supported models are: %s", modelName, strings.Join(GetSupportedModels(), ", "))
	}
	req := lib.ScoreRequest{
		Framework:    modelConfig.Framework,
		EndpointName: PreTrainedModelId(modelName, tier.ID),
		ModelInput:   value.NewList(inputs...),
	}
	res, err := tier.SagemakerClient.Score(ctx, &req)
	if err != nil {
		return nil, err
	}
	return res.Scores, err
}

// Score calls SageMaker to score the model with provided list of inputs and returns a corresponding list of outputs
// on a successful run. Returns an error of type modelstore.RetryError when the error is only
// temporary and sending the request again after a few minutes is recommended.
func Score(
	ctx context.Context, tier tier.Tier, name, version string, featureVecs []value.Value,
) (res []value.Value, err error) {
	ckey := genCacheKey(name, version)
	var model lib.Model
	val, ok := modelCache.Load(ckey)
	if ok {
		model, ok = val.(lib.Model)
	}
	if !ok {
		model, err = db.GetModel(tier, name, version)
		if err != nil {
			return nil, fmt.Errorf("could not get model from db: %w", err)
		}
		modelCache.Store(ckey, model)
	}
	req := lib.ScoreRequest{
		Framework:     model.Framework,
		EndpointName:  tier.ModelStore.EndpointName(),
		ContainerName: model.ContainerName,
		ModelInput:    value.NewList(featureVecs...),
	}
	response, err := tier.SagemakerClient.Score(ctx, &req)

	if err == nil {
		return response.Scores, nil
	}

	/*
		Updating the endpoint on sagemaker takes about 5-20 minutes during which it works with the
		previous endpoint configuration. Attempting to score a newly uploaded model would return
		a not found error. We check if the endpoint is updating, and if the model to be scored
		is active, and if the corresponding covering model is hosted. In that case, we return
		an error asking to wait for the endpoint to be updated.
	*/
	status, err2 := tier.SagemakerClient.GetEndpointStatus(ctx, tier.ModelStore.EndpointName())
	if err2 != nil {
		return nil, fmt.Errorf("failed to score the model: %v; %v", err, err2)
	}
	if status == "Creating" || status == "Updating" || status == "Deleting" || status == "RollingBack" {
		activeModels, err2 := db.GetActiveModels(tier)
		if err2 != nil {
			return nil, fmt.Errorf("failed to score the model: %v; %v", err, err2)
		}
		found := false
		for _, m := range activeModels {
			if name == m.Name && version == m.Version {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("failed to score the model: model is absent/inactive")
		}
		cover, err2 := db.GetCoveringHostedModels(tier)
		if err2 != nil {
			return nil, fmt.Errorf("failed to score the model: %v; %v", err, err2)
		}
		ok, err2 := tier.SagemakerClient.ModelExists(ctx, cover[0])
		if err2 != nil {
			return nil, fmt.Errorf("failed to score the model: %v; %v", err, err2)
		}
		if ok {
			return nil, RetryError{"failed to score the model: endpoint not updated with new model yet"}
		} else {
			return nil, fmt.Errorf("failed to score the model: covering model not hosted")
		}
	}
	return nil, fmt.Errorf("failed to score the model: %v", err)
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
			// Deleting endpoint with autoscaling configured is fine as long as the caller has the permissions
			// https://docs.aws.amazon.com/sagemaker/latest/dg/endpoint-scaling.html
			err := tier.SagemakerClient.DeleteEndpoint(ctx, tier.ModelStore.EndpointName())
			if err != nil {
				return fmt.Errorf("failed to delete endpoint '%s': %v", tier.ModelStore.EndpointName(), err)
			}
		}
		return nil
	}

	// in case model(s) was stored or removed, there wouldn't be a hosted model which would cover all the active models.
	// in these cases, `coveringModels` is an empty list, hence we end up creating a sagemaker hosted model for it
	// and it's corresponding endpoint configuration and update the endpoint by applying this new configuration
	//
	// TODO(REX-1203): Delete headless models and corresponding configurations from sagemaker and DB.
	coveringModels, err := db.GetCoveringHostedModels(tier)
	if err != nil {
		return fmt.Errorf("failed to check if any sagemaker model covers all active models: %v", err)
	}
	var sagemakerModelName string
	if len(coveringModels) == 0 {
		sagemakerModelName = fmt.Sprintf("t-%d-model-%s", tier.ID, tier.Clock.Now().Format("20060102150405"))
		hostedModels := make([]lib.SagemakerHostedModel, len(activeModels))
		for i, model := range activeModels {
			hostedModels[i] = lib.SagemakerHostedModel{
				SagemakerModelName: sagemakerModelName,
				ModelId:            model.Id,
				ContainerHostname:  model.ContainerName,
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
			Name:         fmt.Sprintf("%s-config-%d", sagemakerModelName, rand.Int63()),
			ModelName:    sagemakerModelName,
			VariantName:  sagemakerModelName,
			InstanceType: tier.SagemakerClient.GetInstanceType(),
			// TODO: use a larger number of initial instance size as an additional precaution step - creating and
			// applying a new endpoint configuration results in updating the endpoint during which autoscaling is
			// blocked (or we have not explicitly configured it yet), it might be better to start with a larger
			// initial size and let autoscaling scale-in.
			InstanceCount: tier.SagemakerClient.GetInstanceCount(),
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
			// should deregister the current endpoint configuration as a scalable target
			//
			// this is required in case of changing the instance type for a production variant that previously had
			// automatic scaling configured or removing a production variant that has automatic scaling configured
			//
			// since this is a generalized path, we always de-register the scaling policy attached to the current
			// endpoint configuration and re-enable it once the update has gone through.
			curVariantName, err := tier.SagemakerClient.GetProductionVariantName(ctx, endpointName)
			if err != nil {
				tier.Logger.Error("failed to get production model variant for endpoint", zap.String("endpoint", endpointName), zap.Error(err))
				return fmt.Errorf("failed to get production model variant for endpoint: %v", err)
			}
			if err = tier.SagemakerClient.DisableAutoscaling(ctx, endpointName, curVariantName); err != nil {
				tier.Logger.Error("failed to de-register autoscaling for endpoint", zap.String("endpoint", endpointName), zap.String("variant", curVariantName), zap.Error(err))
				return fmt.Errorf("failed to disable autoscaling for endpoint: %v", err)
			}

			// TODO: since we just registered autoscaling for the endpoint, we should update it with a larger instance
			// count as an additional precaution during the update (which can last upto 10-15 minutes, during which
			// a large traffic could potentially result in the model variant crashing (OOMs or high latencies).
			//
			// this could be done using `UpdateEndpointWeightsAndCapacities`
			err = tier.SagemakerClient.UpdateEndpoint(ctx, lib.SagemakerEndpoint{
				Name:               endpointName,
				EndpointConfigName: endpointCfg.Name,
			})
			if err != nil {
				return fmt.Errorf("failed to update endpoint on sagemaker: %v", err)
			}
		}
	}
	// once the endpoint is "InService", re-enable or register the endpoint configuration as a scalable target.
	// since the endpoint update can take ~10-15 minutes, this needs to happen asynchronously.
	go EnableAutoscalingWhenEndpointInService(tier, endpointName, sagemakerModelName)
	return nil
}

// EnsureEndpointInService checks if the endpoint is in service. Returns an error if it is not in service and a bool
// which is true when the endpoint is only temporarily out of service and will soon be available again.
func EnsureEndpointInService(ctx context.Context, tier tier.Tier) (err error) {
	endpointName := tier.ModelStore.EndpointName()
	status, err := tier.SagemakerClient.GetEndpointStatus(ctx, endpointName)
	if err != nil {
		return fmt.Errorf("failed to get endpoint status: %v", err)
	}
	switch status {
	case "InService":
		return nil
	case "Updating", "SystemUpdating", "RollingBack":
		return RetryError{"endpoint is updating"}
	case "Creating":
		return RetryError{"endpoint is being created"}
	case "Deleting":
		return RetryError{"endpoint is being deleted"}
	case "OutOfService":
		return fmt.Errorf("endpoint out of service and not available to take incoming requests")
	case "Failed":
		return fmt.Errorf("endpoint failed and must be deleted")
	default:
		return fmt.Errorf("endpoint in unknown status")
	}
}

func EnableAutoscalingWhenEndpointInService(tier tier.Tier, sagemakerEndpointName, modelVariantName string) {
	// we use an empty context here since the context passed as part of the request could be cancelled by now.
	ctx := context.Background()

	// check if the variant is configured as a scalable target already (since this is a generalized path,
	// this function could have been called even when there was no update to the variant/configs/endpoints).
	zapEndpointName := zap.String("endpoint", sagemakerEndpointName)
	zapVariantName := zap.String("variant", modelVariantName)
	found, err := tier.SagemakerClient.IsAutoscalingConfigured(ctx, sagemakerEndpointName, modelVariantName)
	if err != nil {
		tier.Logger.Error("failed to check if autoscaling is configured for endpoint and variant", zapEndpointName, zapVariantName, zap.Error(err))
		scalingConfigErrors.WithLabelValues("IsAutoscalingConfigured").Inc()
		return
	}
	if found {
		// nothing to do
		tier.Logger.Info("autoscaling already configured for endpoint and variant", zapEndpointName, zapVariantName)
		return
	}
	// wait for the endpoint to be in `InService` state
	ticker := time.NewTicker(1 * time.Second)
	inService := false
	for {
		select {
		case <-ticker.C:
			err := EnsureEndpointInService(ctx, tier)
			if err == nil {
				inService = true
				break
			}
			_, ok := err.(RetryError)
			if !ok {
				// not a retry error, should fail
				tier.Logger.Error("checking endpoint status failed", zapEndpointName, zap.Error(err))
				scalingConfigErrors.WithLabelValues("EnsureEndpointExists").Inc()
				return
			}
			// retry error, continue
		default:
		}
		if inService {
			break
		}
	}

	// register the variant as a scalable target

	// TODO: Consider configuring these values as dynamic configurations
	// currently starting with reasonable values
	if err := tier.SagemakerClient.EnableAutoscaling(ctx, sagemakerEndpointName, modelVariantName, lib.ScalingConfiguration{
		Cpu: lib.CpuScalingPolicy{
			CpuTargetValue: 70,
			// scale out aggressively than scaling in
			ScaleInCoolDownPeriod:  180,
			ScaleOutCoolDownPeriod: 60,
		},
		BaseConfig: &lib.BaseConfig{
			// use the initial instance count as the min capacity - ideally this should be separately configurable
			// as well
			MinCapacity: int64(tier.SagemakerClient.GetInstanceCount()),
			MaxCapacity: 5,
		},
	}); err != nil {
		tier.Logger.Error("failed to enable autoscaling for endpoint and variant", zapEndpointName, zapVariantName, zap.Error(err))
		scalingConfigErrors.WithLabelValues("EnableAutoscaling").Inc()
		return
	}
	tier.Logger.Info("successfully enabled autoscaling for endpoint and variant", zapEndpointName, zapVariantName)
}

func getDummyModel(model lib.Model) lib.Model {
	return lib.Model{
		Name:             model.Name,
		Version:          strconv.Itoa(rand.Int()),
		Framework:        model.Framework,
		FrameworkVersion: model.FrameworkVersion,
		ArtifactPath:     model.ArtifactPath,
		ContainerName:    lib.GenContainerName(),
	}
}
