package vae

import (
	"context"
	"embed"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	lib "fennel/lib/sagemaker"
	"fennel/lib/value"
	sm "fennel/sagemaker"
	"fennel/tier"
	"fmt"
	"strings"
)

var (
	//go:embed sagemaker-pipelines/*
	res   embed.FS
	pages = map[string]string{
		"vae":  "sagemaker-pipelines/vae.json",
		"test": "sagemaker-pipelines/test.json",
	}
)

const (
	UserHistoryAggSuffix = "-INTERNAL-USER_HISTORY"
	NumEpochs            = "30"
	InferenceType        = "ml.m5.large"
	TrainingDevice       = "cuda"
)

func GetPipelineDefinition(aggregateType string) ([]byte, error) {
	if pipeline, ok := pages[aggregateType]; ok {
		return res.ReadFile(pipeline)
	}
	return []byte{}, fmt.Errorf("unknown pipeline type: %s", aggregateType)
}

func GetModelEndpointName(tierId ftypes.RealmID, aggregateName ftypes.AggName) string {
	// Endpoint names cannot contain underscores.
	aggNameFixed := strings.Replace(string(aggregateName), "_", "-", -1)
	return fmt.Sprintf("%d-%s-%s", tierId, aggNameFixed, "VAE-Endpoint")
}

func GetModelName(tierId ftypes.RealmID, aggregateName ftypes.AggName) string {
	aggNameFixed := strings.Replace(string(aggregateName), "_", "-", -1)
	return fmt.Sprintf("%d-%s-%s", tierId, aggNameFixed, "VAE-Model")
}

func GetDerivedUserHistoryAggregateName(aggregateName ftypes.AggName) ftypes.AggName {
	return ftypes.AggName(fmt.Sprintf("%s-%s", aggregateName, UserHistoryAggSuffix))
}

func GetPipelineARN(ctx context.Context, tier tier.Tier, pipelineName string) (string, error) {
	pipelineArn, err := tier.SagemakerClient.GetPipelineARN(ctx, tier.ID, pipelineName)
	if err != nil && err == sm.SageMakerPipelineNotFound {
		pipelineDef, err := GetPipelineDefinition(pipelineName)
		if err != nil {
			return "", fmt.Errorf("failed to get pipeline defintion: %w", err)
		}
		if err = tier.SagemakerClient.CreatePipeline(ctx, tier.ID, pipelineName, string(pipelineDef)); err != nil {
			return "", fmt.Errorf("failed to create pipeline: %w", err)
		}
		if pipelineArn, err = tier.SagemakerClient.GetPipelineARN(ctx, tier.ID, pipelineName); err != nil {
			return "", fmt.Errorf("failed to get pipeline ARN: %w", err)
		}
	} else if err != nil {
		return "", fmt.Errorf("unknown error while trying to get pipeline ARN: %w", err)
	}
	return pipelineArn, nil
}

func GetSageMakerPipelineParams(tierId ftypes.RealmID, aggName ftypes.AggName, s3Bucket string) map[string]string {
	return map[string]string{
		"ModelEndpointName":         GetModelEndpointName(tierId, aggName),
		"ModelName":                 GetModelName(tierId, aggName),
		"ModelInferenceMachineType": InferenceType,
		"Device":                    TrainingDevice,
		"NumEpochs":                 NumEpochs,
		"S3DataBucket":              s3Bucket,
		"S3DataDirectory":           fmt.Sprintf("%s/%s", "automl", aggName),
	}
}

func GetUserHistoryAggregate(agg aggregate.Aggregate) aggregate.Aggregate {
	userHistoryAggregate := agg
	userHistoryAggregate.Name = GetDerivedUserHistoryAggregateName(agg.Name)
	userHistoryAggregate.Options.AggType = "list"
	userHistoryAggregate.Options.Limit = 0
	userHistoryAggregate.Options.CronSchedule = ""
	return userHistoryAggregate
}

func GetAutoMLPrediction(ctx context.Context, tier tier.Tier, agg aggregate.Aggregate, modelInput []value.Value) ([]value.Value, error) {
	endPoint := GetModelEndpointName(tier.ID, agg.Name)
	scoreInput := value.NewDict(map[string]value.Value{
		"user_histories": value.NewList(modelInput...),
		"get_embedding":  value.Bool(false),
		"limit":          value.Int(agg.Options.Limit),
	})

	scoreRequest := &lib.ScoreRequest{
		EndpointName: endPoint,
		Framework:    "pytorch",
		ModelInput:   scoreInput,
	}
	response, err := tier.SagemakerClient.Score(ctx, scoreRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to score: %w", err)
	}

	if len(response.Scores) == 0 {
		return nil, fmt.Errorf("no scores returned")
	}
	autoMLResponse := make([]value.Value, 0, len(response.Scores))
	for _, responseScores := range response.Scores {
		responseScoresList, ok := responseScores.(value.List)
		if !ok {
			return nil, fmt.Errorf("failed to convert response scores to list: %w", err)
		}
		userResponse := make([]value.Value, 0, responseScoresList.Len())
		for _, result := range responseScoresList.Values() {
			tuple := result.(value.List)
			id, err := tuple.At(0)
			if err != nil {
				return nil, fmt.Errorf("failed to get id: %w", err)
			}
			score, err := tuple.At(1)
			if err != nil {
				return nil, fmt.Errorf("failed to get score: %w", err)
			}
			userResponse = append(userResponse, value.NewDict(map[string]value.Value{
				"item":  id,
				"score": score.(value.Double),
			}))
		}
		autoMLResponse = append(autoMLResponse, value.NewList(userResponse...))
	}
	return autoMLResponse, nil
}
