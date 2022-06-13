package sagemaker

import (
	"context"
	"fmt"

	"fennel/lib/value"
)

type SagemakerRegistry interface {
	CreateModel(ctx context.Context, hostedModels []Model, sagemakerModelName string) error
	CreateEndpointConfig(ctx context.Context, cfg SagemakerEndpointConfig) error
	CreateEndpoint(ctx context.Context, endpoint SagemakerEndpoint) error

	ModelExists(ctx context.Context, sagemakerModelName string) (bool, error)
	EndpointConfigExists(ctx context.Context, sagemakerEndpointConfigName string) (bool, error)
	EndpointExists(ctx context.Context, sagemakerEndpointName string) (bool, error)

	DeleteModel(ctx context.Context, sagemakerModelName string) error
	DeleteEndpointConfig(ctx context.Context, sagemakerEndpointConfigName string) error
	DeleteEndpoint(ctx context.Context, sagemakerEndpointName string) error

	GetEndpointStatus(ctx context.Context, sagemakerEndpointName string) (string, error)
	UpdateEndpoint(ctx context.Context, endpoint SagemakerEndpoint) error

	IsAutoscalingConfigured(ctx context.Context, sagemakerEndpointName string, modelVariantName string) (bool, error)
	EnableAutoscaling(ctx context.Context, sagemakerEndpointName string, modelVariantName string) error
	DisableAutoscaling(ctx context.Context, sagemakerEndpointName string, modelVariantName string) error
}

type InferenceServer interface {
	Score(ctx context.Context, req *ScoreRequest) (*ScoreResponse, error)
}

type ScoreRequest struct {
	EndpointName  string
	ContainerName string
	Framework     string
	FeatureLists  []value.List
}

type ScoreResponse struct {
	Scores []value.Value
}

func GetContainerName(modelName, modelVersion string) string {
	return fmt.Sprintf("Container-%s-%s", modelName, modelVersion)
}
