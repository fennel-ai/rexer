package sagemaker

import (
	"context"
	"fmt"
	"time"

	"fennel/lib/value"
)

type BaseConfig struct {
	MinCapacity int64
	MaxCapacity int64
}

type CpuScalingPolicy struct {
	CpuTargetValue         float64
	ScaleInCoolDownPeriod  int64
	ScaleOutCoolDownPeriod int64
}

type ScalingConfiguration struct {
	*BaseConfig
	Cpu CpuScalingPolicy
}

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
	EnableAutoscaling(ctx context.Context, sagemakerEndpointName string, modelVariantName string, scalingConfig ScalingConfiguration) error
	DisableAutoscaling(ctx context.Context, sagemakerEndpointName string, modelVariantName string) error
}

type InferenceServer interface {
	Score(ctx context.Context, req *ScoreRequest) (*ScoreResponse, error)
}

type ScoreRequest struct {
	EndpointName  string
	ContainerName string
	Framework     string
	FeaturesList  []value.Value
}

type ScoreResponse struct {
	Scores []value.Value
}

func GenContainerName() string {
	return fmt.Sprintf("Container-%d", time.Now().UnixNano())
}
