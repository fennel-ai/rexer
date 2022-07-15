package sagemaker

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/applicationautoscaling"
	"github.com/aws/aws-sdk-go/service/sagemaker"
	"github.com/aws/aws-sdk-go/service/sagemakerruntime"
	"go.uber.org/zap"

	lib "fennel/lib/sagemaker"
)

const (
	serviceNamespace         = "sagemaker"
	scalableDimInstanceCount = "sagemaker:variant:DesiredInstanceCount"
	scalablePolicyType       = "TargetTrackingScaling"
)

type SagemakerArgs struct {
	Region                 string   `arg:"--region,env:AWS_REGION,help:AWS region"`
	SagemakerExecutionRole string   `arg:"--sagemaker-execution-role,env:SAGEMAKER_EXECUTION_ROLE,help:SageMaker execution role"`
	PrivateSubnets         []string `arg:"--private-subnets,env:PRIVATE_SUBNETS,help:Private subnets"`
	SagemakerSecurityGroup string   `arg:"--sagemaker-security-group,env:SAGEMAKER_SECURITY_GROUP,help:SageMaker security group"`
	SagemakerInstanceType  string   `arg:"--sagemaker-instance-type,env:SAGEMAKER_INSTANCE_TYPE,help:SageMaker instance type"`
	SagemakerInstanceCount uint     `arg:"--sagemaker-instance-count,env:SAGEMAKER_INSTANCE_COUNT,help:SageMaker instance count"`
}

func NewClient(args SagemakerArgs, logger *zap.Logger) (SMClient, error) {
	sess := session.Must(session.NewSession(
		&aws.Config{
			Region:                        aws.String(args.Region),
			CredentialsChainVerboseErrors: aws.Bool(true),
		},
	))
	runtime := sagemakerruntime.New(sess)
	metadata := sagemaker.New(sess)
	// TODO: Create application infra for application autoscaling as it supports autoscaling other AWS resources as well
	autoscaler := applicationautoscaling.New(sess)
	return SMClient{
		args:             args,
		logger:           logger,
		runtimeClient:    runtime,
		metadataClient:   metadata,
		autoscalerClient: autoscaler,
	}, nil
}

type SMClient struct {
	args             SagemakerArgs
	logger           *zap.Logger
	runtimeClient    *sagemakerruntime.SageMakerRuntime
	metadataClient   *sagemaker.SageMaker
	autoscalerClient *applicationautoscaling.ApplicationAutoScaling
}

var _ lib.SagemakerRegistry = SMClient{}
var _ lib.InferenceServer = SMClient{}

func (smc SMClient) GetSMCRegion() string {
	return smc.args.Region
}

func (smc SMClient) GetInstanceType() string {
	return smc.args.SagemakerInstanceType
}

func (smc SMClient) GetInstanceCount() uint {
	return smc.args.SagemakerInstanceCount
}

func (smc SMClient) CreateModel(ctx context.Context, hostedModels []lib.Model, sagemakerModelName string) error {
	if len(hostedModels) == 0 {
		return nil
	}
	vpcConfig := &sagemaker.VpcConfig{}
	vpcConfig.Subnets = aws.StringSlice(smc.args.PrivateSubnets)
	vpcConfig.SecurityGroupIds = aws.StringSlice([]string{smc.args.SagemakerSecurityGroup})
	modelInput := sagemaker.CreateModelInput{
		// TODO(abhay): Remove.
		// VpcConfig:        vpcConfig,
		ExecutionRoleArn: aws.String(smc.args.SagemakerExecutionRole),
		ModelName:        aws.String(sagemakerModelName),
	}
	for _, model := range hostedModels {
		env := map[string]*string{}
		if model.Framework == "sklearn" {
			env["SAGEMAKER_SUBMIT_DIRECTORY"] = aws.String("opt/ml/model")
			env["SAGEMAKER_PROGRAM"] = aws.String("inference.py")
		}
		image, err := getImage(model.Framework, model.FrameworkVersion, smc.args.Region)
		if err != nil {
			return fmt.Errorf("failed to get image: %v", err)
		}
		modelInput.Containers = append(modelInput.Containers, &sagemaker.ContainerDefinition{
			ContainerHostname: aws.String(model.ContainerName),
			Image:             aws.String(image),
			ModelDataUrl:      aws.String(model.ArtifactPath),
			Environment:       env,
		})
	}
	// InferenceExecutionConfig can be set only when the model has more than one containers.
	if len(hostedModels) > 1 {
		modelInput.InferenceExecutionConfig = &sagemaker.InferenceExecutionConfig{
			Mode: aws.String("Direct"),
		}
	}
	_, err := smc.metadataClient.CreateModelWithContext(ctx, &modelInput)
	if err != nil {
		return fmt.Errorf("failed to create model: %v", err)
	}
	return nil
}

func (smc SMClient) ModelExists(ctx context.Context, modelName string) (bool, error) {
	input := sagemaker.DescribeModelInput{
		ModelName: aws.String(modelName),
	}
	_, err := smc.metadataClient.DescribeModelWithContext(ctx, &input)
	if err != nil {
		if e, ok := err.(awserr.Error); ok {
			if e.Code() == "ValidationException" && strings.HasPrefix(e.Message(), "Could not find model") {
				return false, nil
			}
		}
		return false, fmt.Errorf("failed to check if model exists on sagemaker: %v", err)
	}
	return true, nil
}

func (smc SMClient) EndpointConfigExists(ctx context.Context, endpointConfigName string) (bool, error) {
	input := sagemaker.DescribeEndpointConfigInput{
		EndpointConfigName: aws.String(endpointConfigName),
	}
	_, err := smc.metadataClient.DescribeEndpointConfigWithContext(ctx, &input)
	if err != nil {
		if e, ok := err.(awserr.Error); ok {
			if e.Code() == "ValidationException" && strings.HasPrefix(e.Message(), "Could not find endpoint config") {
				return false, nil
			}
		}
		return false, fmt.Errorf("failed to check if endpoint config exists on sagemaker: %v", err)
	}
	return true, nil
}

func (smc SMClient) EndpointExists(ctx context.Context, endpointName string) (bool, error) {
	input := sagemaker.DescribeEndpointInput{
		EndpointName: aws.String(endpointName),
	}
	_, err := smc.metadataClient.DescribeEndpointWithContext(ctx, &input)
	if err != nil {
		if e, ok := err.(awserr.Error); ok {
			if e.Code() == "ValidationException" && strings.HasPrefix(e.Message(), "Could not find endpoint") {
				return false, nil
			}
		}
		return false, fmt.Errorf("failed to check if endpoint exists on sagemaker: %v", err)
	}
	return true, nil
}

func (smc SMClient) GetEndpointConfigName(ctx context.Context, endpointName string) (string, error) {
	input := sagemaker.DescribeEndpointInput{
		EndpointName: aws.String(endpointName),
	}
	res, err := smc.metadataClient.DescribeEndpointWithContext(ctx, &input)
	if err != nil {
		return "", fmt.Errorf("failed to get config name of endpoint '%s': %v", endpointName, err)
	}
	return *res.EndpointConfigName, nil
}

func (smc SMClient) GetProductionVariantName(ctx context.Context, endpointName string) (string, error) {
	input := sagemaker.DescribeEndpointInput{
		EndpointName: aws.String(endpointName),
	}
	res, err := smc.metadataClient.DescribeEndpointWithContext(ctx, &input)
	if err != nil {
		return "", fmt.Errorf("failed to get production variant name of endpoint %s: %v", endpointName, err)
	}
	// we always have a single production variant behind an endpoint
	if len(res.ProductionVariants) != 1 {
		return "", fmt.Errorf("found %d production variants, expected 1", len(res.ProductionVariants))
	}
	return *res.ProductionVariants[0].VariantName, nil
}

func (smc SMClient) GetEndpointStatus(ctx context.Context, endpointName string) (string, error) {
	input := sagemaker.DescribeEndpointInput{
		EndpointName: aws.String(endpointName),
	}
	output, err := smc.metadataClient.DescribeEndpointWithContext(ctx, &input)
	if err != nil {
		return "", fmt.Errorf("failed to get endpoint status: %v", err)
	}
	return aws.StringValue(output.EndpointStatus), nil
}

func (smc SMClient) DeleteModel(ctx context.Context, modelName string) error {
	input := sagemaker.DeleteModelInput{
		ModelName: aws.String(modelName),
	}
	_, err := smc.metadataClient.DeleteModelWithContext(ctx, &input)
	if err != nil {
		return fmt.Errorf("failed to delete model: %v", err)
	}
	return nil
}

func (smc SMClient) DeleteEndpointConfig(ctx context.Context, endpointConfigName string) error {
	input := sagemaker.DeleteEndpointConfigInput{
		EndpointConfigName: aws.String(endpointConfigName),
	}
	_, err := smc.metadataClient.DeleteEndpointConfigWithContext(ctx, &input)
	if err != nil {
		return fmt.Errorf("failed to delete endpoint config: %v", err)
	}
	return nil
}

func (smc SMClient) DeleteEndpoint(ctx context.Context, endpointName string) error {
	input := sagemaker.DeleteEndpointInput{
		EndpointName: aws.String(endpointName),
	}
	_, err := smc.metadataClient.DeleteEndpointWithContext(ctx, &input)
	if err != nil {
		return fmt.Errorf("failed to delete endpoint: %v", err)
	}
	// Wait for endpoint to be deleted -- this should take no longer than a few
	// seconds.
	exists := true
	for exists {
		var err error
		exists, err = smc.EndpointExists(context.Background(), endpointName)
		if err != nil {
			return fmt.Errorf("failed to check if endpoint still exists: %v", err)
		}
		if exists {
			log.Printf("Waiting for endpoint [%s] to be deleted", endpointName)
			time.Sleep(time.Second)
		}
	}
	return nil
}

func getImage(framework, version, region string) (string, error) {
	url, ok := imageURIs[region][framework][version]
	if !ok {
		return "", fmt.Errorf("could not find image")
	}
	return url, nil
}

func (smc SMClient) CreateEndpointConfig(ctx context.Context, endpointCfg lib.SagemakerEndpointConfig) error {
	var endpointCfgInput sagemaker.CreateEndpointConfigInput
	if endpointCfg.InstanceCount > 0 {
		endpointCfgInput = sagemaker.CreateEndpointConfigInput{
			EndpointConfigName: aws.String(endpointCfg.Name),
			ProductionVariants: []*sagemaker.ProductionVariant{
				{
					ModelName:            aws.String(endpointCfg.ModelName),
					VariantName:          aws.String(endpointCfg.VariantName),
					InstanceType:         aws.String(endpointCfg.InstanceType),
					InitialInstanceCount: aws.Int64(int64(endpointCfg.InstanceCount)),
				},
			},
		}
	} else {
		endpointCfgInput = sagemaker.CreateEndpointConfigInput{
			EndpointConfigName: aws.String(endpointCfg.Name),
			ProductionVariants: []*sagemaker.ProductionVariant{
				{
					ModelName:   aws.String(endpointCfg.ModelName),
					VariantName: aws.String(endpointCfg.VariantName),
					ServerlessConfig: &sagemaker.ProductionVariantServerlessConfig{
						MaxConcurrency: aws.Int64(int64(endpointCfg.ServerlessMaxConcurrency)),
						MemorySizeInMB: aws.Int64(int64(endpointCfg.ServerlessMemory)),
					},
				},
			},
		}
	}
	_, err := smc.metadataClient.CreateEndpointConfigWithContext(ctx, &endpointCfgInput)
	if err != nil {
		return fmt.Errorf("failed to create endpoint config on sagemaker: %v", err)
	}
	return nil
}

func (smc SMClient) CreateEndpoint(ctx context.Context, endpoint lib.SagemakerEndpoint) error {
	endpointInput := sagemaker.CreateEndpointInput{
		EndpointName:       aws.String(endpoint.Name),
		EndpointConfigName: aws.String(endpoint.EndpointConfigName),
	}
	_, err := smc.metadataClient.CreateEndpointWithContext(ctx, &endpointInput)
	if err != nil {
		return fmt.Errorf("failed to create endpoint on sagemaker: %v", err)
	}
	return nil
}

func (smc SMClient) CreateServerlessEndpoint(ctx context.Context, endpoint lib.SagemakerEndpoint) error {
	endpointInput := sagemaker.CreateEndpointInput{
		EndpointName:       aws.String(endpoint.Name),
		EndpointConfigName: aws.String(endpoint.EndpointConfigName),
	}
	_, err := smc.metadataClient.CreateEndpointWithContext(ctx, &endpointInput)
	if err != nil {
		return fmt.Errorf("failed to create endpoint on sagemaker: %v", err)
	}
	return nil
}

func (smc SMClient) UpdateEndpoint(ctx context.Context, endpoint lib.SagemakerEndpoint) error {
	endpointInput := sagemaker.UpdateEndpointInput{
		EndpointName:       aws.String(endpoint.Name),
		EndpointConfigName: aws.String(endpoint.EndpointConfigName),
	}
	_, err := smc.metadataClient.UpdateEndpointWithContext(ctx, &endpointInput)
	if err != nil {
		return fmt.Errorf("failed to update endpoint on sagemaker: %v", err)
	}
	return nil
}

func (smc SMClient) EnableAutoscaling(ctx context.Context, sagemakerEndpointName string, modelVariantName string, scalingConfig lib.ScalingConfiguration) error {
	resourceId := aws.String(fmt.Sprintf("endpoint/%s/variant/%s", sagemakerEndpointName, modelVariantName))
	if scalingConfig.MaxCapacity < scalingConfig.MinCapacity || scalingConfig.MinCapacity <= 0 || scalingConfig.MaxCapacity <= 0 {
		return fmt.Errorf("MinCapacity and MaxCapacity should have non-zero, positive values with MaxCapacity >= MinCapacity. Given: %d (min) and %d (max)", scalingConfig.MinCapacity, scalingConfig.MaxCapacity)
	}
	req := applicationautoscaling.RegisterScalableTargetInput{
		ServiceNamespace:  aws.String(serviceNamespace),
		ResourceId:        resourceId,
		ScalableDimension: aws.String(scalableDimInstanceCount),
		MinCapacity:       aws.Int64(scalingConfig.MinCapacity),
		MaxCapacity:       aws.Int64(scalingConfig.MaxCapacity),
	}
	_, err := smc.autoscalerClient.RegisterScalableTargetWithContext(ctx, &req)
	if err != nil {
		smc.logger.Error("failed to register model variant for autoscaling", zap.String("endpoint", sagemakerEndpointName), zap.String("variant", modelVariantName), zap.Error(err))
		return fmt.Errorf("failed to register model variant for autoscaling: %w", err)
	}
	// define and apply
	scalingPolicy, err := cpuUtilizationScalingPolicy(sagemakerEndpointName, modelVariantName, scalingConfig.Cpu)
	if err != nil {
		return err
	}
	// TODO: source of truth for these policies should be a DB table and we should ideally have an update path.
	// configuring the policy with "sagemakerEndpointName" and "modelVariantName" makes it uniquely identifiable now
	// and allows migrating this to be stored in DB later
	scalingReq := applicationautoscaling.PutScalingPolicyInput{
		PolicyName:                               aws.String(fmt.Sprintf("CpuScalingPolicy-%s-%s", sagemakerEndpointName, modelVariantName)),
		ServiceNamespace:                         aws.String(serviceNamespace),
		ResourceId:                               resourceId,
		ScalableDimension:                        aws.String(scalableDimInstanceCount),
		PolicyType:                               aws.String(scalablePolicyType),
		TargetTrackingScalingPolicyConfiguration: &scalingPolicy,
	}
	out, err := smc.autoscalerClient.PutScalingPolicyWithContext(ctx, &scalingReq)
	if err != nil {
		smc.logger.Error("failed to apply scaling policy to the register scalable target", zap.String("endpoint", sagemakerEndpointName), zap.String("variant", modelVariantName), zap.Error(err))
		return fmt.Errorf("failed to apply scaling policy for autoscaling: %w", err)
	}
	smc.logger.Info("successfully applied scaling policies for model variant", zap.String("endpoint", sagemakerEndpointName), zap.String("variant", modelVariantName), zap.String("policyArn", *out.PolicyARN))
	return nil
}

func (smc SMClient) DisableAutoscaling(ctx context.Context, sagemakerEndpointName string, modelVariantName string) error {
	found, err := smc.IsAutoscalingConfigured(ctx, sagemakerEndpointName, modelVariantName)
	if err != nil {
		return fmt.Errorf("could not determine if autoscaling is configured: %w", err)
	}
	if !found {
		smc.logger.Info("autoscaling is not configured, skipping disabling it", zap.String("endpoint", sagemakerEndpointName), zap.String("variant", modelVariantName))
		return nil
	}
	resourceId := aws.String(fmt.Sprintf("endpoint/%s/variant/%s", sagemakerEndpointName, modelVariantName))
	req := applicationautoscaling.DeregisterScalableTargetInput{
		ServiceNamespace:  aws.String(serviceNamespace),
		ResourceId:        resourceId,
		ScalableDimension: aws.String(scalableDimInstanceCount),
	}
	_, err = smc.autoscalerClient.DeregisterScalableTargetWithContext(ctx, &req)
	if err != nil {
		smc.logger.Error("failed to disable autoscaling for endpoint and model variant", zap.String("endpoint", sagemakerEndpointName), zap.String("variant", modelVariantName), zap.Error(err))
		return fmt.Errorf("failed to disable autoscaling policy for endpoint: %s, variant: %s: %w", sagemakerEndpointName, modelVariantName, err)
	}
	smc.logger.Info("successfully disabled autoscaling for endpoint and model variant", zap.String("endpoint", sagemakerEndpointName), zap.String("variant", modelVariantName))
	return nil
}

func (smc SMClient) IsAutoscalingConfigured(ctx context.Context, sagemakerEndpointName string, modelVariantName string) (bool, error) {
	resourceId := aws.String(fmt.Sprintf("endpoint/%s/variant/%s", sagemakerEndpointName, modelVariantName))
	req := applicationautoscaling.DescribeScalableTargetsInput{
		ServiceNamespace:  aws.String(serviceNamespace),
		ResourceIds:       []*string{resourceId},
		ScalableDimension: aws.String(scalableDimInstanceCount),
	}
	out, err := smc.autoscalerClient.DescribeScalableTargetsWithContext(ctx, &req)
	if err != nil {
		return false, err
	}
	// we should find exactly one scalable target for the variant
	if len(out.ScalableTargets) == 1 {
		return true, nil
	}
	// if no scalable target is registered
	if len(out.ScalableTargets) == 0 {
		return false, nil
	}
	// this should never happen
	return false, fmt.Errorf("found %d scalable targets for endpoint: %s, variant: %s. Expected 1", len(out.ScalableTargets), sagemakerEndpointName, modelVariantName)
}

func (smc SMClient) Score(ctx context.Context, in *lib.ScoreRequest) (*lib.ScoreResponse, error) {
	adapter, err := smc.getAdapter(in.Framework)
	if err != nil {
		return nil, fmt.Errorf("could not get adapter: %v", err)
	}
	return adapter.Score(ctx, in)
}

func cpuUtilizationScalingPolicy(sagemakerEndpointName string, modelVariantName string, cpu lib.CpuScalingPolicy) (applicationautoscaling.TargetTrackingScalingPolicyConfiguration, error) {
	if cpu.CpuTargetValue <= 0 {
		return applicationautoscaling.TargetTrackingScalingPolicyConfiguration{}, fmt.Errorf("CpuTargetValue should non-zero positive value, given: %v", cpu.CpuTargetValue)
	}
	return applicationautoscaling.TargetTrackingScalingPolicyConfiguration{
		TargetValue: aws.Float64(cpu.CpuTargetValue),
		CustomizedMetricSpecification: &applicationautoscaling.CustomizedMetricSpecification{
			MetricName: aws.String("CPUUtilization"),
			Namespace:  aws.String("/aws/sagemaker/Endpoints"),
			Dimensions: []*applicationautoscaling.MetricDimension{
				{
					Name:  aws.String("EndpointName"),
					Value: aws.String(sagemakerEndpointName),
				},
				{
					Name:  aws.String("VariantName"),
					Value: aws.String(modelVariantName),
				},
			},
			Statistic: aws.String("Average"),
			Unit:      aws.String("Percent"),
		},
		// TODO: tune this according to the load patterns. May be these should be dynamic configurations as well
		// if not set, default value of 300 seconds is used

		// time to wait b/w two consecutive scale-in operations
		ScaleInCooldown: aws.Int64(cpu.ScaleInCoolDownPeriod),
		// time to wait b/w two consecutive scale-out operations
		ScaleOutCooldown: aws.Int64(cpu.ScaleOutCoolDownPeriod),
	}, nil
}
