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
	"github.com/aws/aws-sdk-go/service/sagemaker"
	"github.com/aws/aws-sdk-go/service/sagemakerruntime"

	lib "fennel/lib/sagemaker"
)

type SagemakerArgs struct {
	Region                 string   `arg:"--region,env:AWS_REGION,help:AWS region"`
	SagemakerExecutionRole string   `arg:"--sagemaker-execution-role,env:SAGEMAKER_EXECUTION_ROLE,help:SageMaker execution role"`
	PrivateSubnets         []string `arg:"--private-subnets,env:PRIVATE_SUBNETS,help:Private subnets"`
	SagemakerSecurityGroup string   `arg:"--sagemaker-security-group,env:SAGEMAKER_SECURITY_GROUP,help:SageMaker security group"`
	SagemakerInstanceType  string   `arg:"--sagemaker-instance-type,env:SAGEMAKER_INSTANCE_TYPE,help:SageMaker instance type"`
	SagemakerInstanceCount uint     `arg:"--sagemaker-instance-count,env:SAGEMAKER_INSTANCE_COUNT,help:SageMaker instance count"`
}

func NewClient(args SagemakerArgs) (SMClient, error) {
	sess := session.Must(session.NewSession(
		&aws.Config{
			Region:                        aws.String(args.Region),
			CredentialsChainVerboseErrors: aws.Bool(true),
		},
	))
	runtime := sagemakerruntime.New(sess)
	metadata := sagemaker.New(sess)
	return SMClient{
		args:           args,
		runtimeClient:  runtime,
		metadataClient: metadata,
	}, nil
}

type SMClient struct {
	args           SagemakerArgs
	runtimeClient  *sagemakerruntime.SageMakerRuntime
	metadataClient *sagemaker.SageMaker
}

var _ lib.SagemakerRegistry = SMClient{}
var _ lib.InferenceServer = SMClient{}

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
			ContainerHostname: aws.String(lib.GetContainerName(model.Name, model.Version)),
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
	endpointCfgInput := sagemaker.CreateEndpointConfigInput{
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

func (smc SMClient) Score(ctx context.Context, in *lib.ScoreRequest) (*lib.ScoreResponse, error) {
	adapter, err := smc.getAdapter(in.Framework)
	if err != nil {
		return nil, fmt.Errorf("could not get adapter: %v", err)
	}
	return adapter.Score(ctx, in)
}
