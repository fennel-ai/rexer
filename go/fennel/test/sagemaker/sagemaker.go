package sagemaker

import (
	"os"

	lib "fennel/lib/sagemaker"
	db "fennel/model/sagemaker"
	"fennel/s3"
	"fennel/sagemaker"
	"fennel/tier"
)

func AddSagemakerClientToTier(tier *tier.Tier) error {
	// Set the environment variables to enable access the test sagemaker endpoint.
	os.Setenv("AWS_PROFILE", "admin")
	os.Setenv("AWS_SDK_LOAD_CONFIG", "1")
	c, err := sagemaker.NewClient(sagemaker.SagemakerArgs{
		Region:                 "ap-south-1",
		SagemakerExecutionRole: "arn:aws:iam::030813887342:role/service-role/AmazonSageMaker-ExecutionRole-20220315T123828",
	})
	if err != nil {
		return err
	}
	tier.SagemakerClient = c
	return nil
}

func AddSagemakerDataAndClientToTier(tier *tier.Tier) error {
	s3Args := s3.S3Args{Region: "ap-south-1"}
	tier.S3Client = s3.NewClient(s3Args)

	err := AddSagemakerClientToTier(tier)
	if err != nil {
		return err
	}

	m1 := lib.Model{
		Name:             "integration-test-xgboost-model",
		Version:          "v1",
		Framework:        "xgboost",
		FrameworkVersion: "1.3.1",
		ArtifactPath:     "s3://my-xgboost-test-bucket-2/model.tar.gz",
	}
	m2 := m1
	m2.Version = "v2"

	id1, err := db.InsertModel(*tier, m1)
	if err != nil {
		return err
	}
	id2, err := db.InsertModel(*tier, m2)
	if err != nil {
		return err
	}
	hm1 := lib.SagemakerHostedModel{
		SagemakerModelName: "integration-test-model",
		ModelId:            id1,
		ContainerHostname:  lib.GetContainerName(m1.Name, m1.Version),
	}
	hm2 := hm1
	hm2.ModelId, hm2.ContainerHostname = id2, lib.GetContainerName(m2.Name, m2.Name)
	err = db.InsertHostedModels(*tier, hm1, hm2)
	if err != nil {
		return err
	}
	endpointCfg := lib.SagemakerEndpointConfig{
		Name:          "integration-test-endpoint-config",
		VariantName:   "integration-test-model",
		ModelName:     "integration-test-model",
		InstanceType:  "ml.t2.medium",
		InstanceCount: 1,
	}
	err = db.InsertEndpointConfig(*tier, endpointCfg)
	if err != nil {
		return err
	}
	err = db.InsertEndpoint(*tier, lib.SagemakerEndpoint{
		Name:               "integration-test-endpoint",
		EndpointConfigName: "integration-test-endpoint-config",
	})
	return err
}
