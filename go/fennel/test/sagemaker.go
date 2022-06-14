package test

import (
	"os"

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
	}, tier.Logger)
	if err != nil {
		return err
	}
	tier.SagemakerClient = c
	return nil
}
