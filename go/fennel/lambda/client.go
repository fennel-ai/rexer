package lambda

import (
	"fennel/lib/ftypes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
)

type LambdaArgs struct {
	Region string `arg:"--region,env:AWS_REGION,help:AWS region"`
}

type Client struct {
	client *lambda.Lambda
}

func NewClient(args LambdaArgs) Client {
	sess := session.Must(session.NewSession(
		&aws.Config{
			Region:                        aws.String(args.Region),
			CredentialsChainVerboseErrors: aws.Bool(true),
		},
	))
	client := lambda.New(sess)
	return Client{
		client: client,
	}
}

func (c Client) Invoke(functionName string, payload []byte) ([]byte, error) {
	input := &lambda.InvokeInput{
		FunctionName: aws.String(functionName),
		Payload:      payload,
	}

	output, err := c.client.Invoke(input)
	if err != nil {
		return nil, err
	}

	return output.Payload, nil
}

func (c Client) CreateFunction(tierId ftypes.RealmID, functionName string) error {
	input := &lambda.CreateFunctionInput{
		Architectures: []*string{aws.String("x86_64")},
		Code: &lambda.FunctionCode{
			S3Bucket: aws.String("fennel-lambda"),
		},
		FunctionName: aws.String(fmt.Sprintf("%d-%s", tierId, functionName)),
		Handler:      aws.String("lambda_function.lambda_handler"),
		Runtime:      aws.String("Python 3.9"),
		Timeout:      aws.Int64(800),
		MemorySize:   aws.Int64(512),
	}

	_, err := c.client.CreateFunction(input)
	return err
}
