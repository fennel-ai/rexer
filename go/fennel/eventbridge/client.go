package eventbridge

import (
	"fennel/lib/ftypes"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/eventbridge"
)

type EventBridgeArgs struct {
	Region string `arg:"--region,env:AWS_REGION,help:AWS region"`
}

type Client struct {
	client *eventbridge.EventBridge
}

func NewClient(args EventBridgeArgs) Client {
	sess := session.Must(session.NewSession(
		&aws.Config{
			Region:                        aws.String(args.Region),
			CredentialsChainVerboseErrors: aws.Bool(true),
		},
	))
	client := eventbridge.New(sess)
	return Client{
		client: client,
	}
}

func (c Client) CreateRule(ruleName string, scheduleExpression string) error {
	input := &eventbridge.PutRuleInput{
		Name:               aws.String(ruleName),
		ScheduleExpression: aws.String(scheduleExpression),
	}
	_, err := c.client.PutRule(input)
	return err
}

func (c Client) DeleteRule(ruleName string) error {
	input := &eventbridge.DeleteRuleInput{
		Name: aws.String(ruleName),
	}
	_, err := c.client.DeleteRule(input)
	return err
}

func CreateSageMakePipelineTarget(tierId ftypes.RealmID, name, pipelineArn string, params map[string]string) eventbridge.Target {
	smParams := eventbridge.SageMakerPipelineParameters{}
	smParams.PipelineParameterList = make([]*eventbridge.SageMakerPipelineParameter, 0, len(params))
	for k, v := range params {
		smParams.PipelineParameterList = append(smParams.PipelineParameterList, &eventbridge.SageMakerPipelineParameter{
			Name:  aws.String(k),
			Value: aws.String(v),
		})
	}
	return eventbridge.Target{
		Arn:                         aws.String(pipelineArn),
		Id:                          aws.String(fmt.Sprintf("%s-%s", tierId, name)),
		SageMakerPipelineParameters: &smParams,
		RetryPolicy: &eventbridge.RetryPolicy{
			MaximumRetryAttempts: aws.Int64(3),
		},
	}
}

func (c Client) PutTargets(ruleName string, targets []*eventbridge.Target) error {
	input := &eventbridge.PutTargetsInput{
		Rule:    aws.String(ruleName),
		Targets: targets,
	}
	_, err := c.client.PutTargets(input)
	return err
}
