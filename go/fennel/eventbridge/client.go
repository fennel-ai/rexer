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

func GetRuleName(tierId ftypes.RealmID, name string) string {
	return fmt.Sprintf("%d-%s", tierId, name)
}

func (c Client) CreateRule(tierId ftypes.RealmID, ruleName string, scheduleExpression string) error {
	input := &eventbridge.PutRuleInput{
		Name:               aws.String(GetRuleName(tierId, ruleName)),
		ScheduleExpression: aws.String(scheduleExpression),
	}
	_, err := c.client.PutRule(input)
	return err
}

func (c Client) DeleteRule(tierId ftypes.RealmID, ruleName string) error {
	input := &eventbridge.DeleteRuleInput{
		Name: aws.String(GetRuleName(tierId, ruleName)),
	}
	_, err := c.client.DeleteRule(input)
	return err
}

func (c Client) CreateSageMakeRecurringJob(tierId ftypes.RealmID, name, pipelineArn, roleArn string, params map[string]string) error {
	target := createSageMakePipelineTarget(tierId, name, pipelineArn, roleArn, params)
	return c.CreateTarget(tierId, name, []*eventbridge.Target{&target})
}

func (c Client) CreateTarget(tierId ftypes.RealmID, ruleName string, targets []*eventbridge.Target) error {
	input := &eventbridge.PutTargetsInput{
		Rule:    aws.String(GetRuleName(tierId, ruleName)),
		Targets: targets,
	}
	_, err := c.client.PutTargets(input)
	return err
}

// ---------------------------------------------------------------------------------------------------------------------
// Private Helper Functions
// ---------------------------------------------------------------------------------------------------------------------

func createSageMakePipelineTarget(tierId ftypes.RealmID, name, pipelineArn, roleArn string, params map[string]string) eventbridge.Target {
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
		RoleArn:                     aws.String(roleArn),
		Id:                          aws.String(fmt.Sprintf("%d-%s", tierId, name)),
		SageMakerPipelineParameters: &smParams,
		RetryPolicy: &eventbridge.RetryPolicy{
			MaximumRetryAttempts: aws.Int64(3),
		},
	}
}
