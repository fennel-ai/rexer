package glue

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glue"
)

type GlueArgs struct {
	Region string `arg:"--region,env:AWS_REGION,help:AWS region"`
}

type GlueClient struct {
	client *glue.Glue
}

func NewGlueClient(args GlueArgs) GlueClient {
	sess := session.Must(session.NewSession(
		&aws.Config{
			Region:                        aws.String(args.Region),
			CredentialsChainVerboseErrors: aws.Bool(true),
		},
	))
	client := glue.New(sess)
	return GlueClient{
		client: client,
	}
}

func getGlueJobCommand(name string, scriptLocation string) *glue.JobCommand {
	jobCommand := &glue.JobCommand{
		Name:           aws.String("glueetl"),
		ScriptLocation: aws.String(scriptLocation),
		PythonVersion:  aws.String("3"),
	}

	return jobCommand
}

func (c GlueClient) CreateJob(jobName, jobType, jobCommand string) error {
	input := glue.CreateJobInput{
		Name:    aws.String(jobName),
		Command: getGlueJobCommand("DSf", "dsfsd"),
	}
	_, err := c.client.CreateJob(&input)
	return err
}

func getGlueTriggerActions(jobName string, arguments map[string]*string) []*glue.Action {
	actions := []*glue.Action{
		{
			JobName:   aws.String(jobName),
			Arguments: arguments,
		},
	}
	return actions
}

func (c GlueClient) CreateTrigger(triggerName, triggerType, triggerState string, triggerSchedule string) error {
	input := glue.CreateTriggerInput{
		Name:            aws.String(triggerName),
		Actions:         getGlueTriggerActions("Df", nil),
		Type:            aws.String(triggerType),
		Schedule:        aws.String(triggerSchedule),
		StartOnCreation: aws.Bool(true),
	}
	_, err := c.client.CreateTrigger(&input)
	return err
}
