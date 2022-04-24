package glue

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glue"
)

var aggToScriptLocation = map[string]string{
	"topk": "s3://offline-aggregate-scripts/topk.py",
}

var aggToJobName = map[string]string{
	"topk": "TopK",
}

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

func getGlueJobCommand(scriptLocation string) *glue.JobCommand {
	jobCommand := &glue.JobCommand{
		Name:           aws.String("glueetl"),
		ScriptLocation: aws.String(scriptLocation),
		PythonVersion:  aws.String("3"),
	}

	return jobCommand
}

func (c GlueClient) CreateJob(jobName, aggregateType string) error {
	input := glue.CreateJobInput{
		Name:    aws.String(jobName),
		Command: getGlueJobCommand(aggToScriptLocation[aggregateType]),
	}
	_, err := c.client.CreateJob(&input)
	return err
}

func getGlueTriggerActions(aggregateName string, arguments map[string]*string) []*glue.Action {
	actions := []*glue.Action{
		{
			JobName:   aws.String(aggregateName),
			Arguments: arguments,
		},
	}
	return actions
}

func (c GlueClient) CreateTrigger(aggregateName, aggregateType, cronSchedule string, jobArguments map[string]*string) error {
	triggerName := fmt.Sprintf("%s::%s", aggregateName, *jobArguments["--DURATION"])
	input := glue.CreateTriggerInput{
		Name:            aws.String(triggerName),
		Actions:         getGlueTriggerActions(aggToJobName[aggregateType], jobArguments),
		Type:            aws.String(glue.TriggerTypeScheduled),
		Schedule:        aws.String("cron(" + cronSchedule + " *)"),
		StartOnCreation: aws.Bool(true),
	}
	_, err := c.client.CreateTrigger(&input)
	return err
}

func (c GlueClient) ScheduleOfflineAggregate(aggregateName string, aggregateType string, cronSchedule string, duration uint64) error {
	aggregateType = strings.ToLower(string(aggregateType))
	if _, ok := aggToJobName[aggregateType]; !ok {
		return fmt.Errorf("unknown offline aggregate type: %v", aggregateType)
	}

	params := map[string]string{"k": "1000"}
	jsonParams, _ := json.Marshal(params)
	jobArguments := map[string]*string{
		"--DURATION":       aws.String(fmt.Sprintf("%d", duration)),
		"--PLANE_ID":       aws.String(fmt.Sprintf("%d", 5)),
		"--TIER_ID":        aws.String(fmt.Sprintf("%d", 107)),
		"--AGGREGATE_NAME": aws.String(aggregateName),
		"--AGGREGATE_TYPE": aws.String(aggregateType),
		"--PARAMS":         aws.String(string(jsonParams)),
	}

	return c.CreateTrigger(aggregateName, aggregateType, cronSchedule, jobArguments)
}

func (c GlueClient) getAllOfflineAggregates() ([]string, error) {
	triggers, err := c.client.ListTriggers(&glue.ListTriggersInput{})
	if err != nil {
		return nil, err
	}
	offlineAggregates := make([]string, 0, len(triggers.TriggerNames))
	for _, trigger := range triggers.TriggerNames {
		offlineAggregates = append(offlineAggregates, *trigger)
	}
	return offlineAggregates, nil
}

func (c GlueClient) DeactivateOfflineAggregate(aggregateName string) error {
	triggers, err := c.getAllOfflineAggregates()
	if err != nil {
		return err
	}

	for _, trigger := range triggers {
		if strings.HasPrefix(trigger, aggregateName) {
			input := glue.DeleteTriggerInput{
				Name: aws.String(trigger),
			}
			_, err := c.client.DeleteTrigger(&input)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
