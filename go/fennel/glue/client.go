package glue

import (
	"encoding/json"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	hp "fennel/lib/hyperparam"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glue"
	"reflect"
	"strings"
)

var supportedHyperParameters = hp.HyperParamRegistry{
	"cf": map[string]hp.HyperParameterInfo{
		"min_co_occurence":          {Default: 3, Type: reflect.Int, Options: []string{}},
		"object_normalization_func": {Default: "sqrt", Type: reflect.String, Options: []string{"none", "log", "sqrt", "identity"}},
	},
}

type GlueArgs struct {
	Region string `arg:"--region,env:AWS_REGION,help:AWS region"`
	// these are passed as key1=value1 key2=value2
	JobNameByAgg map[string]string `arg:"--job-name-by-agg,env:JOB_NAME_BY_AGG,help:GLUE Job name by Agg name" json:"job_name_by_agg,omitempty"`
}

type GlueClient struct {
	client       *glue.Glue
	jobNameByAgg map[string]string
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
		client:       client,
		jobNameByAgg: args.JobNameByAgg,
	}
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

func getTriggerName(tierID ftypes.RealmID, aggregateName ftypes.AggName, duration string) string {
	return fmt.Sprintf("%d::%s::%s", tierID, aggregateName, duration)
}

func (c GlueClient) createTrigger(tierID ftypes.RealmID, aggregateName ftypes.AggName, cronSchedule, jobName string, jobArguments map[string]*string) error {
	input := glue.CreateTriggerInput{
		Name:            aws.String(getTriggerName(tierID, aggregateName, *jobArguments["--DURATION"])),
		Actions:         getGlueTriggerActions(jobName, jobArguments),
		Type:            aws.String(glue.TriggerTypeScheduled),
		Schedule:        aws.String("cron(" + cronSchedule + " *)"),
		StartOnCreation: aws.Bool(true),
	}
	_, err := c.client.CreateTrigger(&input)
	return err
}

func (c GlueClient) StartAggregate(tierID ftypes.RealmID, agg aggregate.Aggregate, duration int) error {
	aggregateType := strings.ToLower(string(agg.Options.AggType))
	jobArguments := map[string]*string{
		"--AGGREGATE_NAME": aws.String(string(agg.Name)),
		"--AGGREGATE_TYPE": aws.String(aggregateType),
		"--LIMIT":          aws.String(fmt.Sprintf("%d", agg.Options.Limit)),
	}

	if _, ok := supportedHyperParameters[aggregateType]; ok {
		hyperparameters, err := hp.GetHyperParameters(aggregateType, agg.Options.HyperParameters, supportedHyperParameters)
		if err != nil {
			return err
		}

		hyperparametersStr, err := json.Marshal(hyperparameters)

		if err != nil {
			return err
		}
		jobArguments["--HYPERPARAMETERS"] = aws.String(string(hyperparametersStr))
	}
	jobArguments["--DURATION"] = aws.String(fmt.Sprintf("%d", duration))
	input := glue.StartJobRunInput{
		JobName:         aws.String("t-" + fmt.Sprintf("%d", tierID) + "-" + aggregateType),
		Arguments:       jobArguments,
		NumberOfWorkers: aws.Int64(5),
		WorkerType:      aws.String("G.2X"),
	}
	_, err := c.client.StartJobRun(&input)
	return err
}

func (c GlueClient) ScheduleOfflineAggregate(tierID ftypes.RealmID, agg aggregate.Aggregate) error {
	aggregateType := strings.ToLower(string(agg.Options.AggType))
	jobName, ok := c.jobNameByAgg[aggregateType]
	if !ok {
		return fmt.Errorf("unknown offline aggregate type: %v", aggregateType)
	}

	jobArguments := map[string]*string{
		"--AGGREGATE_NAME": aws.String(string(agg.Name)),
		"--AGGREGATE_TYPE": aws.String(aggregateType),
		"--LIMIT":          aws.String(fmt.Sprintf("%d", agg.Options.Limit)),
	}

	if _, ok := supportedHyperParameters[aggregateType]; ok {
		hyperparameters, err := hp.GetHyperParameters(aggregateType, agg.Options.HyperParameters, supportedHyperParameters)
		if err != nil {
			return err
		}

		hyperparametersStr, err := json.Marshal(hyperparameters)

		if err != nil {
			return err
		}
		jobArguments["--HYPERPARAMETERS"] = aws.String(string(hyperparametersStr))
	}

	// Create a trigger for every duration.
	for _, duration := range agg.Options.Durations {
		jobArguments["--DURATION"] = aws.String(fmt.Sprintf("%d", duration))

		err := c.createTrigger(tierID, agg.Name, agg.Options.CronSchedule, jobName, jobArguments)
		if err != nil {
			return err
		}
	}
	return nil
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

func (c GlueClient) DeactivateOfflineAggregate(tierID ftypes.RealmID, aggregateName string) error {
	triggers, err := c.getAllOfflineAggregates()
	if err != nil {
		return err
	}

	prefix := fmt.Sprintf("%d::%s::", tierID, aggregateName)
	for _, trigger := range triggers {
		if strings.HasPrefix(trigger, prefix) {
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
