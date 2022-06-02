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
	"cf": {
		"min_co_occurence":          hp.HyperParameterInfo{3, reflect.Int, []string{}},
		"object_normalization_func": hp.HyperParameterInfo{"sqrt", reflect.String, []string{"none", "log", "sqrt"}},
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

func (c GlueClient) createTrigger(aggregateName, aggregateType, cronSchedule, jobName string, jobArguments map[string]*string) error {
	triggerName := fmt.Sprintf("%s::%s", aggregateName, *jobArguments["--DURATION"])
	input := glue.CreateTriggerInput{
		Name:            aws.String(triggerName),
		Actions:         getGlueTriggerActions(jobName, jobArguments),
		Type:            aws.String(glue.TriggerTypeScheduled),
		Schedule:        aws.String("cron(" + cronSchedule + " *)"),
		StartOnCreation: aws.Bool(true),
	}
	_, err := c.client.CreateTrigger(&input)
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

		err := c.createTrigger(string(agg.Name), aggregateType, agg.Options.CronSchedule, jobName, jobArguments)
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
