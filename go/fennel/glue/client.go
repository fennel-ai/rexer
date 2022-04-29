package glue

import (
	"encoding/json"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glue"
)

var aggToJobName = map[string]string{
	"topk": "TopK",
	"cf":   "CF",
}

type HyperParameterInfo struct {
	Default interface{}  `json:"default"`
	Type    reflect.Kind `json:"type"`
	Options []string     `json:"options"`
}

type HyperParamRegistry = map[string]map[string]HyperParameterInfo

var supportedHyperParameters = HyperParamRegistry{
	"cf": {
		"min_co_occurence":          HyperParameterInfo{3, reflect.Int, []string{}},
		"object_normalization_func": HyperParameterInfo{"sqrt", reflect.String, []string{"none", "log", "sqrt"}},
	},
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

func getGlueTriggerActions(jobName string, arguments map[string]*string) []*glue.Action {
	actions := []*glue.Action{
		{
			JobName:   aws.String(jobName),
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

func contains(sl []string, name string) bool {
	for _, value := range sl {
		if value == name {
			return true
		}
	}
	return false
}

func getHyperParameters(aggregateType string, hyperparamters string) (string, error) {
	var aggParams map[string]json.RawMessage
	err := json.Unmarshal([]byte(hyperparamters), &aggParams)
	if err != nil {
		return "", fmt.Errorf("aggregate type: %v, failed to parse aggregate tuning params: %v", aggregateType, err)
	}

	if _, ok := supportedHyperParameters[aggregateType]; !ok {
		return "", fmt.Errorf("aggregate type: %v, doesnt support hyperparameters", aggregateType)
	}
	hyperparamtersMap := supportedHyperParameters[aggregateType]

	for param, value := range aggParams {

		if _, ok := hyperparamtersMap[param]; !ok {
			return "", fmt.Errorf("aggregate type: %v, doesnt support hyperparameter %v", aggregateType, param)
		}

		if len(hyperparamtersMap[param].Options) > 0 {
			var s string
			_ = json.Unmarshal(value, &s)
			if !contains(hyperparamtersMap[param].Options, s) {
				return "", fmt.Errorf("aggregate type: %v, hyperparameter %v must be one of %v", aggregateType, param, hyperparamtersMap[param].Options)
			}
			continue
		}

		s := string(value)

		if _, err := strconv.ParseInt(s, 10, 64); err == nil {
			if hyperparamtersMap[param].Type != reflect.Int {
				return "", fmt.Errorf("aggregate type: %v, hyperparameter %v must be type : %v", aggregateType, param, hyperparamtersMap[param].Type)
			}
			continue
		}

		if _, err = strconv.ParseFloat(s, 64); err == nil {
			if hyperparamtersMap[param].Type != reflect.Float64 {
				return "", fmt.Errorf("aggregate type: %v, hyperparameter %v must be type : %v", aggregateType, param, hyperparamtersMap[param].Type)
			}
			continue
		}

		if hyperparamtersMap[param].Type == reflect.Int || hyperparamtersMap[param].Type == reflect.Float64 {
			return "", fmt.Errorf("aggregate type: %v, hyperparameter %v must be type : %v", aggregateType, param, hyperparamtersMap[param].Type)
		}
	}

	var retParams map[string]interface{}
	_ = json.Unmarshal([]byte(hyperparamters), &retParams)

	for param := range hyperparamtersMap {
		if _, ok := retParams[param]; !ok {
			retParams[param] = hyperparamtersMap[param].Default
		}
	}

	str, err := json.Marshal(retParams)
	if err != nil {
		return "", fmt.Errorf("failed to marshal hyper params: %v", err)
	}
	return string(str), nil
}

func (c GlueClient) ScheduleOfflineAggregate(tierID ftypes.RealmID, agg aggregate.Aggregate) error {
	aggregateType := strings.ToLower(string(agg.Options.AggType))
	if _, ok := aggToJobName[aggregateType]; !ok {
		return fmt.Errorf("unknown offline aggregate type: %v", aggregateType)
	}

	jobArguments := map[string]*string{
		"--AGGREGATE_NAME": aws.String(string(agg.Name)),
		"--AGGREGATE_TYPE": aws.String(aggregateType),
		"--LIMIT":          aws.String(fmt.Sprintf("%d", agg.Options.Limit)),
	}

	if agg.Options.HyperParameters != "" {
		hyperparameters, err := getHyperParameters(aggregateType, agg.Options.HyperParameters)
		if err != nil {
			return err
		}
		jobArguments["--HYPERPARAMETERS"] = aws.String(hyperparameters)
	}

	// Create a trigger for every duration.
	for _, duration := range agg.Options.Durations {
		jobArguments["--DURATION"] = aws.String(fmt.Sprintf("%d", duration))

		err := c.CreateTrigger(string(agg.Name), aggregateType, agg.Options.CronSchedule, jobArguments)
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
