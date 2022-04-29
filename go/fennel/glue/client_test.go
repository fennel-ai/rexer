//go:build glue

package glue

import (
	"fennel/engine/ast"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func listIntersectionCount(a, b []string) int {
	count := 0
	hash := make(map[string]bool)
	for _, x := range a {
		hash[x] = true
	}
	for _, x := range b {
		if exist, ok := hash[x]; exist && ok {
			count++
			hash[x] = false
		}
	}
	return count
}

func TestGlueClient(t *testing.T) {
	glueArgs := GlueArgs{Region: "us-west-2"}
	glueClient := NewGlueClient(glueArgs)
	t0 := ftypes.Timestamp(0)

	agg := aggregate.Aggregate{
		Name:      "OfflineAggregateTest",
		Query:     ast.MakeInt(0),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:      "topk",
			Durations:    []uint64{7 * 24 * 3600, 3 * 24 * 3600},
			CronSchedule: "37 1 * * ?",
		},
		Id: 1,
	}

	err := glueClient.ScheduleOfflineAggregate(107, agg)
	assert.NoError(t, err)

	aggs, err := glueClient.getAllOfflineAggregates()
	assert.NoError(t, err)

	expectedAggregates := []string{"OfflineAggregateTest::604800", "OfflineAggregateTest::259200"}
	// Could find both aggregates
	assert.Equal(t, 2, listIntersectionCount(expectedAggregates, aggs))

	err = glueClient.DeactivateOfflineAggregate(string(agg.Name))
	assert.NoError(t, err)
}

func TestHyperParameters(t *testing.T) {
	glueArgs := GlueArgs{Region: "us-west-2"}
	glueClient := NewGlueClient(glueArgs)
	t0 := ftypes.Timestamp(0)

	agg := aggregate.Aggregate{
		Name:      "OfflineAggregateTest",
		Query:     ast.MakeInt(0),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:         "cf",
			Durations:       []uint64{7 * 24 * 3600, 3 * 24 * 3600},
			CronSchedule:    "37 1 * * ?",
			HyperParameters: `{"rand": 123.5}`,
		},
		Id: 1,
	}

	err := glueClient.ScheduleOfflineAggregate(107, agg)
	assert.Error(t, err)
	assert.Equal(t, "aggregate type: cf, doesnt support hyperparameter rand", err.Error())

	agg = aggregate.Aggregate{
		Name:      "OfflineAggregateTest",
		Query:     ast.MakeInt(0),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:         "cf",
			Durations:       []uint64{7 * 24 * 3600, 3 * 24 * 3600},
			CronSchedule:    "37 1 * * ?",
			HyperParameters: `{"min_co_occurence": 123.5}`,
		},
		Id: 1,
	}

	err = glueClient.ScheduleOfflineAggregate(107, agg)
	assert.Error(t, err)
	assert.Equal(t, "aggregate type: cf, hyperparameter min_co_occurence must be type : int", err.Error())

	var registry = &supportedHyperParameters
	(*registry)["test"] = map[string]HyperParameterInfo{
		"a": HyperParameterInfo{3, reflect.Int, []string{}},
		"b": HyperParameterInfo{4.5, reflect.Float64, []string{}},
		"c": HyperParameterInfo{"sqrt", reflect.String, []string{"none", "log", "sqrt"}},
		"d": HyperParameterInfo{"blah", reflect.String, []string{}},
	}

	_, err = getHyperParameters("test", `{"min_co_occurence": 123.5}`)
	assert.Error(t, err)
	assert.Equal(t, "aggregate type: test, doesnt support hyperparameter min_co_occurence", err.Error())

	_, err = getHyperParameters("test", `{"a": 123.5}`)
	assert.Error(t, err)
	assert.Equal(t, "aggregate type: test, hyperparameter a must be type : int", err.Error())

	h, err := getHyperParameters("test", `{"a": 123}`)
	assert.NoError(t, err)
	assert.Equal(t, `{"a":123,"b":4.5,"c":"sqrt","d":"blah"}`, h)

	_, err = getHyperParameters("test", `{"c": "fasdf"}`)
	assert.Error(t, err)
	assert.Equal(t, "aggregate type: test, hyperparameter c must be one of [none log sqrt]", err.Error())

	_, err = getHyperParameters("test", `{"d": 123.5}`)
	assert.Error(t, err)
	assert.Equal(t, "aggregate type: test, hyperparameter d must be type : string", err.Error())

	h, err = getHyperParameters("test", `{"c": "none", "d": "xyz"}`)
	assert.NoError(t, err)
	assert.Equal(t, `{"a":3,"b":4.5,"c":"none","d":"xyz"}`, h)

	_, err = getHyperParameters("test", `{"a":43, "b": "sqrt"}`)
	assert.Error(t, err)
	assert.Equal(t, "aggregate type: test, hyperparameter b must be type : float64", err.Error())

	_, err = getHyperParameters("test", `{"a":43, "b": "sqrt"}`)
	assert.Error(t, err)
	assert.Equal(t, "aggregate type: test, hyperparameter b must be type : float64", err.Error())

	h, err = getHyperParameters("test", `{"a":43, "b": 12.5}`)
	assert.NoError(t, err)
	assert.Equal(t, `{"a":43,"b":12.5,"c":"sqrt","d":"blah"}`, h)

	_, err = getHyperParameters("test", `{"a":"qrt", "b": 12.5}`)
	assert.Error(t, err)
	assert.Equal(t, `aggregate type: test, hyperparameter a must be type : int`, err.Error())

	_, err = getHyperParameters("test", `{"c": 12.5}`)
	assert.Error(t, err)
	assert.Equal(t, `aggregate type: test, hyperparameter c must be one of [none log sqrt]`, err.Error())

	_, err = getHyperParameters("test", `{"c": "sqrt", "a": "sqrt"}`)
	assert.Error(t, err)
	assert.Equal(t, `aggregate type: test, hyperparameter a must be type : int`, err.Error())

	h, err = getHyperParameters("test", `{"d" : "qwe", "c": "sqrt", "a": 1, "b": 2.5}`)
	assert.NoError(t, err)
	assert.Equal(t, `{"a":1,"b":2.5,"c":"sqrt","d":"qwe"}`, h)

	agg = aggregate.Aggregate{
		Name:      "OfflineAggregateTest",
		Query:     ast.MakeInt(0),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:         "cf",
			Durations:       []uint64{7 * 24 * 3600, 3 * 24 * 3600},
			CronSchedule:    "37 1 * * ?",
			HyperParameters: `{"min_co_occurence": 123}`,
		},
		Id: 1,
	}

	err = glueClient.ScheduleOfflineAggregate(107, agg)
	assert.NoError(t, err)

	err = glueClient.DeactivateOfflineAggregate(string(agg.Name))
	assert.NoError(t, err)
}
