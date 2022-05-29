//go:build glue

package glue

import (
	"fennel/engine/ast"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"github.com/stretchr/testify/assert"
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
	glueArgs := GlueArgs{Region: "us-west-2", JobNameByAgg: map[string]string{"cf": "CF", "topk": "TopK"}}
	glueClient := NewGlueClient(glueArgs)
	t0 := ftypes.Timestamp(0)

	agg := aggregate.Aggregate{
		Name:      "OfflineAggTest",
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

	expectedAggregates := []string{"OfflineAggTest::604800", "OfflineAggTest::259200"}
	// Could find both aggregates
	assert.Equal(t, 2, listIntersectionCount(expectedAggregates, aggs))

	err = glueClient.DeactivateOfflineAggregate(string(agg.Name))
	assert.NoError(t, err)
}

func TestHyperParameters(t *testing.T) {
	glueArgs := GlueArgs{Region: "us-west-2", JobNameByAgg: map[string]string{"cf": "CF", "topk": "TopK"}}
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
