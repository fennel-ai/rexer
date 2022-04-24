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
	glueArgs := GlueArgs{Region: "ap-south-1"}
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
