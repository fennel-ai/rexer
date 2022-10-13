//go:build glue

package aggregate

import (
	"context"
	"testing"
	"time"

	"github.com/raulk/clock"

	"fennel/kafka"
	"fennel/lib/action"
	"fennel/lib/aggregate"
	libcounter "fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/resource"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

// TODO(mohit): Remove build tag `glue` dependency for the following test cases
func TestOfflineAggregates(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()

	ck := tier.Clock.(*clock.Mock)
	t1 := ftypes.Timestamp(456)
	ck.Add(time.Duration(t1) * time.Second)
	t0 := ftypes.Timestamp(0)

	agg := aggregate.Aggregate{
		Name:      "aggTest",
		Query:     getQuery(),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:      "cf",
			Durations:    []uint32{259200},
			CronSchedule: "37 */2 * * ?",
			Limit:        10,
		},
		Id: 1,
	}
	assert.NoError(t, Store(ctx, tier, agg))

	a1 := getAction(1, "3434", ftypes.Timestamp(1000), "like")
	a2 := getAction(2, "123", ftypes.Timestamp(1005), "share")
	a3 := getAction(1, "325235", ftypes.Timestamp(1000), "like")

	// when time is not specified we use the current time to populate it
	a1.Timestamp = t1

	err := Update(ctx, tier, []action.Action{a1, a2, a3}, agg)
	assert.NoError(t, err)

	// test that actions were written as JSON as well
	expectedJsonTable := []string{
		`{"aggregate":"aggTest","groupkey":[3434],"timestamp":456,"value":null}`,
		`{"aggregate":"aggTest","groupkey":[325235],"timestamp":1000,"value":null}`,
	}
	consumer, err := tier.NewKafkaConsumer(kafka.ConsumerConfig{
		Scope:        resource.NewTierScope(tier.ID),
		Topic:        libcounter.AGGREGATE_OFFLINE_TRANSFORM_TOPIC_NAME,
		GroupID:      utils.RandString(6),
		OffsetPolicy: kafka.DefaultOffsetPolicy,
	})
	assert.NoError(t, err)
	defer consumer.Close()
	data, err := consumer.ReadBatch(ctx, 2, 15*time.Second)
	assert.NoError(t, err)
	found := make([]string, 0, 2)
	for i := range data {
		assert.NoError(t, err)
		found = append(found, string(data[i]))
	}
	assert.Equal(t, expectedJsonTable, found)
	// TODO: Mock Glue Client
	err = tier.GlueClient.DeactivateOfflineAggregate(tier.ID, string(agg.Name))
	assert.NoError(t, err)
}
