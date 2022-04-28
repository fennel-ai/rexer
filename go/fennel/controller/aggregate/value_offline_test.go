//go:build glue

package aggregate

import (
	"context"
	actionlib "fennel/controller/action"
	"fennel/kafka"
	"fennel/lib/action"
	"fennel/lib/aggregate"
	libcounter "fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/test"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TODO(mohit): Remove build tag `glue` dependency for the following test cases
func TestOfflineAggregates(t *testing.T) {
	tier, err := test.Tier()
	defer test.Teardown(tier)
	assert.NoError(t, err)
	ctx := context.Background()

	clock := test.FakeClock{}
	tier.Clock = &clock
	t1 := ftypes.Timestamp(456)
	clock.Set(int64(t1))
	t0 := ftypes.Timestamp(0)

	agg := aggregate.Aggregate{
		Name:      "aggTest",
		Query:     getQuery(),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:      "topk",
			Durations:    []uint64{259200},
			CronSchedule: "37 1 * * ?",
			Limit:        10,
		},
		Id: 1,
	}
	assert.NoError(t, Store(ctx, tier, agg))

	a1 := getAction(1, "3434", ftypes.Timestamp(1000), "like")
	a2 := getAction(2, "123", ftypes.Timestamp(1005), "share")
	a3 := getAction(1, "325235", ftypes.Timestamp(1000), "like")
	assert.NoError(t, actionlib.BatchInsert(ctx, tier, []action.Action{a1, a2, a3}))

	// when time is not specified we use the current time to populate it
	a1.Timestamp = t1

	wg := sync.WaitGroup{}

	wg.Add(2)
	go func() {
		defer wg.Done()
		consumer, err := tier.NewKafkaConsumer(kafka.ConsumerConfig{
			Topic:        action.ACTIONLOG_KAFKA_TOPIC,
			GroupID:      string(agg.Name),
			OffsetPolicy: kafka.DefaultOffsetPolicy,
		})
		assert.NoError(t, err)
		defer consumer.Close()
		err = Update(ctx, tier, consumer, agg)
		assert.NoError(t, err)
	}()

	expectedJsonTable := []string{
		`{"action_id":2,"action_type":"like","actor_id":3434,"actor_type":"user","aggregate":"agg","groupkey":[3434],"metadata":6,"request_id":7,"target_id":3,"target_type":"video","timestamp":1000}`,
		`{"action_id":2,"action_type":"like","actor_id":325235,"actor_type":"user","aggregate":"agg","groupkey":[325235],"metadata":6,"request_id":7,"target_id":3,"target_type":"video","timestamp":1000}`,
	}
	// test that actions were written as JSON as well
	go func() {
		defer wg.Done()
		consumer, err := tier.NewKafkaConsumer(kafka.ConsumerConfig{
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
	}()

	wg.Wait()

	// TODO: Mock Glue Client
	err = tier.GlueClient.DeactivateOfflineAggregate(string(agg.Name))
	assert.NoError(t, err)
}
