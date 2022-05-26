//go:build badger

package aggregate_delta

import (
	"context"
	"fennel/controller/counter"
	"fennel/engine/ast"
	"fennel/kafka"
	libaggregate "fennel/lib/aggregate"
	libcounter "fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/lib/value"
	"fennel/model/aggregate"
	counter2 "fennel/model/counter"
	"fennel/test"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestTransferToDB(t *testing.T) {
	// Ingest actions so that aggregate deltas are written to the topic
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	start := 24*3600*12 + 60*30
	agg := libaggregate.Aggregate{
		Name:      "mycounter",
		Query:     ast.MakeInt(1),
		Timestamp: 0,
		Options: libaggregate.Options{
			AggType:   "sum",
			Durations: []uint64{3600 * 14, 3600 * 28},
		},
		Id: 1,
	}
	querySer, err := ast.Marshal(agg.Query)
	assert.NoError(t, err)
	optionSer, err := proto.Marshal(libaggregate.ToProtoOptions(agg.Options))
	assert.NoError(t, err)

	key := value.NewList(value.Int(1), value.Int(2))
	assert.NoError(t, aggregate.Store(ctx, tier, agg.Name, querySer, agg.Timestamp, optionSer))
	table := value.NewList()
	// create an event every minute for 2 days
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(start + i*60 + 30)
		row := value.NewDict(map[string]value.Value{
			"timestamp": value.Int(ts),
			"groupkey":  key,
			"value":     value.Int(1),
		})
		table.Append(row)
	}
	histogram := counter2.NewSum([]uint64{3600 * 28, 3600 * 24})

	// this should write the deltas to kafka log
	err = counter.Update(ctx, tier, agg, table, histogram)
	assert.NoError(t, err)

	// Consume from the topic and write to DB
	consumer, err := tier.NewKafkaConsumer(kafka.ConsumerConfig{
		Topic:        libcounter.AGGREGATE_DELTA_TOPIC_NAME,
		GroupID:      utils.RandString(6),
		OffsetPolicy: kafka.DefaultOffsetPolicy,
	})
	assert.NoError(t, err)
	assert.NoError(t, TransferAggrDeltasToDB(ctx, tier, consumer))

	// Assert fetching from storage
	clock := &test.FakeClock{}
	tier.Clock = clock
	clock.Set(int64(start + 24*3600*2))
	// at the end of 2 days, rolling average should only be worth 28 hours, not full 48 hours
	found, err := counter.Value(ctx, tier, agg.Id, key, histogram, value.NewDict(map[string]value.Value{"duration": value.Int(28 * 3600)}))
	assert.NoError(t, err)
	assert.Equal(t, value.Int(28*60), found)
}
