//go:build badger

package tailer

import (
	"context"
	"testing"
	"time"

	"fennel/controller/counter"
	"fennel/engine/ast"
	libaggregate "fennel/lib/aggregate"
	"fennel/lib/badger"
	libcounter "fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	modelcounter "fennel/model/counter"
	counterkv "fennel/model/counter/kv"
	"fennel/test"

	db "github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAggregateDeltaSet(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	start := 0
	clock := &test.FakeClock{}
	tier.Clock = clock
	clock.Set(int64(start + 3600))

	ctx := context.Background()
	// Start a consumer to read the aggr delta log and write to kv store.
	closeCh := make(chan struct{})
	writeAggrDeltasToLocalKvStore(tier, closeCh)

	agg := libaggregate.Aggregate{
		Name:      "mycounter",
		Query:     ast.MakeInt(1),
		Timestamp: 0,
		Options: libaggregate.Options{
			AggType:   "sum",
			Durations: []uint64{3600},
		},
		Id: 1,
	}
	histogram := modelcounter.NewSum([]uint64{3600})
	b := libcounter.Bucket{
		Key:    "\"user\"",
		Window: ftypes.Window_MINUTE, // Minute
		Width:  6,                    // 6 minute
		Index:  0,                    // there will be 10 valid indices
		Value:  value.Int(10),
	}

	// Write aggregate delta to kafka.
	p := tier.Producers[libcounter.AGGREGATE_DELTA_TOPIC_NAME]
	pad, err := libcounter.ToProtoAggregateDelta(agg.Id, agg.Options, []libcounter.Bucket{b})
	require.NoError(t, err)
	err = p.LogProto(ctx, &pad, nil)
	require.NoError(t, err)

	// Wait for the delta to be written to kv store.
	var v value.Value
	for {
		time.Sleep(time.Second * 1)
		v, err = counter.Value(ctx, tier, 1, value.String("user"), histogram, value.NewDict(map[string]value.Value{"duration": value.Int(3600)}))
		require.NoError(t, err)
		if v != histogram.Zero() {
			break
		}
	}
	require.Equal(t, value.Int(10), v)

	// Close the channel to stop the consumer.
	close(closeCh)

	// Set the value to a different value directly in the store.
	bnew := b
	bnew.Value = value.Int(20)
	err = tier.Badger.Update(func(txn *db.Txn) error {
		writer := badger.NewTransactionalStore(tier, txn)
		return counterkv.Set(ctx, tier, []ftypes.AggId{agg.Id}, [][]libcounter.Bucket{{bnew}}, writer)
	})
	require.NoError(t, err)

	// Also log a new aggregate delta buckets to kafka.
	badd := b
	badd.Value = value.Int(3)
	newDelta, err := libcounter.ToProtoAggregateDelta(agg.Id, agg.Options, []libcounter.Bucket{badd})
	require.NoError(t, err)
	err = p.LogProto(ctx, &newDelta, nil)
	require.NoError(t, err)

	// Now start a new consumer to read the new delta from kafka and write to kv store.
	closeCh = make(chan struct{})
	writeAggrDeltasToLocalKvStore(tier, closeCh)

	// Wait till `badd`` is written to kv store.
	for {
		time.Sleep(time.Second * 1)
		v, err = counter.Value(ctx, tier, 1, value.String("user"), histogram, value.NewDict(map[string]value.Value{"duration": value.Int(3600)}))
		require.NoError(t, err)
		if v != value.Int(20) {
			break
		}
	}
	require.Equal(t, value.Int(23), v)

	// Close the channel to stop the consumer.
	close(closeCh)

	// Now, read the first update from the kv store. It should be same as `bnew` + `badd`
	// and not `bnew` + `badd` + `b`, which validates that the consumer did not process the earlier
	// message in kafka.
	got, err := counter.Value(ctx, tier, 1, value.String("user"), histogram, value.NewDict(map[string]value.Value{"duration": value.Int(3600)}))
	require.NoError(t, err)
	require.Equal(t, got, value.Int(23))
}
