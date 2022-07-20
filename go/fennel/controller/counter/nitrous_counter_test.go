package counter

import (
	"context"
	"testing"
	"time"

	"fennel/engine/ast"
	libaggregate "fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/test"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestNitrousBatchValue(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()

	wait := func() {
		count := 0
		for count < 3 {
			// Assuming that nitrous tails the log every 1s in tests.
			time.Sleep(1 * time.Second)
			lag, err := tier.NitrousClient.MustGet().GetLag(ctx)
			assert.NoError(t, err)
			tier.Logger.Info("Lag", zap.Uint64("value", lag))
			if lag == 0 {
				count++
			}
		}
	}

	aggs := []libaggregate.Aggregate{{
		Name:      "mycounter",
		Query:     ast.MakeInt(1),
		Timestamp: 0,
		Options: libaggregate.Options{
			AggType:   "sum",
			Durations: []uint32{14 * 24 * 3600, 24 * 3600},
		},
		Id: 1,
	}, {
		Name:      "mycounter2",
		Query:     ast.MakeInt(1),
		Timestamp: 0,
		Options: libaggregate.Options{
			AggType:   "average",
			Durations: []uint32{14 * 24 * 3600, 24 * 3600},
		},
		Id: 2,
	}}

	for _, agg := range aggs {
		err := tier.NitrousClient.MustGet().CreateAggregate(ctx, agg.Id, agg.Options)
		assert.NoError(t, err)
	}
	// Wait for nitrous to finish consuming from binlog.
	wait()

	aggIds := []ftypes.AggId{aggs[0].Id, aggs[1].Id}
	aggOptions := []libaggregate.Options{aggs[0].Options, aggs[1].Options}
	key := value.Int(0)
	keys := []value.Value{key, key}
	kwargs := []value.Dict{
		value.NewDict(map[string]value.Value{"duration": value.Int(14 * 3600 * 24)}),
		value.NewDict(map[string]value.Value{"duration": value.Int(14 * 3600 * 24)}),
	}

	// initially should find nothing
	exp1, exp2 := value.Int(0), value.Double(0)
	found, err := NitrousBatchValue(ctx, tier, aggIds, aggOptions, keys, kwargs)
	assert.NoError(t, err)
	assert.True(t, exp1.Equal(found[0]))
	assert.True(t, exp2.Equal(found[1]))

	// now update with actions
	table := value.NewList()
	// create an event every minute for past 2 days and next 1 day.
	start := time.Now().Add(-2 * 24 * time.Hour).Unix()
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(start + int64(i*60+30))
		row := value.NewDict(map[string]value.Value{
			"timestamp": value.Int(ts),
			"groupkey":  key,
			"value":     value.Int(1),
		})
		table.Append(row)
	}
	err = Update(ctx, tier, aggs[0].Id, aggs[0].Options, table)
	assert.NoError(t, err)
	err = Update(ctx, tier, aggs[1].Id, aggs[1].Options, table)
	assert.NoError(t, err)
	// Wait for nitrous to finish consuming from binlog.
	wait()

	// should find this time
	exp1, exp2 = value.Int(60*48), value.Double(1.0)
	found, err = NitrousBatchValue(ctx, tier, aggIds, aggOptions, keys, kwargs)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(found))
	for {
		found, err = NitrousBatchValue(ctx, tier, aggIds, aggOptions, keys, kwargs)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(found))
		if found[0].(value.Int) < exp1 {
			t.Logf("%d < %d", found[0], exp1)
			time.Sleep(1 * time.Second)
			continue
		}
		if found[1].(value.Double) < exp2 {
			t.Logf("%s < %f", found[1].String(), exp2)
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}

	// now check with duration of 1 day
	kwargs[0] = value.NewDict(map[string]value.Value{"duration": value.Int(24 * 3600)})
	kwargs[1] = value.NewDict(map[string]value.Value{"duration": value.Int(24 * 3600)})
	exp1, exp2 = value.Int(60*24), value.Double(1.0)
	found, err = NitrousBatchValue(ctx, tier, aggIds, aggOptions, keys, kwargs)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, found[0], exp1)
	assert.GreaterOrEqual(t, found[1], exp2)

	// not specifying a duration in kwargs should return an error
	kwargs[1] = value.NewDict(nil)
	_, err = NitrousBatchValue(ctx, tier, aggIds, aggOptions, keys, kwargs)
	assert.Error(t, err)

	// specifying a duration that wasn't registered should also return an error
	kwargs[1] = value.NewDict(map[string]value.Value{"duration": value.Int(7 * 24 * 3600)})
	_, err = NitrousBatchValue(ctx, tier, aggIds, aggOptions, keys, kwargs)
	assert.Error(t, err)
}
