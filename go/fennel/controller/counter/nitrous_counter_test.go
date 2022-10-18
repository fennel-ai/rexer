package counter

import (
	"context"
	"testing"
	"time"

	clock2 "github.com/raulk/clock"

	"fennel/engine/ast"
	libaggregate "fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/test"
	"fennel/test/nitrous"

	"github.com/stretchr/testify/assert"
)

func TestNitrousBatchValue(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()

	clock := tier.Clock.(*clock2.Mock)
	clock.Set(time.Now())

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
		err := tier.NitrousClient.CreateAggregate(ctx, agg.Id, agg.Options)
		assert.NoError(t, err)
	}
	// Wait for nitrous to finish consuming from binlog.
	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)

	aggIds := []ftypes.AggId{aggs[0].Id, aggs[1].Id}
	key := value.Int(0)
	keys := []value.Value{key, key}
	kwargs := []value.Dict{
		value.NewDict(map[string]value.Value{"duration": value.Int(14 * 3600 * 24)}),
		value.NewDict(map[string]value.Value{"duration": value.Int(14 * 3600 * 24)}),
	}

	// initially should find nothing
	exp1, exp2 := value.Int(0), value.Double(0)
	found, err := BatchValue(ctx, tier, aggIds, keys, kwargs)
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
	err = Update(ctx, tier, aggs[0].Id, table)
	assert.NoError(t, err)
	err = Update(ctx, tier, aggs[1].Id, table)
	assert.NoError(t, err)
	// Wait for nitrous to finish consuming from binlog.
	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)

	// should find this time
	exp1, exp2 = value.Int(60*48), value.Double(1.0)
	found, err = BatchValue(ctx, tier, aggIds, keys, kwargs)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(found))
	assert.GreaterOrEqual(t, found[0], exp1)
	assert.GreaterOrEqual(t, found[1], exp2)

	// now check with duration of 1 day
	kwargs[0] = value.NewDict(map[string]value.Value{"duration": value.Int(24 * 3600)})
	kwargs[1] = value.NewDict(map[string]value.Value{"duration": value.Int(24 * 3600)})
	exp1, exp2 = value.Int(60*24), value.Double(1.0)
	found, err = BatchValue(ctx, tier, aggIds, keys, kwargs)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, found[0], exp1)
	assert.GreaterOrEqual(t, found[1], exp2)

	// not specifying a duration in kwargs should return an error
	kwargs[1] = value.NewDict(nil)
	found, err = BatchValue(ctx, tier, aggIds, keys, kwargs)
	assert.Error(t, err)

	// specifying a duration that wasn't registered should also return an error
	kwargs[1] = value.NewDict(map[string]value.Value{"duration": value.Int(7 * 24 * 3600)})
	found, err = BatchValue(ctx, tier, aggIds, keys, kwargs)
	assert.Error(t, err)
}
