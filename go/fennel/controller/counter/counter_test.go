package counter

import (
	"context"
	"fmt"
	"testing"
	"time"

	clock2 "github.com/raulk/clock"

	"fennel/engine/ast"
	libaggregate "fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/utils/math"
	"fennel/lib/value"
	"fennel/test"
	"fennel/test/nitrous"
	"fennel/tier"

	"github.com/stretchr/testify/assert"
)

// verifies that given a table created from a query, we do correct inserts/queries
func TestRolling(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	clock := tier.Clock.(*clock2.Mock)

	ctx := context.Background()
	t0 := clock.Now()
	t1 := t0.Add((24*3600*12 + 60*30) * time.Second)
	agg := libaggregate.Aggregate{
		Name:      "mycounter",
		Query:     ast.MakeInt(1),
		Timestamp: 0,
		Mode:      "rql",
		Options: libaggregate.Options{
			AggType:   "sum",
			Durations: []uint32{3600 * 24, 3600 * 28},
		},
		Id: 1,
	}
	err := tier.NitrousClient.CreateAggregate(ctx, agg.Id, agg.Options)
	assert.NoError(t, err)

	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)

	key := value.NewList(value.Int(1), value.Int(2))
	table := value.NewList()
	// create an event every minute for 2 days
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(int(t1.Unix()) + i*60 + 30)
		row := value.NewDict(map[string]value.Value{
			"timestamp": value.Int(ts),
			"groupkey":  key,
			"value":     value.Int(1),
		})
		table.Append(row)
	}
	err = Update(ctx, tier, agg.Id, table)
	assert.NoError(t, err)

	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)

	clock.Set(t1)
	clock.Add(24 * 3600 * 2 * time.Second)
	// at the end of 2 days, rolling counter should only be worth 28 hours, not full 48 hours
	found, err := Value(ctx, tier, agg.Id, key, value.NewDict(map[string]value.Value{"duration": value.Int(28 * 3600)}))
	assert.NoError(t, err)
	assert.Equal(t, value.Int(28*60), found)
	// with a duration of 1 day, rolling counter should only be worth 24 hours
	found, err = Value(
		ctx, tier, agg.Id, key, value.NewDict(map[string]value.Value{"duration": value.Int(24 * 3600)}),
	)
	assert.NoError(t, err)
	assert.Equal(t, value.Int(24*60), found)
}

func TestTimeseries(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	clock := tier.Clock.(*clock2.Mock)

	ctx := context.Background()
	start := 24*3600*12 + 60
	opts := libaggregate.Options{
		AggType: libaggregate.TIMESERIES_SUM,
		Window:  ftypes.Window_HOUR,
		Limit:   9,
	}
	agg := libaggregate.Aggregate{
		Id:        1,
		Name:      "mycounter",
		Mode:      "rql",
		Query:     ast.MakeInt(1),
		Timestamp: 0,
		// at any time, we want data from last 9 hours
		Options: opts,
	}
	err := tier.NitrousClient.CreateAggregate(ctx, agg.Id, agg.Options)
	assert.NoError(t, err)

	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)

	key := value.NewList(value.Int(1), value.Int(2))
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
	err = Update(ctx, tier, agg.Id, table)
	assert.NoError(t, err)

	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)

	t0 := clock.Now()
	clock.Set(t0.Add(time.Duration(start) * time.Second))
	t1 := clock.Now()
	clock.Set(t1.Add(24 * 3600 * 2 * time.Second))
	// at the end of 2 days, we should get one data point each for 9 days
	f, err := Value(ctx, tier, agg.Id, key, value.NewDict(nil))
	assert.NoError(t, err)
	found, ok := f.(value.List)
	assert.True(t, ok)

	//assert.Len(t, found, 9)
	assert.Equal(t, 9, found.Len())
	for i := 0; i < found.Len(); i++ {
		e, err := found.At(i)
		assert.NoError(t, err)
		assert.Equal(t, value.Int(60), e)
	}

	// but if we set time to just at 6 hours from start, we will still 9 entries, but few will be zero padded
	// and since our start time is 1 min delayed, the 4th entry will be one short of 60
	clock.Set(t1.Add(6 * 3600 * time.Second))
	f, err = Value(ctx, tier, agg.Id, key, value.NewDict(nil))
	assert.NoError(t, err)
	found, ok = f.(value.List)
	assert.True(t, ok)
	//assert.Len(t, found, 9)
	assert.Equal(t, 9, found.Len())
	for i := 0; i < found.Len(); i++ {
		e, err := found.At(i)
		assert.NoError(t, err)
		//for i := range found {
		if i < 3 {
			assert.Equal(t, value.Int(0), e)
		} else if i == 3 {
			assert.Equal(t, value.Int(59), e)
		} else {
			assert.Equal(t, value.Int(60), e)
		}
	}
}

func TestRollingAverage(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	clock := tier.Clock.(*clock2.Mock)

	ctx := context.Background()
	t0 := clock.Now()
	t1 := t0.Add((24*3600*12 + 60*30) * time.Second)
	agg := libaggregate.Aggregate{
		Name:      "mycounter",
		Query:     ast.MakeInt(1),
		Timestamp: 0,
		Options: libaggregate.Options{
			AggType:   "average",
			Durations: []uint32{28 * 3600, 24 * 3600},
		},
		Id: 1,
	}
	err := tier.NitrousClient.CreateAggregate(ctx, agg.Id, agg.Options)
	assert.NoError(t, err)

	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)

	key := value.NewList(value.Int(1), value.Int(2))
	table := value.NewList()
	// create an event every minute for 2 days
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(int(t1.Unix()) + i*60 + 30)
		row := value.NewDict(map[string]value.Value{
			"timestamp": value.Int(ts),
			"groupkey":  key,
			"value":     value.Int(i / (24 * 60)), // amount is zero for first day and one for the next day
		})
		table.Append(row)
	}
	err = Update(ctx, tier, agg.Id, table)
	assert.NoError(t, err)

	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)

	clock.Set(t1)
	clock.Add(24 * 3600 * 2 * time.Second)
	// at the end of 2 days, rolling average should only be worth 28 hours, not full 48 hours
	found, err := Value(ctx, tier, agg.Id, key, value.NewDict(map[string]value.Value{"duration": value.Int(28 * 3600)}))
	assert.NoError(t, err)
	expected := float64(24*60) / float64(28*60)
	assert.Equal(t, value.Double(expected), found)
	// with a duration of 1 day, rolling average should only be worth 24 hours
	found, err = Value(
		ctx, tier, agg.Id, key, value.NewDict(map[string]value.Value{"duration": value.Int(24 * 3600)}),
	)
	assert.NoError(t, err)
	expected = float64(24*60) / float64(24*60)
	assert.Equal(t, value.Double(expected), found)
}

func TestStream(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	clock := tier.Clock.(*clock2.Mock)

	ctx := context.Background()
	t0 := clock.Now()
	t1 := t0.Add((24*3600*12) * time.Second)
	agg := libaggregate.Aggregate{
		Name:      "mycounter",
		Query:     ast.MakeInt(1),
		Timestamp: 0,
		Options: libaggregate.Options{
			AggType:   "list",
			Durations: []uint32{28 * 3600, 24 * 3600},
		},
		Id: 1,
	}
	err := tier.NitrousClient.CreateAggregate(ctx, agg.Id, agg.Options)
	assert.NoError(t, err)

	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)

	key := value.NewList(value.String("user_follows"), value.Int(2))
	table := value.List{}
	expected := make([]value.Value, 0)
	expected2 := make([]value.Value, 0)
	// create an event every minute for 2 days
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(int(t1.Unix()) + i*60)
		row := value.NewDict(map[string]value.Value{
			"timestamp": value.Int(ts),
			"groupkey":  key,
			"value":     value.Int(i),
		})
		table.Append(row)

		// need to computed based on the buckets they would fall into
		if i >= 20*60 {
			expected = append(expected, value.Int(i))
		}
		if i >= 24*60 {
			expected2 = append(expected2, value.Int(i))
		}
	}
	err = Update(ctx, tier, agg.Id, table)
	assert.NoError(t, err)

	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)

	clock.Set(t1)
	clock.Add(24 * 3600 * 2 * time.Second)
	// at the end of 2 days, stream should only be worth 28 hours, not full 48 hours
	found, err := Value(ctx, tier, agg.Id, key, value.NewDict(map[string]value.Value{"duration": value.Int(28 * 3600)}))
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, slice(found.(value.List)))
	// with a duration of 1 day, stream should only be worth 24 hours
	found, err = Value(
		ctx, tier, agg.Id, key, value.NewDict(map[string]value.Value{"duration": value.Int(24 * 3600)}),
	)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected2, slice(found.(value.List)))
}

func slice(l value.List) []value.Value {
	elems := make([]value.Value, l.Len())
	for i := 0; i < l.Len(); i++ {
		elems[i], _ = l.At(i)
	}
	return elems
}

func TestRate(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	clock := tier.Clock.(*clock2.Mock)

	ctx := context.Background()
	t0 := clock.Now()
	t1 := t0.Add((24*3600*12) * time.Second)
	agg := libaggregate.Aggregate{
		Name:      "mycounter",
		Query:     ast.MakeInt(1),
		Timestamp: 0,
		Options: libaggregate.Options{
			AggType:   "rate",
			Durations: []uint32{28 * 3600, 24 * 3600},
			Normalize: true,
		},
		Id: 1,
	}
	err := tier.NitrousClient.CreateAggregate(ctx, agg.Id, agg.Options)
	assert.NoError(t, err)

	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)

	key := value.NewList(value.Int(2))
	table := value.List{}
	// create an event every minute for 2 days
	var num, den int64 = 0, 0
	var num2, den2 int64 = 0, 0
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(int(t1.Unix()) + i*60)
		row := value.NewDict(map[string]value.Value{
			"timestamp": value.Int(ts),
			"groupkey":  key,
			"value":     value.NewList(value.Int(i), value.Int(i+1)),
		})
		table.Append(row)
		if i >= 20*60 {
			num += int64(i)
			den += int64(i + 1)
		}
		if i >= 24*60 {
			num2 += int64(i)
			den2 += int64(i + 1)
		}
	}
	err = Update(ctx, tier, agg.Id, table)
	assert.NoError(t, err)

	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)

	clock.Set(t1)
	clock.Add(24 * 3600 * 2 * time.Second)
	// at the end of 2 days, rate should only be worth 28 hours, not full 48 hours
	found, err := Value(ctx, tier, agg.Id, key, value.NewDict(map[string]value.Value{"duration": value.Int(28 * 3600)}))
	assert.NoError(t, err)
	expected, err := math.Wilson(float64(num), float64(den), true)
	assert.NoError(t, err)
	assert.Equal(t, value.Double(expected), found)
	// with a duration of 1 day, rate should only be worth 24 hours
	found, err = Value(ctx, tier, agg.Id, key, value.NewDict(map[string]value.Value{"duration": value.Int(24 * 3600)}))
	assert.NoError(t, err)
	expected, err = math.Wilson(float64(num2), float64(den2), true)
	assert.NoError(t, err)
	assert.Equal(t, value.Double(expected), found)
}

func TestCounterUpdateInvalid(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()
	// no col for key or timestamp
	assertInvalid(tier, ctx, t, value.NewDict(map[string]value.Value{"hi": value.Int(1)}), value.NewDict(map[string]value.Value{"hi": value.Int(3)}))
	// no col for key
	assertInvalid(tier, ctx, t, value.NewDict(map[string]value.Value{"timestamp": value.Int(1)}), value.NewDict(map[string]value.Value{"timestamp": value.Int(3)}))
	// timestamp is not int
	assertInvalid(tier, ctx, t,
		value.NewDict(map[string]value.Value{"timestamp": value.Double(1), "key": value.NewList(value.Int(1))}),
		value.NewDict(map[string]value.Value{"timestamp": value.Double(3), "key": value.NewList(value.Int(3))}),
	)
}

func TestBatchValue(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()
	clock := tier.Clock.(*clock2.Mock)

	// set some non-zero time so that on the first `BatchValue` call, during bucketizer, the timestamp used is not ZERO
	clock.Set(time.Now())
	t0 := clock.Now()

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

	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)

	aggIds := []ftypes.AggId{aggs[0].Id, aggs[1].Id}
	keys := []value.Value{value.Int(0), value.Int(0)}
	kwargs := []value.Dict{
		value.NewDict(map[string]value.Value{"duration": value.Int(14 * 3600 * 24)}),
		value.NewDict(map[string]value.Value{"duration": value.Int(14 * 3600 * 24)})}
	// initially should find nothing
	exp1, exp2 := value.Int(0), value.Double(0)
	found, err := BatchValue(ctx, tier, aggIds, keys, kwargs)
	assert.NoError(t, err)
	assert.True(t, exp1.Equal(found[0]))
	assert.True(t, exp2.Equal(found[1]))

	// now update with actions
	key := value.Int(0)
	table := value.NewList()
	// create an event every minute for 2 days
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(int(t0.Unix()) + i*60 + 30)
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

	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)

	// should find this time
	clock.Set(t0.Add(24 * 3600 * 2 * time.Second))

	exp1, exp2 = value.Int(60*48), value.Double(1.0)
	found, err = BatchValue(ctx, tier, aggIds, keys, kwargs)
	assert.NoError(t, err)
	fmt.Printf("found: %v\n", found)
	assert.True(t, exp1.Equal(found[0]))
	assert.True(t, exp2.Equal(found[1]))

	// now go forward 2 more days and check with duration of 1 day
	// should find nothing
	clock.Set(t0.Add(24 * 3600 * 4 * time.Second))
	kwargs[0] = value.NewDict(map[string]value.Value{"duration": value.Int(24 * 3600)})
	kwargs[1] = value.NewDict(map[string]value.Value{"duration": value.Int(24 * 3600)})
	exp1, exp2 = value.Int(0), value.Double(0.0)
	found, err = BatchValue(ctx, tier, aggIds, keys, kwargs)
	assert.NoError(t, err)
	assert.True(t, exp1.Equal(found[0]))
	assert.True(t, exp2.Equal(found[1]))

	// not specifying a duration in kwargs should return an error
	kwargs[1] = value.NewDict(nil)
	_, err = BatchValue(ctx, tier, aggIds, keys, kwargs)
	assert.Error(t, err)

	// specifying a duration that wasn't registered should also return an error
	kwargs[1] = value.NewDict(map[string]value.Value{"duration": value.Int(7 * 24 * 3600)})
	_, err = BatchValue(ctx, tier, aggIds, keys, kwargs)
	assert.Error(t, err)
}

func TestDurations(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	clock := tier.Clock.(*clock2.Mock)
	// set a non-zero time so that when `Value` is called in the bucketizer logic, it does not crash
	clock.Set(time.Now())
	ctx := context.Background()
	agg := libaggregate.Aggregate{
		Name:      "mycounter",
		Query:     ast.MakeInt(1),
		Timestamp: 0,
		Options: libaggregate.Options{
			AggType:   "rate",
			Durations: []uint32{7 * 24 * 3600, 14 * 24 * 3600},
			Normalize: true,
		},
		Id: 1,
	}
	err := tier.NitrousClient.CreateAggregate(ctx, agg.Id, agg.Options)
	assert.NoError(t, err)
	nitrous.WaitForMessagesToBeConsumed(t, ctx, tier.NitrousClient)

	// not specifying a duration in kwargs should return an error
	_, err = Value(ctx, tier, agg.Id, value.Int(0), value.NewDict(nil))
	assert.Error(t, err)
	// specifying a duration that wasn't registered should also return an error
	_, err = Value(ctx, tier, agg.Id, value.Int(0), value.NewDict(map[string]value.Value{"duration": value.Int(10 * 24 * 3600)}))
	assert.Error(t, err)
	// no error when using a registered duration
	_, err = Value(ctx, tier, agg.Id, value.Int(0), value.NewDict(map[string]value.Value{"duration": value.Int(7 * 24 * 3600)}))
	assert.NoError(t, err)
}

func assertInvalid(tier tier.Tier, ctx context.Context, t *testing.T, ds ...value.Dict) {
	table := value.List{}
	for _, d := range ds {
		table.Append(d)
	}
	agg := libaggregate.Aggregate{
		Name:      "mycounter",
		Query:     ast.MakeInt(1),
		Timestamp: 0,
		Options: libaggregate.Options{
			AggType:   "sum",
			Durations: []uint32{123},
		},
		Id: 1,
	}
	assert.Error(t, Update(ctx, tier, agg.Id, table))
}
