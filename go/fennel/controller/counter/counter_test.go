//go:build !badger

package counter

import (
	"context"
	"testing"
	"time"

	"fennel/engine/ast"
	"fennel/kafka"
	libaggregate "fennel/lib/aggregate"
	libcounter "fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/lib/utils/math"
	"fennel/lib/value"
	"fennel/model/aggregate"
	"fennel/model/counter"
	counter2 "fennel/model/counter"
	"fennel/test"
	"fennel/tier"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

// verifies that given a table created from a query, we do correct inserts/queries
func TestRolling(t *testing.T) {
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
	key := value.NewList(value.Int(1), value.Int(2))
	assert.NoError(t, aggregate.Store(ctx, tier, agg))
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
	err = Update(ctx, tier, agg, table, histogram)
	assert.NoError(t, err)
	assertDeltaLogged(t, ctx, tier, []libaggregate.Aggregate{agg}, 1 /*count=*/)

	clock := &test.FakeClock{}
	tier.Clock = clock
	clock.Set(int64(start + 24*3600*2))
	// at the end of 2 days, rolling counter should only be worth 28 hours, not full 48 hours
	found, err := Value(ctx, tier, agg.Id, key, histogram, value.NewDict(map[string]value.Value{"duration": value.Int(28 * 3600)}))
	assert.NoError(t, err)
	assert.Equal(t, value.Int(28*60), found)
	// with a duration of 1 day, rolling counter should only be worth 24 hours
	found, err = Value(
		ctx, tier, agg.Id, key, histogram, value.NewDict(map[string]value.Value{"duration": value.Int(24 * 3600)}),
	)
	assert.NoError(t, err)
	assert.Equal(t, value.Int(24*60), found)
}

func TestTimeseries(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	ctx := context.Background()
	start := 24*3600*12 + 60
	agg := libaggregate.Aggregate{
		Name:      "mycounter",
		Query:     ast.MakeInt(1),
		Timestamp: 0,
		// at any time, we want data from last 9 hours
		Options: libaggregate.Options{
			AggType: "timeseries_counter",
			Window:  ftypes.Window_HOUR,
			Limit:   9,
		},
		Id: 1,
	}
	histogram := counter2.NewTimeseriesSum(ftypes.Window_HOUR, 9)

	assert.NoError(t, aggregate.Store(ctx, tier, agg))
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
	err = Update(ctx, tier, agg, table, histogram)
	assert.NoError(t, err)
	assertDeltaLogged(t, ctx, tier, []libaggregate.Aggregate{agg}, 1 /*count=*/)

	clock := &test.FakeClock{}
	tier.Clock = clock
	clock.Set(int64(start + 24*3600*2))
	// at the end of 2 days, we should get one data point each for 9 days
	f, err := Value(ctx, tier, agg.Id, key, histogram, value.NewDict(nil))
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
	clock.Set(int64(start + 6*3600))
	f, err = Value(ctx, tier, agg.Id, key, histogram, value.NewDict(nil))
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

	key := value.NewList(value.Int(1), value.Int(2))
	table := value.NewList()
	// create an event every minute for 2 days
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(start + i*60 + 30)
		row := value.NewDict(map[string]value.Value{
			"timestamp": value.Int(ts),
			"groupkey":  key,
			"value":     value.Int(i / (24 * 60)), // amount is zero for first day and one for the next day
		})
		table.Append(row)
	}
	histogram := counter2.NewAverage([]uint64{28 * 3600, 24 * 3600})
	err = Update(ctx, tier, agg, table, histogram)
	assert.NoError(t, err)
	assertDeltaLogged(t, ctx, tier, []libaggregate.Aggregate{agg}, 1 /*count=*/)

	clock := &test.FakeClock{}
	tier.Clock = clock
	clock.Set(int64(start + 24*3600*2))
	// at the end of 2 days, rolling average should only be worth 28 hours, not full 48 hours
	found, err := Value(ctx, tier, agg.Id, key, histogram, value.NewDict(map[string]value.Value{"duration": value.Int(28 * 3600)}))
	assert.NoError(t, err)
	expected := float64(24*60) / float64(28*60)
	assert.Equal(t, value.Double(expected), found)
	// with a duration of 1 day, rolling average should only be worth 24 hours
	found, err = Value(
		ctx, tier, agg.Id, key, histogram, value.NewDict(map[string]value.Value{"duration": value.Int(24 * 3600)}),
	)
	assert.NoError(t, err)
	expected = float64(24*60) / float64(24*60)
	assert.Equal(t, value.Double(expected), found)
}

func TestStream(t *testing.T) {
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

	key := value.NewList(value.String("user_follows"), value.Int(2))
	table := value.List{}
	expected := make([]value.Value, 0)
	expected2 := make([]value.Value, 0)
	// create an event every minute for 2 days
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(start + i*60 + 30)
		row := value.NewDict(map[string]value.Value{
			"timestamp": value.Int(ts),
			"groupkey":  key,
			"value":     value.Int(i),
		})
		table.Append(row)
		if i >= 20*60 {
			expected = append(expected, value.Int(i))
		}
		if i >= 24*60 {
			expected2 = append(expected2, value.Int(i))
		}
	}
	histogram := counter2.NewList([]uint64{28 * 3600, 24 * 3600})
	err = Update(ctx, tier, agg, table, histogram)
	assert.NoError(t, err)
	assertDeltaLogged(t, ctx, tier, []libaggregate.Aggregate{agg}, 1 /*count=*/)

	clock := &test.FakeClock{}
	tier.Clock = clock
	clock.Set(int64(start + 24*3600*2))
	// at the end of 2 days, stream should only be worth 28 hours, not full 48 hours
	found, err := Value(ctx, tier, agg.Id, key, histogram, value.NewDict(map[string]value.Value{"duration": value.Int(28 * 3600)}))
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, slice(found.(value.List)))
	// with a duration of 1 day, stream should only be worth 24 hours
	found, err = Value(
		ctx, tier, agg.Id, key, histogram, value.NewDict(map[string]value.Value{"duration": value.Int(24 * 3600)}),
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

	key := value.NewList(value.Int(2))
	table := value.List{}
	// create an event every minute for 2 days
	var num, den int64 = 0, 0
	var num2, den2 int64 = 0, 0
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(start + i*60 + 30)
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
	histogram := counter2.NewRate([]uint64{28 * 3600, 24 * 3600}, true)
	err = Update(ctx, tier, agg, table, histogram)
	assert.NoError(t, err)
	assertDeltaLogged(t, ctx, tier, []libaggregate.Aggregate{agg}, 1 /*count=*/)

	clock := &test.FakeClock{}
	tier.Clock = clock
	clock.Set(int64(start + 24*3600*2))
	// at the end of 2 days, rate should only be worth 28 hours, not full 48 hours
	found, err := Value(ctx, tier, agg.Id, key, histogram, value.NewDict(map[string]value.Value{"duration": value.Int(28 * 3600)}))
	assert.NoError(t, err)
	expected, err := math.Wilson(float64(num), float64(den), true)
	assert.NoError(t, err)
	assert.Equal(t, value.Double(expected), found)
	// with a duration of 1 day, rate should only be worth 24 hours
	found, err = Value(ctx, tier, agg.Id, key, histogram, value.NewDict(map[string]value.Value{"duration": value.Int(24 * 3600)}))
	assert.NoError(t, err)
	expected, err = math.Wilson(float64(num2), float64(den2), true)
	assert.NoError(t, err)
	assert.Equal(t, value.Double(expected), found)
}

func TestCounterUpdateInvalid(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
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
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	start := 0
	key := value.Int(0)
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

	aggs := []libaggregate.Aggregate{{
		Name:      "mycounter",
		Query:     ast.MakeInt(1),
		Timestamp: 0,
		Options: libaggregate.Options{
			AggType:   "sum",
			Durations: []uint64{3600 * 14, 3600 * 28},
		},
		Id: 1,
	}, {
		Name:      "mycounter2",
		Query:     ast.MakeInt(1),
		Timestamp: 0,
		Options: libaggregate.Options{
			AggType:   "sum",
			Durations: []uint64{3600 * 14, 3600 * 28},
		},
		Id: 2,
	}}
	aggIds := []ftypes.AggId{aggs[0].Id, aggs[1].Id}
	keys := []value.Value{value.Int(0), value.Int(0)}
	h1 := counter2.NewSum([]uint64{14 * 3600 * 24, 3600 * 24})
	h2 := counter2.NewAverage([]uint64{14 * 3600 * 24, 3600 * 24})
	kwargs := []value.Dict{
		value.NewDict(map[string]value.Value{"duration": value.Int(14 * 3600 * 24)}),
		value.NewDict(map[string]value.Value{"duration": value.Int(14 * 3600 * 24)})}
	// initially should find nothing
	exp1, exp2 := value.Int(0), value.Double(0)
	found, err := BatchValue(ctx, tier, aggIds, keys, []counter2.Histogram{h1, h2}, kwargs)
	assert.NoError(t, err)
	assert.True(t, exp1.Equal(found[0]))
	assert.True(t, exp2.Equal(found[1]))

	// now update with actions
	err = Update(ctx, tier, aggs[0], table, h1)
	assert.NoError(t, err)
	err = Update(ctx, tier, aggs[1], table, h2)
	assert.NoError(t, err)
	assertDeltaLogged(t, ctx, tier, aggs, 2 /*count=*/)

	// should find this time
	clock := &test.FakeClock{}
	tier.Clock = clock
	clock.Set(int64(start + 24*3600*2))

	exp1, exp2 = value.Int(60*48), value.Double(1.0)
	found, err = BatchValue(ctx, tier, aggIds, keys, []counter2.Histogram{h1, h2}, kwargs)
	assert.NoError(t, err)
	assert.True(t, exp1.Equal(found[0]))
	assert.True(t, exp2.Equal(found[1]))

	// now go forward 2 more days and check with duration of 1 day
	// should find nothing
	clock.Set(int64(start + 24*3600*4))
	kwargs[0] = value.NewDict(map[string]value.Value{"duration": value.Int(24 * 3600)})
	kwargs[1] = value.NewDict(map[string]value.Value{"duration": value.Int(24 * 3600)})
	exp1, exp2 = value.Int(0), value.Double(0.0)
	found, err = BatchValue(ctx, tier, aggIds, keys, []counter2.Histogram{h1, h2}, kwargs)
	assert.NoError(t, err)
	assert.True(t, exp1.Equal(found[0]))
	assert.True(t, exp2.Equal(found[1]))

	// not specifying a duration in kwargs should return an error
	kwargs[1] = value.NewDict(nil)
	_, err = BatchValue(ctx, tier, aggIds, keys, []counter2.Histogram{h1, h2}, kwargs)
	assert.Error(t, err)

	// specifying a duration that wasn't registered should also return an error
	kwargs[1] = value.NewDict(map[string]value.Value{"duration": value.Int(7 * 24 * 3600)})
	_, err = BatchValue(ctx, tier, aggIds, keys, []counter2.Histogram{h1, h2}, kwargs)
	assert.Error(t, err)
}

func TestDurations(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	aggId := ftypes.AggId(1)
	durations := []uint64{7 * 24 * 3600, 14 * 24 * 3600}
	h := counter2.NewSum(durations)

	// not specifying a duration in kwargs should return an error
	_, err = Value(ctx, tier, aggId, value.Int(0), h, value.NewDict(nil))
	assert.Error(t, err)
	// specifying a duration that wasn't registered should also return an error
	_, err = Value(ctx, tier, aggId, value.Int(0), h, value.NewDict(map[string]value.Value{"duration": value.Int(10 * 24 * 3600)}))
	assert.Error(t, err)
	// no error when using a registered duration
	_, err = Value(ctx, tier, aggId, value.Int(0), h, value.NewDict(map[string]value.Value{"duration": value.Int(7 * 24 * 3600)}))
	assert.NoError(t, err)
}

func TestDeltaLogged(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	tier2, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	defer test.Teardown(tier2)
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

	key := value.NewList(value.Int(1), value.Int(2))
	table := value.NewList()
	// create an event every minute for 2 days
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(start + i*60 + 30)
		row := value.NewDict(map[string]value.Value{
			"timestamp": value.Int(ts),
			"groupkey":  key,
			"value":     value.Int(i / (24 * 60)), // amount is zero for first day and one for the next day
		})
		table.Append(row)
	}
	histogram := counter2.NewAverage([]uint64{28 * 3600, 24 * 3600})
	err = Update(ctx, tier, agg, table, histogram)
	assert.NoError(t, err)
	a, err := readAggregateDelta(t, ctx, tier, 1 /*count=*/)
	assert.NoError(t, err)
	assert.Equal(t, a[0].AggId, agg.Id)

	// Assert that the buckets were written to the tier by querying for the value of the aggregate
	clock := &test.FakeClock{}
	tier.Clock = clock
	clock.Set(int64(start + 24*3600*2))
	// at the end of 2 days, rolling average should only be worth 28 hours, not full 48 hours
	found, err := Value(ctx, tier, agg.Id, key, histogram, value.NewDict(map[string]value.Value{"duration": value.Int(28 * 3600)}))
	assert.NoError(t, err)
	expected := float64(24*60) / float64(28*60)
	assert.Equal(t, value.Double(expected), found)

	// Simulate applying deltas written in the kafka topic by applying them on a new tier and asserting the value of the aggregate at the end is the same
	tier2.Clock = clock
	assert.NoError(t, counter.Update(ctx, tier2, agg.Id, a[0].Buckets, histogram))
	// at the end of 2 days, rolling average should only be worth 28 hours, not full 48 hours
	found2, err := Value(ctx, tier2, agg.Id, key, histogram, value.NewDict(map[string]value.Value{"duration": value.Int(28 * 3600)}))
	assert.NoError(t, err)
	assert.Equal(t, found, found2)
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
			Durations: []uint64{3600 * 14, 3600 * 28},
		},
		Id: 1,
	}
	assert.Error(t, Update(ctx, tier, agg, table, counter2.NewSum([]uint64{123})))
}

// assertDeltaLogged asserts that aggregate delta was logged to the kafka queue
func assertDeltaLogged(t *testing.T, ctx context.Context, tr tier.Tier, aggs []libaggregate.Aggregate, count int) {
	actual, err := readAggregateDelta(t, ctx, tr, count)
	assert.NoError(t, err)
	for i, agg := range actual {
		assert.Equal(t, actual[i].AggId, agg.AggId)
		assert.Equal(t, actual[i].Options, agg.Options)
	}
}

func readAggregateDelta(t *testing.T, ctx context.Context, tr tier.Tier, count int) ([]libcounter.AggregateDelta, error) {
	consumer, err := tr.NewKafkaConsumer(kafka.ConsumerConfig{
		Topic:        libcounter.AGGREGATE_DELTA_TOPIC_NAME,
		GroupID:      utils.RandString(6),
		OffsetPolicy: kafka.DefaultOffsetPolicy,
	})
	if err != nil {
		return nil, err
	}
	msg, err := consumer.ReadBatch(ctx, count, 5*time.Second)
	if err != nil {
		return nil, err
	}
	aggs := make([]libcounter.AggregateDelta, count)
	for i := 0; i < count; i++ {
		pa := msg[0]
		var p libcounter.ProtoAggregateDelta
		err = proto.Unmarshal(pa, &p)
		if err != nil {
			return nil, err
		}
		aggs[i], err = libcounter.FromProtoAggregateDelta(&p)
		if err != nil {
			return nil, err
		}
	}
	return aggs, nil
}
