package counter

import (
	"context"
	"testing"

	"fennel/engine/ast"
	libaggregate "fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/utils/math"
	"fennel/lib/value"
	"fennel/model/aggregate"
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
	start := 24*3600*12 + 60*31
	agg := libaggregate.Aggregate{
		Name:      "mycounter",
		Query:     ast.MakeInt(1),
		Timestamp: 0,
		Options: libaggregate.Options{
			AggType:  "sum",
			Duration: 3600 * 28,
		},
	}
	querySer, err := ast.Marshal(agg.Query)
	assert.NoError(t, err)
	optionSer, err := proto.Marshal(libaggregate.ToProtoOptions(agg.Options))
	assert.NoError(t, err)

	key := value.List{value.Int(1), value.Int(2)}
	assert.NoError(t, aggregate.Store(ctx, tier, agg.Name, querySer, agg.Timestamp, optionSer))
	table := value.List{}
	// create an event every minute for 2 days
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(start + i*60 + 30)
		row := value.Dict{
			"timestamp": value.Int(ts),
			"groupkey":  key,
			"value":     value.Int(1),
		}
		assert.NoError(t, table.Append(row))
	}
	err = Update(ctx, tier, agg.Name, table, counter2.RollingCounter{})
	assert.NoError(t, err)

	clock := &test.FakeClock{}
	tier.Clock = clock
	clock.Set(int64(start + 24*3600*2))
	// at the end of 2 days, rolling counter should only be worth 28 hours, not full 48 hours
	found, err := Value(ctx, tier, agg.Name, key, counter2.RollingCounter{Duration: 28 * 3600})
	assert.NoError(t, err)
	assert.Equal(t, value.Int(28*60), found)
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
	}
	histogram := counter2.TimeseriesCounter{
		Window: ftypes.Window_HOUR,
		Limit:  9,
	}
	querySer, err := ast.Marshal(agg.Query)
	assert.NoError(t, err)
	optionSer, err := proto.Marshal(libaggregate.ToProtoOptions(agg.Options))
	assert.NoError(t, err)

	assert.NoError(t, aggregate.Store(ctx, tier, agg.Name, querySer, agg.Timestamp, optionSer))
	key := value.List{value.Int(1), value.Int(2)}
	table := value.List{}
	// create an event every minute for 2 days
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(start + i*60 + 30)
		row := value.Dict{
			"timestamp": value.Int(ts),
			"groupkey":  key,
			"value":     value.Int(1),
		}
		assert.NoError(t, table.Append(row))
	}
	err = Update(ctx, tier, agg.Name, table, histogram)
	assert.NoError(t, err)

	clock := &test.FakeClock{}
	tier.Clock = clock
	clock.Set(int64(start + 24*3600*2))
	// at the end of 2 days, we should get one data point each for 9 days
	f, err := Value(ctx, tier, agg.Name, key, histogram)
	assert.NoError(t, err)
	found, ok := f.(value.List)
	assert.True(t, ok)

	assert.Len(t, found, 9)
	for i := range found {
		assert.Equal(t, value.Int(60), found[i])
	}

	// but if we set time to just at 6 hours from start, we will still 9 entries, but few will be zero padded
	// and since our start time is 1 min delayed, the 4th entry will be one short of 60
	clock.Set(int64(start + 6*3600))
	f, err = Value(ctx, tier, agg.Name, key, histogram)
	assert.NoError(t, err)
	found, ok = f.(value.List)
	assert.True(t, ok)
	assert.Len(t, found, 9)
	for i := range found {
		if i < 3 {
			assert.Equal(t, value.Int(0), found[i])
		} else if i == 3 {
			assert.Equal(t, value.Int(59), found[i])
		} else {
			assert.Equal(t, value.Int(60), found[i])
		}
	}
}

func TestRollingAverage(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	ctx := context.Background()
	start := 24*3600*12 + 60*31
	aggname := ftypes.AggName("some counter")

	key := value.List{value.Int(1), value.Int(2)}
	//assert.NoError(t, aggregate.Store(tier, agg.Name, querySer, agg.Timestamp, optionSer))
	table := value.List{}
	// create an event every minute for 2 days
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(start + i*60 + 30)
		row := value.Dict{
			"timestamp": value.Int(ts),
			"groupkey":  key,
			"value":     value.Int(i / (24 * 60)), // amount is zero for first day and one for the next day
		}
		assert.NoError(t, table.Append(row))
	}
	histogram := counter2.RollingAverage{Duration: 28 * 3600}
	err = Update(ctx, tier, aggname, table, histogram)
	assert.NoError(t, err)

	clock := &test.FakeClock{}
	tier.Clock = clock
	clock.Set(int64(start + 24*3600*2))
	// at the end of 2 days, rolling average should only be worth 28 hours, not full 48 hours
	found, err := Value(ctx, tier, aggname, key, histogram)
	assert.NoError(t, err)
	expected := float64(24*60) / float64(28*60)
	assert.Equal(t, value.Double(expected), found)
}

func TestStream(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	ctx := context.Background()
	start := 24*3600*12 + 60*31
	aggname := ftypes.AggName("some stream")

	key := value.List{value.String("user_follows"), value.Int(2)}
	table := value.List{}
	expected := make([]value.Value, 0)
	// create an event every minute for 2 days
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(start + i*60 + 30)
		row := value.Dict{
			"timestamp": value.Int(ts),
			"groupkey":  key,
			"value":     value.Int(i),
		}
		assert.NoError(t, table.Append(row))
		if i >= 20*60 {
			expected = append(expected, value.Int(i))
		}
	}
	histogram := counter2.List{Duration: 28 * 3600}
	err = Update(ctx, tier, aggname, table, histogram)
	assert.NoError(t, err)

	clock := &test.FakeClock{}
	tier.Clock = clock
	clock.Set(int64(start + 24*3600*2))
	// at the end of 2 days, rolling average should only be worth 28 hours, not full 48 hours
	found, err := Value(ctx, tier, aggname, key, histogram)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, found.(value.List))
}

func TestRate(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	ctx := context.Background()
	start := 24*3600*12 + 60*31
	aggname := ftypes.AggName("some rate")

	key := value.List{value.String("user_follows"), value.Int(2)}
	table := value.List{}
	// create an event every minute for 2 days
	var num, den int64 = 0, 0
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(start + i*60 + 30)
		row := value.Dict{
			"timestamp": value.Int(ts),
			"groupkey":  key,
			"value":     value.List{value.Int(i), value.Int(i + 1)},
		}
		assert.NoError(t, table.Append(row))
		if i >= 20*60 {
			num += int64(i)
			den += int64(i + 1)
		}
	}
	histogram := counter2.Rate{Duration: 28 * 3600, Normalize: true}
	err = Update(ctx, tier, aggname, table, histogram)
	assert.NoError(t, err)

	clock := &test.FakeClock{}
	tier.Clock = clock
	clock.Set(int64(start + 24*3600*2))
	// at the end of 2 days, rolling average should only be worth 28 hours, not full 48 hours
	found, err := Value(ctx, tier, aggname, key, histogram)
	assert.NoError(t, err)
	expected, err := math.Wilson(uint64(num), uint64(den), true)
	assert.NoError(t, err)
	assert.Equal(t, value.Double(expected), found)
}

func TestCounterUpdateInvalid(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()
	// no col for key or timestamp
	assertInvalid(tier, ctx, t, value.Dict{"hi": value.Int(1)}, value.Dict{"hi": value.Int(3)})
	// no col for key
	assertInvalid(tier, ctx, t, value.Dict{"timestamp": value.Int(1)}, value.Dict{"timestamp": value.Int(3)})
	// timestamp is not int
	assertInvalid(tier, ctx, t,
		value.Dict{"timestamp": value.Double(1), "key": value.List{value.Int(1)}},
		value.Dict{"timestamp": value.Double(3), "key": value.List{value.Int(3)}},
	)
}

func assertInvalid(tier tier.Tier, ctx context.Context, t *testing.T, ds ...value.Dict) {
	table := value.List{}
	for _, d := range ds {
		err := table.Append(d)
		assert.NoError(t, err)
	}
	assert.Error(t, Update(ctx, tier, "some name", table, counter2.RollingCounter{}))
}
