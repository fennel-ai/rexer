package counter

import (
	"fennel/engine/ast"
	libaggregate "fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/model/aggregate"
	"fennel/test"
	"fennel/tier"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"testing"
)

// verifies that given a table created from a query, we do correct inserts/queries
func TestRolling(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	start := 24*3600*12 + 60*31
	agg := libaggregate.Aggregate{
		CustID:    tier.CustID,
		Type:      "rolling_counter",
		Name:      "mycounter",
		Query:     ast.MakeInt(1),
		Timestamp: 0,
		Options: libaggregate.AggOptions{
			Duration: 3600 * 28,
		},
	}
	querySer, err := ast.Marshal(agg.Query)
	assert.NoError(t, err)
	optionSer, err := proto.Marshal(&agg.Options)
	assert.NoError(t, err)

	key := value.List{value.Int(1), value.Int(2)}
	assert.NoError(t, aggregate.Store(tier, agg.Type, agg.Name, querySer, agg.Timestamp, optionSer))
	table := value.NewTable()
	// create an event every minute for 2 days
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(start + i*60 + 30)
		row := value.Dict{
			"timestamp": value.Int(ts),
			"key":       key,
		}
		assert.NoError(t, table.Append(row))
	}
	err = Update(tier, agg.Name, table)
	assert.NoError(t, err)

	clock := &test.FakeClock{}
	tier.Clock = clock
	clock.Set(int64(start + 24*3600*2))
	// at the end of 2 days, rolling counter should only be worth 28 hours, not full 48 hours
	found, err := RollingValue(tier, agg, key)
	assert.NoError(t, err)
	assert.Equal(t, value.Int(28*60), found)
}

func TestTimeseries(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	start := 24*3600*12 + 60
	agg := libaggregate.Aggregate{
		CustID:    tier.CustID,
		Type:      "timeseries_counter",
		Name:      "mycounter",
		Query:     ast.MakeInt(1),
		Timestamp: 0,
		// at any time, we want data from last 9 hours
		Options: libaggregate.AggOptions{
			Window: ftypes.Window_HOUR,
			Limit:  9,
		},
	}
	querySer, err := ast.Marshal(agg.Query)
	assert.NoError(t, err)
	optionSer, err := proto.Marshal(&agg.Options)
	assert.NoError(t, err)

	assert.NoError(t, aggregate.Store(tier, agg.Type, agg.Name, querySer, agg.Timestamp, optionSer))
	key := value.List{value.Int(1), value.Int(2)}
	table := value.NewTable()
	// create an event every minute for 2 days
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(start + i*60 + 30)
		row := value.Dict{
			"timestamp": value.Int(ts),
			"key":       key,
		}
		assert.NoError(t, table.Append(row))
	}
	err = Update(tier, agg.Name, table)
	assert.NoError(t, err)

	clock := &test.FakeClock{}
	tier.Clock = clock
	clock.Set(int64(start + 24*3600*2))
	// at the end of 2 days, we should get one data point each for 9 days
	found, err := TimeseriesValue(tier, agg, key)
	assert.NoError(t, err)
	assert.Len(t, found, 9)
	for i := range found {
		assert.Equal(t, value.Int(60), found[i])
	}

	// but if we set time to just at 6 hours from start, we will still 9 entries, but few will be zero padded
	// and since our start time is 1 min delayed, the 4th entry will be one short of 60
	clock.Set(int64(start + 6*3600))
	found, err = TimeseriesValue(tier, agg, key)
	assert.NoError(t, err)
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

func TestCounterUpdateInvalid(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	// no col for key or timestamp
	assertInvalid(tier, t, value.Dict{"hi": value.Int(1)}, value.Dict{"hi": value.Int(3)})
	// no col for key
	assertInvalid(tier, t, value.Dict{"timestamp": value.Int(1)}, value.Dict{"timestamp": value.Int(3)})
	// timestamp is not int
	assertInvalid(tier, t,
		value.Dict{"timestamp": value.Double(1), "key": value.List{value.Int(1)}},
		value.Dict{"timestamp": value.Double(3), "key": value.List{value.Int(3)}},
	)
}

func assertInvalid(tier tier.Tier, t *testing.T, ds ...value.Dict) {
	table := value.NewTable()
	for _, d := range ds {
		err := table.Append(d)
		assert.NoError(t, err)
	}
	assert.Error(t, Update(tier, "some name", table))
}
