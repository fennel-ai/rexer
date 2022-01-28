package aggregate

import (
	"fennel/engine/ast"
	"fennel/instance"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/test"
	"github.com/stretchr/testify/assert"
	"testing"
)

// verifies that given a table created from a query, we do correct inserts/queries
func TestRolling(t *testing.T) {
	instance, err := test.DefaultInstance()
	assert.NoError(t, err)

	start := 24*3600*12 + 60*31
	agg := aggregate.Aggregate{
		CustID:    instance.CustID,
		Type:      "rolling_counter",
		Name:      "mycounter",
		Query:     ast.MakeInt(1),
		Timestamp: 0,
		Options: aggregate.AggOptions{
			Duration: 3600 * 28,
		},
	}
	oids := value.List{value.Int(1), value.Int(2)}
	key, err := makeKey(oids)
	assert.NoError(t, err)
	assert.NoError(t, Store(instance, agg))
	table := value.NewTable()
	// create an event every minute for 2 days
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(start + i*60 + 30)
		row := value.Dict{
			"timestamp": value.Int(ts),
			"key":       oids,
		}
		assert.NoError(t, table.Append(row))
	}
	err = counterUpdate(instance, agg.Name, table)
	assert.NoError(t, err)

	clock := &test.FakeClock{}
	instance.Clock = clock
	clock.Set(int64(start + 24*3600*2))
	// at the end of 2 days, rolling counter should only be worth 28 hours, not full 48 hours
	found, err := Value(instance, agg.Type, agg.Name, key)
	assert.NoError(t, err)
	assert.Equal(t, value.Int(28*60), found)
}

func TestTimeseries(t *testing.T) {
	instance, err := test.DefaultInstance()
	assert.NoError(t, err)

	start := 24*3600*12 + 60
	agg := aggregate.Aggregate{
		CustID:    instance.CustID,
		Type:      "timeseries_counter",
		Name:      "mycounter",
		Query:     ast.MakeInt(1),
		Timestamp: 0,
		// at any time, we want data from last 9 hours
		Options: aggregate.AggOptions{
			Window: ftypes.Window_HOUR,
			Limit:  9,
		},
	}
	assert.NoError(t, Store(instance, agg))
	oids := value.List{value.Int(1), value.Int(2)}
	key, err := makeKey(oids)
	assert.NoError(t, err)
	table := value.NewTable()
	// create an event every minute for 2 days
	for i := 0; i < 60*24*2; i++ {
		ts := ftypes.Timestamp(start + i*60 + 30)
		row := value.Dict{
			"timestamp": value.Int(ts),
			"key":       oids,
		}
		assert.NoError(t, table.Append(row))
	}
	err = counterUpdate(instance, agg.Name, table)
	assert.NoError(t, err)

	clock := &test.FakeClock{}
	instance.Clock = clock
	clock.Set(int64(start + 24*3600*2))
	// at the end of 2 days, we should get one data point each for 9 days
	//found, err := timeseriesValue(instance, agg, key)
	foundVal, err := Value(instance, agg.Type, agg.Name, key)
	found, ok := foundVal.(value.List)
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.Len(t, found, 9)
	for i := range found {
		assert.Equal(t, value.Int(60), found[i])
	}

	// but if we set time to just at 6 hours from start, we will still 9 entries, but few will be zero padded
	// and since our start time is 1 min delayed, the 4th entry will be one short of 60
	clock.Set(int64(start + 6*3600))
	foundVal, err = Value(instance, agg.Type, agg.Name, key)
	found, ok = foundVal.(value.List)
	assert.True(t, ok)
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
	instance, err := test.DefaultInstance()
	assert.NoError(t, err)
	assertInvalid(instance, t, value.Dict{"hi": value.Int(1)}, value.Dict{"hi": value.Int(3)})
	assertInvalid(instance, t, value.Dict{"timestamp": value.Int(1)}, value.Dict{"timestamp": value.Int(3)})
	assertInvalid(instance, t, value.Dict{"timestamp": value.Int(1), "key": value.Int(1)}, value.Dict{"timestamp": value.Int(3), "key": value.Int(1)})
	assertInvalid(instance, t,
		value.Dict{"timestamp": value.Int(1), "key": value.List{value.Bool(false)}},
		value.Dict{"timestamp": value.Int(3), "key": value.List{value.Bool(true)}},
	)
	assertInvalid(instance, t,
		value.Dict{"timestamp": value.Double(1), "key": value.List{value.Int(1)}},
		value.Dict{"timestamp": value.Double(3), "key": value.List{value.Int(3)}},
	)
}

func assertInvalid(instance instance.Instance, t *testing.T, ds ...value.Dict) {
	table := value.NewTable()
	for _, d := range ds {
		err := table.Append(d)
		assert.NoError(t, err)
	}
	assert.Error(t, counterUpdate(instance, "some name", table))
}
