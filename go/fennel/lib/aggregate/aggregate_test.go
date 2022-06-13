package aggregate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
)

func TestAggregate_Validate_Valid(t *testing.T) {
	validCases := []Aggregate{
		{Name: "some name", Options: Options{AggType: "sum", Durations: []uint32{1231}}},
		{Name: "some name", Options: Options{AggType: "average", Durations: []uint32{1200, 1231}}},
		{Name: "some name", Options: Options{AggType: "timeseries_sum", Window: ftypes.Window_DAY, Limit: 10}},
		{Name: "some name", Options: Options{AggType: "timeseries_sum", Window: ftypes.Window_HOUR, Limit: 10}},
		{Name: "some name", Options: Options{AggType: "list", Durations: []uint32{1200, 1212}}},
		{Name: "some name", Options: Options{AggType: "min", Durations: []uint32{1200, 1212}}},
		{Name: "some name", Options: Options{AggType: "max", Durations: []uint32{1200, 1212}}},
		{Name: "some name", Options: Options{AggType: "stddev", Durations: []uint32{1200, 1212}}},
		{Name: "some name", Options: Options{AggType: "rate", Durations: []uint32{1200, 1212}, Normalize: false}},
		{Name: "some name", Options: Options{AggType: "rate", Durations: []uint32{1200, 1212}, Normalize: true}},
	}
	for _, test := range validCases {
		assert.NoError(t, test.Validate())
	}
}

func TestAggregate_Validate_Invalid(t *testing.T) {
	validCases := []Aggregate{
		{Options: Options{AggType: "sum", Durations: []uint32{0}}},
		{Options: Options{AggType: "sum", Window: ftypes.Window_MINUTE}},
		{Options: Options{AggType: "sum", Window: ftypes.Window_MINUTE, Durations: []uint32{1200, 123}}},
		{Options: Options{AggType: "sum", Window: ftypes.Window_MINUTE, Limit: 123}},
		{Options: Options{AggType: "average", Durations: []uint32{1200, 0}}},
		{Options: Options{AggType: "average", Window: ftypes.Window_MINUTE}},
		{Options: Options{AggType: "average", Window: ftypes.Window_MINUTE, Durations: []uint32{1200, 123}}},
		{Options: Options{AggType: "average", Window: ftypes.Window_MINUTE, Limit: 123}},
		{Options: Options{AggType: "timeseries_sum", Limit: 10}},
		{Options: Options{AggType: "timeseries_sum", Window: ftypes.Window_HOUR}},
		{Options: Options{AggType: "timeseries_sum", Durations: []uint32{1200, 41}}},
		{Options: Options{AggType: "timeseries_sum", Window: ftypes.Window_HOUR, Limit: 10, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: "list", Window: ftypes.Window_HOUR, Limit: 10, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: "list", Window: ftypes.Window_HOUR, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: "list", Window: ftypes.Window_HOUR}},
		{Options: Options{AggType: "list"}},
		{Options: Options{AggType: "min", Window: ftypes.Window_HOUR, Limit: 10, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: "min", Window: ftypes.Window_HOUR, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: "min", Window: ftypes.Window_HOUR}},
		{Options: Options{AggType: "min"}},
		{Options: Options{AggType: "max", Window: ftypes.Window_HOUR, Limit: 10, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: "max", Window: ftypes.Window_HOUR, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: "max", Window: ftypes.Window_HOUR}},
		{Options: Options{AggType: "max"}},
		{Options: Options{AggType: "stddev", Window: ftypes.Window_HOUR, Limit: 10, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: "stddev", Window: ftypes.Window_HOUR, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: "stddev", Window: ftypes.Window_HOUR}},
		{Options: Options{AggType: "stddev"}},
		{Options: Options{AggType: "random", Durations: []uint32{1200, 41}}},
		{Options: Options{AggType: "rate"}},
		{Options: Options{AggType: "rate", Window: ftypes.Window_HOUR, Limit: 10, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: "rate", Window: ftypes.Window_HOUR, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: "rate", Window: ftypes.Window_HOUR}},
	}
	for _, test := range validCases {
		assert.Error(t, test.Validate())
	}
}

func TestOfflineAggregate(t *testing.T) {
	assert.True(t, Aggregate{Options: Options{CronSchedule: "(3 * * * *)"}}.IsOffline())
	assert.False(t, Aggregate{Options: Options{}}.IsOffline())
}
