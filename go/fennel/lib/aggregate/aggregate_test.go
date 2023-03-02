package aggregate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
)

func TestAggregate_Validate_Valid(t *testing.T) {
	validCases := []Aggregate{
		{Name: "some name", Options: Options{AggType: SUM, Durations: []uint32{1231}}},
		{Name: "some name", Options: Options{AggType: AVERAGE, Durations: []uint32{1200, 1231}}},
		{Name: "some name", Options: Options{AggType: TIMESERIES_SUM, Window: ftypes.Window_DAY, Limit: 10}},
		{Name: "some name", Options: Options{AggType: TIMESERIES_SUM, Window: ftypes.Window_HOUR, Limit: 10}},
		{Name: "some name", Options: Options{AggType: LIST, Durations: []uint32{1200, 1212}}},
		{Name: "some name", Options: Options{AggType: MIN, Durations: []uint32{1200, 1212}}},
		{Name: "some name", Options: Options{AggType: MAX, Durations: []uint32{1200, 1212}}},
		{Name: "some name", Options: Options{AggType: STDDEV, Durations: []uint32{1200, 1212}}},
		{Name: "some name", Options: Options{AggType: RATE, Durations: []uint32{1200, 1212}, Normalize: false}},
		{Name: "some name", Options: Options{AggType: RATE, Durations: []uint32{1200, 1212}, Normalize: true}},
	}
	for _, test := range validCases {
		assert.NoError(t, test.Validate())
	}
}

func TestAggregate_Validate_Invalid(t *testing.T) {
	validCases := []Aggregate{
		{Options: Options{AggType: SUM, Durations: []uint32{0}}},
		{Options: Options{AggType: SUM, Window: ftypes.Window_MINUTE}},
		{Options: Options{AggType: SUM, Window: ftypes.Window_MINUTE, Durations: []uint32{1200, 123}}},
		{Options: Options{AggType: SUM, Window: ftypes.Window_MINUTE, Limit: 123}},
		{Options: Options{AggType: AVERAGE, Durations: []uint32{1200, 0}}},
		{Options: Options{AggType: AVERAGE, Window: ftypes.Window_MINUTE}},
		{Options: Options{AggType: AVERAGE, Window: ftypes.Window_MINUTE, Durations: []uint32{1200, 123}}},
		{Options: Options{AggType: AVERAGE, Window: ftypes.Window_MINUTE, Limit: 123}},
		{Options: Options{AggType: TIMESERIES_SUM, Limit: 10}},
		{Options: Options{AggType: TIMESERIES_SUM, Window: ftypes.Window_HOUR}},
		{Options: Options{AggType: TIMESERIES_SUM, Durations: []uint32{1200, 41}}},
		{Options: Options{AggType: TIMESERIES_SUM, Window: ftypes.Window_HOUR, Limit: 10, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: LIST, Window: ftypes.Window_HOUR, Limit: 10, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: LIST, Window: ftypes.Window_HOUR, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: LIST, Window: ftypes.Window_HOUR}},
		{Options: Options{AggType: LIST}},
		{Options: Options{AggType: MIN, Window: ftypes.Window_HOUR, Limit: 10, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: MIN, Window: ftypes.Window_HOUR, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: MIN, Window: ftypes.Window_HOUR}},
		{Options: Options{AggType: MIN}},
		{Options: Options{AggType: MAX, Window: ftypes.Window_HOUR, Limit: 10, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: MAX, Window: ftypes.Window_HOUR, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: MAX, Window: ftypes.Window_HOUR}},
		{Options: Options{AggType: MAX}},
		{Options: Options{AggType: STDDEV, Window: ftypes.Window_HOUR, Limit: 10, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: STDDEV, Window: ftypes.Window_HOUR, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: STDDEV, Window: ftypes.Window_HOUR}},
		{Options: Options{AggType: STDDEV}},
		{Options: Options{AggType: "random", Durations: []uint32{1200, 41}}},
		{Options: Options{AggType: RATE}},
		{Options: Options{AggType: RATE, Window: ftypes.Window_HOUR, Limit: 10, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: RATE, Window: ftypes.Window_HOUR, Durations: []uint32{1200, 12}}},
		{Options: Options{AggType: RATE, Window: ftypes.Window_HOUR}},
	}
	for _, test := range validCases {
		assert.Error(t, test.Validate())
	}
}

func TestOfflineAggregate(t *testing.T) {
	assert.True(t, Aggregate{Options: Options{CronSchedule: "(3 * * * *)", AggType: CF}}.IsOffline())
	assert.False(t, Aggregate{Options: Options{}}.IsOffline())
}
