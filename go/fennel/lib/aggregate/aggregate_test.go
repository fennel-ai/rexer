package aggregate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
)

func TestAggregate_Validate_Valid(t *testing.T) {
	validCases := []Aggregate{
		{Name: "some name", Options: Options{AggType: "count", Duration: 1231}},
		{Name: "some name", Options: Options{AggType: "average", Duration: 1231}},
		{Name: "some name", Options: Options{AggType: "timeseries_count", Window: ftypes.Window_DAY, Limit: 10}},
		{Name: "some name", Options: Options{AggType: "timeseries_count", Window: ftypes.Window_HOUR, Limit: 10}},
		{Name: "some name", Options: Options{AggType: "list", Duration: 1212}},
		{Name: "some name", Options: Options{AggType: "min", Duration: 1212}},
		{Name: "some name", Options: Options{AggType: "max", Duration: 1212}},
		{Name: "some name", Options: Options{AggType: "stddev", Duration: 1212}},
		{Name: "some name", Options: Options{AggType: "rate", Duration: 1212, Normalize: false}},
		{Name: "some name", Options: Options{AggType: "rate", Duration: 1212, Normalize: true}},
	}
	for _, test := range validCases {
		assert.NoError(t, test.Validate())
	}
}
func TestAggregate_Validate_Invalid(t *testing.T) {
	validCases := []Aggregate{
		{Options: Options{AggType: "count", Duration: 0}},
		{Options: Options{AggType: "count", Window: ftypes.Window_MINUTE}},
		{Options: Options{AggType: "count", Window: ftypes.Window_MINUTE, Duration: 123}},
		{Options: Options{AggType: "count", Window: ftypes.Window_MINUTE, Limit: 123}},
		{Options: Options{AggType: "average", Duration: 0}},
		{Options: Options{AggType: "average", Window: ftypes.Window_MINUTE}},
		{Options: Options{AggType: "average", Window: ftypes.Window_MINUTE, Duration: 123}},
		{Options: Options{AggType: "average", Window: ftypes.Window_MINUTE, Limit: 123}},
		{Options: Options{AggType: "timeseries_count", Limit: 10}},
		{Options: Options{AggType: "timeseries_count", Window: ftypes.Window_HOUR}},
		{Options: Options{AggType: "timeseries_count", Duration: 41}},
		{Options: Options{AggType: "timeseries_count", Window: ftypes.Window_HOUR, Limit: 10, Duration: 12}},
		{Options: Options{AggType: "list", Window: ftypes.Window_HOUR, Limit: 10, Duration: 12}},
		{Options: Options{AggType: "list", Window: ftypes.Window_HOUR, Duration: 12}},
		{Options: Options{AggType: "list", Window: ftypes.Window_HOUR}},
		{Options: Options{AggType: "list"}},
		{Options: Options{AggType: "min", Window: ftypes.Window_HOUR, Limit: 10, Duration: 12}},
		{Options: Options{AggType: "min", Window: ftypes.Window_HOUR, Duration: 12}},
		{Options: Options{AggType: "min", Window: ftypes.Window_HOUR}},
		{Options: Options{AggType: "min"}},
		{Options: Options{AggType: "max", Window: ftypes.Window_HOUR, Limit: 10, Duration: 12}},
		{Options: Options{AggType: "max", Window: ftypes.Window_HOUR, Duration: 12}},
		{Options: Options{AggType: "max", Window: ftypes.Window_HOUR}},
		{Options: Options{AggType: "max"}},
		{Options: Options{AggType: "stddev", Window: ftypes.Window_HOUR, Limit: 10, Duration: 12}},
		{Options: Options{AggType: "stddev", Window: ftypes.Window_HOUR, Duration: 12}},
		{Options: Options{AggType: "stddev", Window: ftypes.Window_HOUR}},
		{Options: Options{AggType: "stddev"}},
		{Options: Options{AggType: "random", Duration: 41}},
		{Options: Options{AggType: "rate"}},
		{Options: Options{AggType: "rate", Window: ftypes.Window_HOUR, Limit: 10, Duration: 12}},
		{Options: Options{AggType: "rate", Window: ftypes.Window_HOUR, Duration: 12}},
		{Options: Options{AggType: "rate", Window: ftypes.Window_HOUR}},
	}
	for _, test := range validCases {
		assert.Error(t, test.Validate())
	}
}
