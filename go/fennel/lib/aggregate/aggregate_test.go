package aggregate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
)

func TestAggregate_Validate_Valid(t *testing.T) {
	validCases := []Aggregate{
		{Name: "some name", Options: AggOptions{AggType: "rolling_counter", Duration: 1231}},
		{Name: "some name", Options: AggOptions{AggType: "rolling_average", Duration: 1231}},
		{Name: "some name", Options: AggOptions{AggType: "timeseries_counter", Window: ftypes.Window_DAY, Limit: 10}},
		{Name: "some name", Options: AggOptions{AggType: "timeseries_counter", Window: ftypes.Window_HOUR, Limit: 10}},
		{Name: "some name", Options: AggOptions{AggType: "stream", Duration: 1212}},
		{Name: "some name", Options: AggOptions{AggType: "min", Duration: 1212}},
		{Name: "some name", Options: AggOptions{AggType: "max", Duration: 1212}},
		{Name: "some name", Options: AggOptions{AggType: "stddev", Duration: 1212}},
	}
	for _, test := range validCases {
		assert.NoError(t, test.Validate())
	}
}
func TestAggregate_Validate_Invalid(t *testing.T) {
	validCases := []Aggregate{
		{Options: AggOptions{AggType: "rolling_counter", Duration: 0}},
		{Options: AggOptions{AggType: "rolling_counter", Window: ftypes.Window_MINUTE}},
		{Options: AggOptions{AggType: "rolling_counter", Window: ftypes.Window_MINUTE, Duration: 123}},
		{Options: AggOptions{AggType: "rolling_counter", Window: ftypes.Window_MINUTE, Limit: 123}},
		{Options: AggOptions{AggType: "rolling_average", Duration: 0}},
		{Options: AggOptions{AggType: "rolling_average", Window: ftypes.Window_MINUTE}},
		{Options: AggOptions{AggType: "rolling_average", Window: ftypes.Window_MINUTE, Duration: 123}},
		{Options: AggOptions{AggType: "rolling_average", Window: ftypes.Window_MINUTE, Limit: 123}},
		{Options: AggOptions{AggType: "timeseries_counter", Limit: 10}},
		{Options: AggOptions{AggType: "timeseries_counter", Window: ftypes.Window_HOUR}},
		{Options: AggOptions{AggType: "timeseries_counter", Duration: 41}},
		{Options: AggOptions{AggType: "timeseries_counter", Window: ftypes.Window_HOUR, Limit: 10, Duration: 12}},
		{Options: AggOptions{AggType: "stream", Window: ftypes.Window_HOUR, Limit: 10, Duration: 12}},
		{Options: AggOptions{AggType: "stream", Window: ftypes.Window_HOUR, Duration: 12}},
		{Options: AggOptions{AggType: "stream", Window: ftypes.Window_HOUR}},
		{Options: AggOptions{AggType: "stream"}},
		{Options: AggOptions{AggType: "min", Window: ftypes.Window_HOUR, Limit: 10, Duration: 12}},
		{Options: AggOptions{AggType: "min", Window: ftypes.Window_HOUR, Duration: 12}},
		{Options: AggOptions{AggType: "min", Window: ftypes.Window_HOUR}},
		{Options: AggOptions{AggType: "min"}},
		{Options: AggOptions{AggType: "max", Window: ftypes.Window_HOUR, Limit: 10, Duration: 12}},
		{Options: AggOptions{AggType: "max", Window: ftypes.Window_HOUR, Duration: 12}},
		{Options: AggOptions{AggType: "max", Window: ftypes.Window_HOUR}},
		{Options: AggOptions{AggType: "max"}},
		{Options: AggOptions{AggType: "stddev", Window: ftypes.Window_HOUR, Limit: 10, Duration: 12}},
		{Options: AggOptions{AggType: "stddev", Window: ftypes.Window_HOUR, Duration: 12}},
		{Options: AggOptions{AggType: "stddev", Window: ftypes.Window_HOUR}},
		{Options: AggOptions{AggType: "stddev"}},
		{Options: AggOptions{AggType: "random", Duration: 41}},
	}
	for _, test := range validCases {
		assert.Error(t, test.Validate())
	}
}
