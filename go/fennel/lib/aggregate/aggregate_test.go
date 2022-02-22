package aggregate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
)

func TestAggregate_Validate_Valid(t *testing.T) {
	validCases := []Aggregate{
		{Name: "some name", Options: AggOptions{AggType: "count", Duration: 1231}},
		{Name: "some name", Options: AggOptions{AggType: "average", Duration: 1231}},
		{Name: "some name", Options: AggOptions{AggType: "timeseries_count", Window: ftypes.Window_DAY, Limit: 10}},
		{Name: "some name", Options: AggOptions{AggType: "timeseries_count", Window: ftypes.Window_HOUR, Limit: 10}},
		{Name: "some name", Options: AggOptions{AggType: "list", Duration: 1212}},
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
		{Options: AggOptions{AggType: "count", Duration: 0}},
		{Options: AggOptions{AggType: "count", Window: ftypes.Window_MINUTE}},
		{Options: AggOptions{AggType: "count", Window: ftypes.Window_MINUTE, Duration: 123}},
		{Options: AggOptions{AggType: "count", Window: ftypes.Window_MINUTE, Limit: 123}},
		{Options: AggOptions{AggType: "average", Duration: 0}},
		{Options: AggOptions{AggType: "average", Window: ftypes.Window_MINUTE}},
		{Options: AggOptions{AggType: "average", Window: ftypes.Window_MINUTE, Duration: 123}},
		{Options: AggOptions{AggType: "average", Window: ftypes.Window_MINUTE, Limit: 123}},
		{Options: AggOptions{AggType: "timeseries_count", Limit: 10}},
		{Options: AggOptions{AggType: "timeseries_count", Window: ftypes.Window_HOUR}},
		{Options: AggOptions{AggType: "timeseries_count", Duration: 41}},
		{Options: AggOptions{AggType: "timeseries_count", Window: ftypes.Window_HOUR, Limit: 10, Duration: 12}},
		{Options: AggOptions{AggType: "list", Window: ftypes.Window_HOUR, Limit: 10, Duration: 12}},
		{Options: AggOptions{AggType: "list", Window: ftypes.Window_HOUR, Duration: 12}},
		{Options: AggOptions{AggType: "list", Window: ftypes.Window_HOUR}},
		{Options: AggOptions{AggType: "list"}},
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
