package aggregate

import (
	"fennel/lib/ftypes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAggregate_Validate_Valid(t *testing.T) {
	validCases := []Aggregate{
		{Name: "some name", Options: AggOptions{AggType: "rolling_counter", Duration: 1231}},
		{Name: "some name", Options: AggOptions{AggType: "rolling_average", Duration: 1231}},
		{Name: "some name", Options: AggOptions{AggType: "timeseries_counter", Window: ftypes.Window_DAY, Limit: 10}},
		{Name: "some name", Options: AggOptions{AggType: "timeseries_counter", Window: ftypes.Window_HOUR, Limit: 10}},
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
		{Options: AggOptions{AggType: "random", Duration: 41}},
	}
	for _, test := range validCases {
		assert.Error(t, test.Validate())
	}
}
