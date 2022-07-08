package counter

import (
	"testing"

	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"

	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
)

func TestTimeseriesCounter_Reduce(t *testing.T) {
	h := NewTimeseriesSum(aggregate.Options{Limit: 4})
	//nums := []int64{1, 4, -2}
	nums := []value.Value{value.Int(1), value.Int(4), value.Int(-2)}
	found, err := h.Reduce(nums)
	assert.NoError(t, err)
	assert.Equal(t, value.NewList(value.Int(0), value.Int(1), value.Int(4), value.Int(-2)), found)

	h = NewTimeseriesSum(aggregate.Options{Limit: 2})
	found, err = h.Reduce(nums)
	assert.NoError(t, err)
	assert.Equal(t, value.NewList(value.Int(4), value.Int(-2)), found)
}

func TestTimeseriesCounter_Start(t *testing.T) {
	// Limit: 1, Window: Hour makes duration = 7200
	h, err := ToHistogram(ftypes.AggId(1), aggregate.Options{
		AggType: aggregate.TIMESERIES_SUM,
		Limit:   1,
		Window:  ftypes.Window_HOUR,
	})
	assert.NoError(t, err)
	s, err := Start(h, 7300, mo.None[uint32]())
	assert.NoError(t, err)
	assert.Equal(t, s, ftypes.Timestamp(100))
	// limit is larger than end.
	s, err = Start(h, 7100, mo.None[uint32]())
	assert.NoError(t, err)
	assert.Equal(t, s, ftypes.Timestamp(0))

	// Limit: 1, Window: day makes duration = 172800
	h, err = ToHistogram(ftypes.AggId(1), aggregate.Options{
		AggType: aggregate.TIMESERIES_SUM,
		Limit:   1,
		Window:  ftypes.Window_DAY,
	})
	assert.NoError(t, err)
	s, err = Start(h, 172900, mo.None[uint32]())
	assert.NoError(t, err)
	assert.Equal(t, s, ftypes.Timestamp(100))
	// limit is larger than end.
	s, err = Start(h, 172700, mo.None[uint32]())
	assert.NoError(t, err)
	assert.Equal(t, s, ftypes.Timestamp(0))
}
