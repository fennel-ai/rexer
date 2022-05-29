package counter

import (
	"testing"

	"fennel/lib/ftypes"
	"fennel/lib/value"

	"github.com/stretchr/testify/assert"
)

func TestTimeseriesCounter_Reduce(t *testing.T) {
	h := timeseriesSum{Limit: 4}
	//nums := []int64{1, 4, -2}
	nums := []value.Value{value.Int(1), value.Int(4), value.Int(-2)}
	found, err := h.Reduce(nums)
	assert.NoError(t, err)
	assert.Equal(t, value.NewList(value.Int(0), value.Int(1), value.Int(4), value.Int(-2)), found)

	h = timeseriesSum{Limit: 2}
	found, err = h.Reduce(nums)
	assert.NoError(t, err)
	assert.Equal(t, value.NewList(value.Int(4), value.Int(-2)), found)
}

func TestTimeseriesCounter_Start(t *testing.T) {
	// Limit: 1, Window: Hour makes duration = 7200
	h := timeseriesSum{Limit: 1, Window: ftypes.Window_HOUR}
	s, err := h.Start(7300, value.NewDict(nil))
	assert.NoError(t, err)
	assert.Equal(t, s, ftypes.Timestamp(100))
	// limit is larger than end.
	s, err = h.Start(7100, value.NewDict(nil))
	assert.NoError(t, err)
	assert.Equal(t, s, ftypes.Timestamp(0))

	// Limit: 1, Window: day makes duration = 172800
	h = timeseriesSum{Limit: 1, Window: ftypes.Window_DAY}
	s, err = h.Start(172900, value.NewDict(nil))
	assert.NoError(t, err)
	assert.Equal(t, s, ftypes.Timestamp(100))
	// limit is larger than end.
	s, err = h.Start(172700, value.NewDict(nil))
	assert.NoError(t, err)
	assert.Equal(t, s, ftypes.Timestamp(0))
}
