package counter

import (
	"fennel/lib/value"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTimeseriesCounter_Reduce(t *testing.T) {
	h := TimeseriesCounter{Limit: 4}
	//nums := []int64{1, 4, -2}
	nums := []value.Value{value.Int(1), value.Int(4), value.Int(-2)}
	found, err := h.Reduce(nums)
	assert.NoError(t, err)
	assert.Equal(t, value.List{value.Int(0), value.Int(1), value.Int(4), value.Int(-2)}, found)

	h = TimeseriesCounter{Limit: 2}
	found, err = h.Reduce(nums)
	assert.NoError(t, err)
	assert.Equal(t, value.List{value.Int(4), value.Int(-2)}, found)
}
