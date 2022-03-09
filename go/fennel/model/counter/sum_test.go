package counter

import (
	"testing"

	"fennel/lib/ftypes"
	"fennel/lib/value"

	"github.com/stretchr/testify/assert"
)

func TestRollingCounter_Reduce(t *testing.T) {
	h := rollingSum{}
	nums := []value.Value{value.Int(1), value.Int(4), value.Int(-2)}
	found, err := h.Reduce(nums)
	assert.NoError(t, err)
	assert.Equal(t, value.Int(3), found)
}

func TestRollingCounter_Start(t *testing.T) {
	h := rollingSum{Duration: 100}
	assert.Equal(t, h.Start(110), ftypes.Timestamp(10))
	// Duration > end
	assert.Equal(t, h.Start(90), ftypes.Timestamp(0))
}
