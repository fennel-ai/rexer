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
	s, err := h.Start(110, value.Dict{})
	assert.NoError(t, err)
	assert.Equal(t, s, ftypes.Timestamp(10))
	// Duration > end
	s, err = h.Start(90, value.Dict{})
	assert.NoError(t, err)
	assert.Equal(t, s, ftypes.Timestamp(0))
	// Test kwargs
	s, err = h.Start(200, value.Dict{"duration": value.Int(50)})
	assert.NoError(t, err)
	assert.Equal(t, s, ftypes.Timestamp(150))
}
