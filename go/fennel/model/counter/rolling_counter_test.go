package counter

import (
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRollingCounter_Reduce(t *testing.T) {
	h := RollingCounter{}
	nums := []value.Value{value.Int(1), value.Int(4), value.Int(-2)}
	found, err := h.Reduce(nums)
	assert.NoError(t, err)
	assert.Equal(t, value.Int(3), found)
}

func TestRollingCounter_Start(t *testing.T) {
	h := RollingCounter{Duration: 100}
	assert.Equal(t, h.Start(110), ftypes.Timestamp(10))
	// Duration > end
	assert.Equal(t, h.Start(90), ftypes.Timestamp(0))
}
