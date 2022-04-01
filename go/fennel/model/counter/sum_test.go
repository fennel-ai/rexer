package counter

import (
	"testing"

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
