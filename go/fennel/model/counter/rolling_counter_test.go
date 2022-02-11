package counter

import (
	"fennel/lib/value"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRollingCounter_Reduce(t *testing.T) {
	h := RollingCounter{}
	nums := []int64{1, 4, -2}
	found, err := h.Reduce(nums)
	assert.NoError(t, err)
	assert.Equal(t, value.Int(3), found)
}

func TestCounterMarshal(t *testing.T) {
	h := RollingCounter{}
	nums := []int64{1, 2, -1, 0, 12312312, 1312123, -34131212, 1e12, -1e12}
	for _, n := range nums {
		s, err := h.Marshal(n)
		assert.NoError(t, err)
		m, err := h.Unmarshal(s)
		assert.NoError(t, err)
		assert.Equal(t, n, m)
	}
}
