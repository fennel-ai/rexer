package counter

import (
	"fennel/lib/value"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRollingCounter_Reduce(t *testing.T) {
	h := RollingCounter{}
	nums := []value.Value{value.Int(1), value.Int(4), value.Int(-2)}
	found, err := h.Reduce(nums)
	assert.NoError(t, err)
	assert.Equal(t, value.Int(3), found)
}

func TestCounterMarshal(t *testing.T) {
	h := RollingCounter{}
	nums := []value.Value{
		value.Int(1), value.Int(2), value.Int(-1), value.Int(0), value.Int(12312312),
		value.Int(1312123), value.Int(-34131212), value.Int(1e12), value.Int(-1e12),
	}
	for _, n := range nums {
		s, err := h.Marshal(n)
		assert.NoError(t, err)
		m, err := h.Unmarshal(s)
		assert.NoError(t, err)
		assert.Equal(t, n, m)
	}
}
