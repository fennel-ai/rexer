package math

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNextPowerOf2(t *testing.T) {
	cases := []struct {
		n uint64
		p uint64
	}{
		{0, 1}, {3, 4}, {7, 8}, {121, 128}, {(1 << 33) - 4, 1 << 33},
	}
	for _, case_ := range cases {
		assert.Equal(t, case_.p, NextPowerOf2(case_.n))
	}
	for i := 0; i < 63; i++ {
		assert.Equal(t, uint64(1<<i), NextPowerOf2(1<<i))
	}
}
