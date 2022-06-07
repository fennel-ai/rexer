package test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnix_Now(t *testing.T) {
	tier, err := Tier()
	assert.NoError(t, err)

	clock := &FakeClock{}
	tier.Clock = clock
	assert.Equal(t, uint32(0), tier.Clock.Now())
	clock.Set(123)
	assert.Equal(t, uint32(123), tier.Clock.Now())
	clock.Set(12321)
	assert.Equal(t, uint32(12321), tier.Clock.Now())
}
