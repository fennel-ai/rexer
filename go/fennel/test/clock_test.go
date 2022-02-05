package test

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUnix_Now(t *testing.T) {
	tier, err := Tier()
	assert.NoError(t, err)

	clock := &FakeClock{}
	tier.Clock = clock
	assert.Equal(t, int64(0), tier.Clock.Now())
	clock.Set(123)
	assert.Equal(t, int64(123), tier.Clock.Now())
	clock.Set(12321)
	assert.Equal(t, int64(12321), tier.Clock.Now())
}
