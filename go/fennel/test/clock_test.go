package test

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUnix_Now(t *testing.T) {
	instance, err := MockPlane()
	assert.NoError(t, err)

	clock := &FakeClock{}
	instance.Clock = clock
	assert.Equal(t, int64(0), instance.Clock.Now())
	clock.Set(123)
	assert.Equal(t, int64(123), instance.Clock.Now())
	clock.Set(12321)
	assert.Equal(t, int64(12321), instance.Clock.Now())
}
