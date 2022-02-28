package math

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWilson(t *testing.T) {
	s, err := Wilson(35, 200, true)
	assert.NoError(t, err)
	assert.Equal(t, 0.12860441174608936, s)
	s, err = Wilson(35, 200, false)
	assert.NoError(t, err)
	assert.Equal(t, 0.23364549210081922, s)

	// and if num is < den, we also get error
	_, err = Wilson(201, 200, true)
	assert.Error(t, err)
	_, err = Wilson(201, 200, false)
	assert.Error(t, err)
}
