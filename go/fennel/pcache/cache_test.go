package pcache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPCache_Get(t *testing.T) {
	// Note - wait 10ms for value to be set (to pass through buffers)
	cache, err := NewPCache(1<<10, 1<<4)
	assert.NoError(t, err)

	// initially, should get nothing as this key was not set
	_, ok := cache.Get("some key")
	assert.False(t, ok)

	// set it now
	ok = cache.Set("some key", 27)
	assert.True(t, ok)
	time.Sleep(10 * time.Millisecond)

	// should get it
	v, ok := cache.Get("some key")
	assert.True(t, ok)
	assert.Equal(t, 27, v)

	// set a key with TTL
	ok = cache.SetWithTTL("other key", 64, 5*time.Second)
	assert.True(t, ok)
	time.Sleep(10 * time.Millisecond)

	// should get it
	v, ok = cache.Get("other key")
	assert.True(t, ok)
	assert.Equal(t, 64, v)

	// should expire after 5 seconds
	time.Sleep(5 * time.Second)
	v, ok = cache.Get("other key")
	assert.False(t, ok)
}
