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

	key1 := "some key"
	key2 := "other key"

	// initially, should get nothing as this key was not set
	_, ok := cache.Get(key1, "Test")
	assert.False(t, ok)

	// set it now
	ok = cache.Set(key1, 27, 4)
	assert.True(t, ok)
	time.Sleep(10 * time.Millisecond)

	// should get it
	v, ok := cache.Get(key1, "Test")
	assert.True(t, ok)
	assert.Equal(t, 27, v)

	// set a key with TTL
	ok = cache.SetWithTTL(key2, 64, 0, 5*time.Second, "Test")
	assert.True(t, ok)
	time.Sleep(10 * time.Millisecond)

	// should get it
	v, ok = cache.Get(key2, "Test")
	assert.True(t, ok)
	assert.Equal(t, 64, v)

	// can get its ttl, should be less or equal to initial ttl
	ttl, ok := cache.GetTTL(key2)
	assert.True(t, ok)
	assert.LessOrEqual(t, ttl, 5*time.Second)

	// should expire after 5 seconds
	time.Sleep(5 * time.Second)
	v, ok = cache.Get(key2, "Test")
	assert.False(t, ok)
}
