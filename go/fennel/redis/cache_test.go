package redis

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// TODO: make this test work with a struct (or some non-string)
// when I tried doing this earlier with byte string or a struct that impelemented
// MarshalBinary, it failed. But I gave up soon
func TestCache(t *testing.T) {
	mr, err := miniredis.Run()
	defer mr.Close()
	assert.NoError(t, err)
	client := NewClient(mr.Addr(), nil)
	cache := NewCache(client)

	k := "hi"
	exp := "bye"
	_, err = cache.Get(context.Background(), k)
	assert.Error(t, err)

	// set the cache and check again
	err = cache.Set(context.Background(), k, exp, 5*time.Second)
	assert.NoError(t, err)
	v, err := cache.Get(context.Background(), k)
	assert.NoError(t, err)
	assert.Equal(t, exp, v)

	// now move the time forward a bit
	mr.FastForward(4 * time.Second)
	// we can still get it
	v, err = cache.Get(context.Background(), k)
	assert.NoError(t, err)
	assert.Equal(t, exp, v)

	// but not if we move it further ahead beyond 5 sec
	mr.FastForward(1 * time.Second)
	// we can still get it
	_, err = cache.Get(context.Background(), k)
	assert.Error(t, err)

	// now try deleting it
	err = cache.Delete(context.Background(), k)
	assert.NoError(t, err)
	_, err = cache.Get(context.Background(), k)
	assert.Error(t, err)
}
