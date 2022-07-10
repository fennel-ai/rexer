package redis

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"fennel/lib/cache"
	"fennel/lib/ftypes"
	resource2 "fennel/resource"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
)

// TODO: make this test work with a struct (or some non-string)
// when I tried doing this earlier with byte string or a struct that impelemented
// MarshalBinary, it failed. But I gave up soon
func TestCache(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.RealmID(rand.Uint32())
	scope := resource2.NewTierScope(tierID)
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	resource, err := MiniRedisConfig{MiniRedis: mr, Scope: scope}.Materialize()
	assert.NoError(t, err)
	client := resource.(Client)
	c := NewCache(client)
	defer client.Close()

	k := "hi"
	exp := "bye"
	_, err = c.Get(context.Background(), k)
	assert.Error(t, err)

	// set the cache and check again
	err = c.Set(context.Background(), k, exp, 5*time.Second)
	assert.NoError(t, err)
	v, err := c.Get(context.Background(), k)
	assert.NoError(t, err)
	assert.Equal(t, exp, v)

	// now move the time forward a bit
	var conf MiniRedisConfig = client.conf.(MiniRedisConfig)
	mr = conf.MiniRedis
	mr.FastForward(4 * time.Second)
	// we can still get it
	v, err = c.Get(context.Background(), k)
	assert.NoError(t, err)
	assert.Equal(t, exp, v)

	// but not if we move it further ahead beyond 5 sec
	mr.FastForward(1 * time.Second)
	// we can still get it
	_, err = c.Get(context.Background(), k)
	assert.Error(t, err)

	// try setting it in a txn
	err = c.RunAsTxn(context.Background(), func(c cache.Txn, keys []string) error {
		return c.Set(context.Background(), keys[0], "hi2", *new(time.Duration))
	}, []string{k}, 1)
	assert.NoError(t, err)

	v, err = c.Get(context.Background(), k)
	assert.NoError(t, err)
	assert.Equal(t, "hi2", v)

	// now try deleting it
	err = c.Delete(context.Background(), k)
	assert.NoError(t, err)
	_, err = c.Get(context.Background(), k)
	assert.Error(t, err)
}
