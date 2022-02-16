//go:build integration

package redis

import (
	"context"
	"crypto/tls"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

const (
	addr = "clustercfg.redis-db-5dec5dd.fbjfph.memorydb.us-west-2.amazonaws.com:6379"
)

func TestRedisClientIntegration(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.TierID(rand.Uint32())
	conf := ClientConfig{Addr: addr, TLSConfig: &tls.Config{}}
	rdb, err := conf.Materialize(tierID)
	assert.NoError(t, err)
	t.Run("integration_get_set_del", func(t *testing.T) { testClient(t, rdb.(Client)) })
	t.Run("integration_mget", func(t *testing.T) { testMGet(t, rdb.(Client)) })
}

func TestMultiSetTTL(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.TierID(rand.Uint32())
	conf := ClientConfig{Addr: addr, TLSConfig: &tls.Config{}}
	rdb, err := conf.Materialize(tierID)
	assert.NoError(t, err)
	ctx := context.Background()
	c := rdb.(Client)

	k1, k2, k3 := "{test}one", "{test}two", "{test}three"
	vals := []interface{}{"v1", "v2", "v3"}
	// initially nothing is present in redis
	for _, k := range []string{k1, k2, k3} {
		_, err := c.Get(ctx, k)
		assert.Equal(t, redis.Nil, err)
	}
	ttl1 := time.Second
	ttl2 := time.Second + ttl1
	assert.NoError(t, c.MSet(ctx, []string{k1, k2, k3}, vals, []time.Duration{ttl1, ttl2, ttl1}))
	for i, k := range []string{k1, k2, k3} {
		found, err := c.Get(ctx, k)
		assert.NoError(t, err)
		assert.Equal(t, vals[i], found)
	}
	time.Sleep(ttl1)
	// one and three should be gone but two should still be there
	_, err = c.Get(ctx, k1)
	assert.Equal(t, redis.Nil, err)
	_, err = c.Get(ctx, k3)
	assert.Equal(t, redis.Nil, err)
	found, err := c.Get(ctx, k2)
	assert.NoError(t, err)
	assert.Equal(t, vals[1], found)

	// sleep some more and even this should be gone
	time.Sleep(ttl1)
	_, err = c.Get(ctx, k2)
	assert.Equal(t, redis.Nil, err)
}

func TestMultiGetSet(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.TierID(rand.Uint32())
	conf := ClientConfig{Addr: addr, TLSConfig: &tls.Config{}}
	rdb, err := conf.Materialize(tierID)
	assert.NoError(t, err)
	ctx := context.Background()
	c := rdb.(Client)

	// no errors when keys are sharded carefully
	keys := make([]string, 0)
	values := make([]interface{}, 0)
	shard := utils.RandString(5)
	for j := 0; j < 100; j++ {
		k := fmt.Sprintf("%s{key:%s}%s", utils.RandString(5), shard, utils.RandString(5))
		keys = append(keys, k)
		values = append(values, utils.RandString(5))
	}
	assert.NoError(t, c.MSet(ctx, keys, values, make([]time.Duration, len(keys))))
	vals, err := c.MGet(ctx, keys...)
	assert.NoError(t, err)
	assert.Len(t, vals, 100)
	for i, v := range vals {
		assert.Equal(t, values[i], v)
	}

	// but we get errors when keys aren't shared carefully
	keys = make([]string, 0)
	values = make([]interface{}, 0)
	for j := 0; j < 100; j++ {
		k := fmt.Sprintf("%skey:%d%s", utils.RandString(5), j, utils.RandString(5))
		keys = append(keys, k)
		values = append(values, utils.RandString(5))
	}
	assert.Error(t, c.MSet(ctx, keys, values, make([]time.Duration, len(keys))))
	_, err = c.MGet(ctx, keys...)
	assert.Error(t, err)
}
