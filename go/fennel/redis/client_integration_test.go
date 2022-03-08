//go:build integration

package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/resource"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

const (
	addr = "clustercfg.redis-db-54c4908.fbjfph.memorydb.us-west-2.amazonaws.com:6379"
)

func TestRedisClientIntegration(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.RealmID(rand.Uint32())
	scope := resource.NewTierScope(tierID)
	conf := ClientConfig{Addr: addr, TLSConfig: &tls.Config{}, Scope: scope}
	rdb, err := conf.Materialize()
	assert.NoError(t, err)
	t.Run("integration_get_set_del", func(t *testing.T) { testClient(t, rdb.(Client)) })
	t.Run("integration_mget", func(t *testing.T) { testMGet(t, rdb.(Client)) })
}

func TestMultiSetTTL(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.RealmID(rand.Uint32())
	scope := resource.NewTierScope(tierID)
	conf := ClientConfig{Addr: addr, TLSConfig: &tls.Config{}, Scope: scope}
	rdb, err := conf.Materialize()
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
	tierID := ftypes.RealmID(rand.Uint32())
	scope := resource.NewTierScope(tierID)
	conf := ClientConfig{Addr: addr, TLSConfig: &tls.Config{}, Scope: scope}
	rdb, err := conf.Materialize()
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

	// and since we use clustered client with pipeline, but we get no errors even when keys aren't sharded carefully
	keys = make([]string, 0)
	values = make([]interface{}, 0)
	for j := 0; j < 100; j++ {
		k := fmt.Sprintf("%s{key:%d}%s", utils.RandString(5), j, utils.RandString(5))
		keys = append(keys, k)
		values = append(values, utils.RandString(5))
	}
	assert.NoError(t, c.MSet(ctx, keys, values, make([]time.Duration, len(keys))))
	found, err := c.MGet(ctx, keys...)
	assert.NoError(t, err)
	assert.Equal(t, values, found)
}

func TestClientConfig_Materialize_Invalid(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.RealmID(rand.Uint32())
	scope := resource.NewTierScope(tierID)
	scenarios := []struct {
		name string
		conf ClientConfig
	}{
		{"invalid_url", ClientConfig{"some_random.aws.com:6379", &tls.Config{}, scope}},
		// i.e. include the url without the port
		{"no_port", ClientConfig{strings.Split(addr, ":6379")[0], &tls.Config{}, scope}},
		// i.e. valid url but without tls config
		{"no_tls", ClientConfig{addr, nil, scope}},
	}
	for i := range scenarios {
		scenario := scenarios[i]
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
			_, err := scenario.conf.Materialize()
			assert.Error(t, err)
		})
	}
}
