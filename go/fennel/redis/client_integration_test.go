//go:build integration

package redis

import (
	"context"
	"crypto/tls"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/resource"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

var (
	hostAddr = os.Getenv("REDIS_SERVER_ADDRESS")
)

func tlsConfig() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true,
	}
}

func TestRedisClientIntegration(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.RealmID(rand.Uint32())
	scope := resource.NewTierScope(tierID)
	conf := ClientConfig{Addr: hostAddr, TLSConfig: tlsConfig(), Scope: scope}
	rdb, err := conf.Materialize()
	assert.NoError(t, err)
	t.Run("integration_get_set_del", func(t *testing.T) { testClient(t, rdb.(Client)) })
	t.Run("integration_mget", func(t *testing.T) { testMGet(t, rdb.(Client)) })
}

func TestMultiSetTTL(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.RealmID(rand.Uint32())
	scope := resource.NewTierScope(tierID)
	conf := ClientConfig{Addr: hostAddr, TLSConfig: tlsConfig(), Scope: scope}
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
	ttl2 := 5*time.Second + ttl1
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
	time.Sleep(ttl2 - ttl1)
	_, err = c.Get(ctx, k2)
	assert.Equal(t, redis.Nil, err)
}

func TestMultiGetSet(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.RealmID(rand.Uint32())
	scope := resource.NewTierScope(tierID)
	conf := ClientConfig{Addr: hostAddr, TLSConfig: tlsConfig(), Scope: scope}
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
		{"invalid_url", ClientConfig{"some_random.aws.com:6379", tlsConfig(), scope}},
		// i.e. include the url without the port
		{"no_port", ClientConfig{strings.Split(hostAddr, ":6379")[0], tlsConfig(), scope}},
		// i.e. valid url but without tls config
		{"no_tls", ClientConfig{hostAddr, nil, scope}},
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

func TestDeleteCrossSlot(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.RealmID(rand.Uint32())
	scope := resource.NewTierScope(tierID)
	conf := ClientConfig{Addr: hostAddr, TLSConfig: tlsConfig(), Scope: scope}
	rdb, err := conf.Materialize()
	assert.NoError(t, err)

	ctx := context.Background()
	c := rdb.(Client)
	ttl := 5 * time.Second
	keys := []string{"{foo1}:1", "{foo2}:2", "{foo3}:3"}
	vals := []interface{}{"v1", "v2", "v3"}

	// set key works initially
	assert.NoError(t, c.MSet(ctx, keys, vals, []time.Duration{ttl, ttl, ttl}))

	// get to see if the values are set correctly
	actual, err := c.MGet(ctx, keys...)
	assert.NoError(t, err)
	assert.Equal(t, actual, vals)

	// delete the keys now
	assert.NoError(t, c.Del(ctx, keys...))

	// nothing should exist now
	noVal, err := c.MGet(ctx, keys...)
	assert.NoError(t, err)
	assert.Equal(t, noVal, []interface{}{redis.Nil, redis.Nil, redis.Nil})
}

func TestSetNX(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.RealmID(rand.Uint32())
	scope := resource.NewTierScope(tierID)
	conf := ClientConfig{Addr: hostAddr, TLSConfig: tlsConfig(), Scope: scope}
	rdb, err := conf.Materialize()
	assert.NoError(t, err)

	ctx := context.Background()
	c := rdb.(Client)
	ttl := 5 * time.Second

	// set key works initially
	ok, err := c.SetNX(ctx, "key", 1, ttl)
	assert.NoError(t, err)
	assert.Equal(t, true, ok)

	// setting again should not work
	ok, err = c.SetNX(ctx, "key", 1, ttl)
	assert.NoError(t, err)
	assert.Equal(t, false, ok)

	// sleep and set again; works because key expired
	time.Sleep(ttl)
	ok, err = c.SetNX(ctx, "key", 1, ttl)
	assert.NoError(t, err)
	assert.Equal(t, true, ok)
}

func TestSetNXPipelined(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.RealmID(rand.Uint32())
	scope := resource.NewTierScope(tierID)
	conf := ClientConfig{Addr: hostAddr, TLSConfig: tlsConfig(), Scope: scope}
	rdb, err := conf.Materialize()
	assert.NoError(t, err)

	ctx := context.Background()
	c := rdb.(Client)
	n := 5
	ttl := 5 * time.Second
	ttls := make([]time.Duration, n)
	keys := make([]string, n)
	values := make([]interface{}, n)
	exp := make([]SetReturnType, n)

	// try setting with no elements
	_, err = c.SetNXPipelined(ctx, nil, nil, nil)
	assert.NoError(t, err)

	// set 5 keys, all should succeed
	for i := 0; i < n; i++ {
		ttls[i] = ttl
		keys[i] = strconv.Itoa(i)
		values[i] = 1
		exp[i] = NotFoundSet
	}
	ok, err := c.SetNXPipelined(ctx, keys, values, ttls)
	assert.NoError(t, err)
	assert.Equal(t, exp, ok)

	// try setting the same 5 keys again, should fail because already set
	for i := 0; i < n; i++ {
		exp[i] = FoundSkip
	}
	ok, err = c.SetNXPipelined(ctx, keys, values, ttls)
	assert.NoError(t, err)
	assert.Equal(t, exp, ok)

	// sleep until keys expire and try again, should succeed
	for i := 0; i < n; i++ {
		exp[i] = NotFoundSet
	}
	time.Sleep(ttl)
	ok, err = c.SetNXPipelined(ctx, keys, values, ttls)
	assert.NoError(t, err)
	assert.Equal(t, exp, ok)

	// try setting some new keys, only new keys should set successfully
	keys = []string{"3", "4", "5", "6", "7"}
	exp = []SetReturnType{FoundSkip, FoundSkip, NotFoundSet, NotFoundSet, NotFoundSet}
	ok, err = c.SetNXPipelined(ctx, keys, values, ttls)
	assert.NoError(t, err)
	assert.Equal(t, exp, ok)

	// try pipelining with multiple instances of same keys
	keys = []string{"a", "b", "a", "b", "a"}
	ok, err = c.SetNXPipelined(ctx, keys, values, ttls)
	assert.NoError(t, err)
	count := 0
	for i := range ok {
		if ok[i] == NotFoundSet {
			count++
		}
	}
	// number of set keys should be the number of unique keys
	assert.Equal(t, 2, count)
}

func TestHashmap(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.RealmID(rand.Uint32())
	scope := resource.NewTierScope(tierID)
	conf := ClientConfig{Addr: hostAddr, TLSConfig: tlsConfig(), Scope: scope}
	rdb, err := conf.Materialize()
	assert.NoError(t, err)

	ctx := context.Background()
	c := rdb.(Client)

	n := 10
	keys := make([]string, n)
	values := make([]map[string]interface{}, n)
	expected0 := make([]map[string]string, n)
	expected := make([]map[string]string, n)
	ttls := make([]time.Duration, n)

	for i := 0; i < 10; i++ {
		keys[i] = strconv.Itoa(i)
		ttls[i] = 10 * time.Second
		values[i] = make(map[string]interface{}, 10)
		expected0[i] = map[string]string{}
		expected[i] = make(map[string]string, 10)
		for j := 0; j < 10; j++ {
			values[i][strconv.Itoa(j)] = strconv.Itoa(i*10 + j)
			expected[i][strconv.Itoa(j)] = strconv.Itoa(i*10 + j)
		}
	}

	// no error if no keys are given
	err = c.HSetPipelined(ctx, nil, nil, nil)
	found, err := c.HGetAllPipelined(ctx)
	assert.NoError(t, err)
	assert.Empty(t, found)

	// should get nothing initially
	found, err = c.HGetAllPipelined(ctx, keys...)
	assert.NoError(t, err)
	assert.Equal(t, expected0, found)

	// set some keys
	err = c.HSetPipelined(ctx, keys, values, ttls)
	assert.NoError(t, err)

	// should get them
	found, err = c.HGetAllPipelined(ctx, keys...)
	assert.NoError(t, err)
	assert.Equal(t, expected, found)

	// now test TTL
	time.Sleep(5 * time.Second)

	// resetting keys should not change ttl if ttl is 0
	err = c.HSetPipelined(ctx, keys, values, make([]time.Duration, n))
	assert.NoError(t, err)

	time.Sleep(6 * time.Second)

	// keys should have expired now
	found, err = c.HGetAllPipelined(ctx, keys...)
	assert.NoError(t, err)
	assert.Equal(t, expected0, found)
}
