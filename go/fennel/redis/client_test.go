package redis

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/resource"

	"github.com/alicebob/miniredis/v2"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func testClient(t *testing.T, c Client) {
	ctx := context.Background()

	// initially nothing to show
	_, err := c.Get(ctx, "key")
	assert.Error(t, err)
	assert.Equal(t, redis.Nil, err)
	err = c.Set(ctx, "key", "myvalue", 0)
	assert.NoError(t, err)

	val, err := c.Get(ctx, "key")
	assert.NoError(t, err)
	assert.Equal(t, "myvalue", val)
	err = c.Del(ctx, "key")
	assert.NoError(t, err)

	_, err = c.Get(ctx, "key2")
	assert.Equal(t, err, redis.Nil)

}

func testMGet(t *testing.T, c Client) {
	ctx := context.Background()
	// keys with valid value are returned in MGet. Keys without value get an error
	k1, k2 := "{a}k1", "{a}k2"
	assert.NoError(t, c.Set(ctx, k1, "myvalue", 0))
	vals, err := c.MGet(ctx, k1, k2)
	assert.NoError(t, err)
	assert.Equal(t, "myvalue", vals[0])
	assert.Equal(t, redis.Nil, vals[1])
}

func testMSet(t *testing.T, c Client) {
	ctx := context.Background()
	// empty keys is a no op
	assert.NoError(t, c.MSet(ctx, []string{}, []interface{}{""}, []time.Duration{}))
	// keys with valid value are returned in MGet. Keys without value get an error
	k1, k2 := "{a}k1", "{a}k2"
	v1, v2 := "value1", "value2"
	assert.NoError(t, c.MSet(ctx, []string{k1, k2}, []interface{}{v1, v2}, make([]time.Duration, 2)))
	vals, err := c.MGet(ctx, k1, k2)
	assert.NoError(t, err)
	assert.Equal(t, "value1", vals[0])
	assert.Equal(t, "value2", vals[1])
}

func testDeleteMulti(t *testing.T, c Client) {
	ctx := context.Background()

	ks := make([]string, 100)
	for i := range ks {
		ks[i] = utils.RandString(10)
		err := c.Set(ctx, ks[i], utils.RandString(5), 0)
		assert.NoError(t, err)
	}
	for _, k := range ks {
		_, err := c.Get(ctx, k)
		assert.NoError(t, err)
	}

	// but delete them all together
	err := c.Del(ctx, ks...)
	assert.NoError(t, err)
	for _, k := range ks {
		_, err = c.Get(ctx, k)
		assert.Equal(t, redis.Nil, err)
	}
}

func testSetNX(t *testing.T, c Client) {
	ctx := context.Background()
	ttl := 5 * time.Second

	// set key works initially
	ok, err := c.SetNX(ctx, "key", 1, ttl)
	assert.NoError(t, err)
	assert.Equal(t, true, ok)

	// setting again should not work
	ok, err = c.SetNX(ctx, "key", 1, ttl)
	assert.NoError(t, err)
	assert.Equal(t, false, ok)

	// fastforward and set again; works because key expired
	c.conf.(MiniRedisConfig).MiniRedis.FastForward(ttl)
	ok, err = c.SetNX(ctx, "key", 1, ttl)
	assert.NoError(t, err)
	assert.Equal(t, true, ok)
}

func testSetNXPipelined(t *testing.T, c Client) {
	ctx := context.Background()
	n := 5
	ttl := 5 * time.Second
	ttls := make([]time.Duration, n)
	keys := make([]string, n)
	values := make([]interface{}, n)
	exp := make([]SetReturnType, n)

	// try setting with no elements
	_, err := c.SetNXPipelined(ctx, nil, nil, nil)
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

	// fastforward until keys expire and try again, should succeed
	for i := 0; i < n; i++ {
		exp[i] = NotFoundSet
	}
	c.conf.(MiniRedisConfig).MiniRedis.FastForward(ttl)
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

func testHashmap(t *testing.T, c Client) {
	ctx := context.Background()

	n := 10
	keys := make([]string, n)
	values := make([]map[string]interface{}, n)
	expected0 := make([]map[string]string, n)
	expected := make([]map[string]string, n)
	ttls := make([]time.Duration, n)

	for i := 0; i < 10; i++ {
		keys[i] = "h" + strconv.Itoa(i)
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
	err := c.HSetPipelined(ctx, nil, nil, nil)
	assert.NoError(t, err)
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
}

func TestRedisClientLocal(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.RealmID(rand.Uint32())
	scope := resource.NewTierScope(tierID)
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	client, err := MiniRedisConfig{MiniRedis: mr, Scope: scope}.Materialize()
	assert.NoError(t, err)
	defer client.Close()
	t.Run("local_get_set", func(t *testing.T) { testClient(t, client.(Client)) })
	t.Run("local_delete_multi", func(t *testing.T) { testDeleteMulti(t, client.(Client)) })
	t.Run("local_mget", func(t *testing.T) { testMGet(t, client.(Client)) })
	t.Run("local_mset", func(t *testing.T) { testMSet(t, client.(Client)) })
	t.Run("local_setnx", func(t *testing.T) { testSetNX(t, client.(Client)) })
	t.Run("local_setnx_pipelined", func(t *testing.T) { testSetNXPipelined(t, client.(Client)) })
	t.Run("local_hashmap", func(t *testing.T) { testHashmap(t, client.(Client)) })
}
