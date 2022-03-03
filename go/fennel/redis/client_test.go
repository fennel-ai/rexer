package redis

import (
	"context"
	"math/rand"
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
}
