package redis

import (
	"context"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"github.com/alicebob/miniredis/v2"
	"math/rand"
	"testing"
	"time"

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
	tierID := ftypes.TierID(rand.Uint32())
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	client, err := MiniRedisConfig{MiniRedis: mr}.Materialize(tierID)
	assert.NoError(t, err)
	defer client.Close()
	t.Run("local_get_set", func(t *testing.T) {
		testClient(t, client.(Client))
	})
	t.Run("delete_multi", func(t *testing.T) {
		testDeleteMulti(t, client.(Client))
	})
}
