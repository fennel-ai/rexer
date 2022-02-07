package redis

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func testClient(t *testing.T, rdb Client) {
	var ctx = context.Background()

	err := rdb.Set(ctx, "key", "myvalue", 0).Err()
	assert.NoError(t, err)

	defer rdb.Del(ctx, "key")

	val, err := rdb.Get(ctx, "key").Result()
	assert.NoError(t, err)
	assert.Equal(t, "myvalue", val)

	_, err = rdb.Get(ctx, "key2").Result()
	assert.Equal(t, err, redis.Nil)
}

func TestRedisClientLocal(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	client, err := MiniRedisConfig{MiniRedis: mr}.Materialize()
	assert.NoError(t, err)
	defer client.Close()
	testClient(t, client.(Client))
}
