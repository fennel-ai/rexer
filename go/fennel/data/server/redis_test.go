package main_test

import (
	"context"
	"crypto/tls"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestRedisClient(t *testing.T) {
	var ctx = context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:      "clustercfg.redis-db-e5ae558.sumkzb.memorydb.ap-south-1.amazonaws.com:6379",
		Password:  "",            // no password set
		DB:        0,             // use default DB
		TLSConfig: &tls.Config{}, // use TLS.
	})

	err := rdb.Set(ctx, "key", "myvalue", 0).Err()
	assert.NoError(t, err)

	defer rdb.Del(ctx, "key")

	val, err := rdb.Get(ctx, "key").Result()
	assert.NoError(t, err)
	assert.Equal(t, "myvalue", val)

	_, err = rdb.Get(ctx, "key2").Result()
	assert.Equal(t, err, redis.Nil)
}
