package redis

import (
	"context"
	"crypto/tls"
	"flag"
	"github.com/alicebob/miniredis/v2"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

var integration = flag.Bool("integration", false, "flag to indicate whether to run integration tests")
var testRedisAddr = flag.String("addr", "clustercfg.redis-db-e5ae558.sumkzb.memorydb.ap-south-1.amazonaws.com:6379", "address of test redis cluster")

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

func TestRedisClientIntegration(t *testing.T) {
	// TODO: verify this test passes
	if !*integration {
		t.SkipNow()
	}
	conf := ClientConfig{Addr: *testRedisAddr, TLSConfig: &tls.Config{}}
	rdb, err := conf.Materialize()
	assert.NoError(t, err)
	testClient(t, rdb.(Client))
}

func TestRedisClientLocal(t *testing.T) {
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	client, err := MiniRedisConfig{MiniRedis: mr}.Materialize()
	assert.NoError(t, err)
	defer client.Close()
	testClient(t, client.(Client))
}
