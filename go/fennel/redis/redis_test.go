package redis

import (
	"context"
	"crypto/tls"
	"flag"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

var integration = flag.Bool("integration", false, "flag to indicate whether to run integration tests")
var testRedisAddr = flag.String("addr", "clustercfg.redis-db-e5ae558.sumkzb.memorydb.ap-south-1.amazonaws.com:6379", "address of test redis cluster")

func testClient(t *testing.T, rdb *redis.Client) {
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
	if !*integration {
		t.SkipNow()
	}
	rdb := NewClient(*testRedisAddr, &tls.Config{})
	testClient(t, rdb)
}

func TestRedisClientLocal(t *testing.T) {
	// This server is never explicitly shutdown. Instead, we depend on the process
	// termination after the tests are run for cleanup.
	mr, err := miniredis.Run()
	defer mr.Close()
	assert.NoError(t, err)
	testClient(t, NewClient(mr.Addr(), nil))
}
