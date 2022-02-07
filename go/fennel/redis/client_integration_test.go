//go:build integration

package redis

import (
	"context"
	"crypto/tls"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fmt"
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
	t.Run("integration_get_set_del", func(t *testing.T) {
		testClient(t, rdb.(Client))
	})
	t.Run("integration_get_set_multi", func(t *testing.T) {
		testmultigetset(t, rdb.(Client))
	})
}

func testmultigetset(t *testing.T, c Client) {
	ctx := context.Background()

	// no errors when keys are sharded carefully
	m := make(map[string]interface{})
	keys := make([]string, 0)
	shard := utils.RandString(5)
	for j := 0; j < 100; j++ {
		k := fmt.Sprintf("%s{key:%s}%s", utils.RandString(5), shard, utils.RandString(5))
		keys = append(keys, k)
		m[k] = k
	}
	assert.NoError(t, c.MSet(ctx, m))
	vals, err := c.MGet(ctx, keys...)
	assert.NoError(t, err)
	assert.Len(t, vals, 100)
	for i, v := range vals {
		assert.Equal(t, keys[i], v)
	}

	// but we get errors when keys aren't shared carefully
	m = make(map[string]interface{})
	keys = make([]string, 0)
	for j := 0; j < 100; j++ {
		k := fmt.Sprintf("%skey:%d%s", utils.RandString(5), j, utils.RandString(5))
		keys = append(keys, k)
		m[k] = k
	}
	assert.Error(t, c.MSet(ctx, m))
	_, err = c.MGet(ctx, keys...)
	assert.Error(t, err)
}
