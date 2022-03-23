//go:build !integration

package counter

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/redis"
	"fennel/resource"
	"fennel/test"
)

func TestTwoLevelRedisStore_TTL(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	scope := resource.NewTierScope(tier.ID)
	mr, err := miniredis.Run()
	assert.NoError(t, err)
	client, err := redis.MiniRedisConfig{MiniRedis: mr, Scope: scope}.Materialize()
	assert.NoError(t, err)
	oldClient := tier.Redis
	tier.Redis = client.(redis.Client)
	defer oldClient.Close()

	ctx := context.Background()

	h := NewSum("some name", 0)
	retention := 3 * 24 * 3600
	store := twoLevelRedisStore{
		period:    24 * 3600,
		retention: uint64(retention),
	}
	buckets := []Bucket{
		{Key: "k1", Window: ftypes.Window_DAY, Width: 1, Index: 5, Value: value.Int(1)},
		{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, Index: 9, Value: value.Int(2)},
		{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, Index: 8, Value: value.Int(3)},
		{Key: "k1", Window: ftypes.Window_MINUTE, Width: 6, Index: 480, Value: value.Int(4)},
		{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 8, Value: value.Int(5)},
	}
	// set buckets
	assert.NoError(t, store.Set(ctx, tier, h, buckets))

	// check it went through
	z := value.Int(0)
	found, err := store.Get(ctx, tier, h, buckets)
	assert.NoError(t, err)
	assert.Len(t, found, len(buckets))
	for i, v := range found {
		assert.Equal(t, buckets[i].Value, v)
	}

	// now push time forward to just before retention
	mr.FastForward(time.Second*time.Duration(retention) - 10)

	// all values should be same for now
	found, err = store.Get(ctx, tier, h, buckets)
	assert.NoError(t, err)
	assert.Len(t, found, len(buckets))
	for i, v := range found {
		assert.Equal(t, buckets[i].Value, v)
	}
	// now fast-forward barely beyond retention
	mr.FastForward(11)
	// and now all keys should be gone
	found, err = store.Get(ctx, tier, h, buckets)
	assert.NoError(t, err)
	assert.Len(t, found, len(buckets))
	for _, v := range found {
		assert.Equal(t, z, v)
	}
}
