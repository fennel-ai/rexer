//go:build !integration

package counter

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"

	libcounter "fennel/lib/counter"
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

	var aggId ftypes.AggId = 123
	retention := 3 * 24 * 3600
	store := twoLevelRedisStore{
		period:    24 * 3600,
		retention: uint64(retention),
	}
	bucketLists := []libcounter.BucketList{
		{Key: "k1", Window: ftypes.Window_DAY, Width: 1, StartIndex: 5, EndIndex: 5},
		{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 9, EndIndex: 9},
		{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 8, EndIndex: 8},
		{Key: "k1", Window: ftypes.Window_MINUTE, Width: 6, StartIndex: 480, EndIndex: 480},
		{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 8, EndIndex: 8},
	}
	vals := []value.Value{
		value.Int(1), value.Int(2), value.Int(3), value.Int(4), value.Int(5),
	}
	// set buckets
	assert.NoError(t, store.SetMulti(ctx, tier, []ftypes.AggId{aggId}, toBuckets([][]libcounter.BucketList{bucketLists}), [][]value.Value{vals}))

	// check it went through
	z := value.Int(0)
	found, err := store.GetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]libcounter.BucketList{bucketLists}, []value.Value{z})
	assert.NoError(t, err)
	assert.Len(t, found[0], len(bucketLists))
	for i, v := range vals {
		assert.Equal(t, found[0][i], v)
	}

	// now push time forward to just before retention
	mr.FastForward(time.Second*time.Duration(retention) - 10)

	// all values should be same for now
	found, err = store.GetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]libcounter.BucketList{bucketLists}, []value.Value{z})
	assert.NoError(t, err)
	assert.Len(t, found[0], len(bucketLists))
	for i, v := range vals {
		assert.Equal(t, found[0][i], v)
	}
	// now fast-forward barely beyond retention
	mr.FastForward(11)
	// and now all keys should be gone
	found, err = store.GetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]libcounter.BucketList{bucketLists}, []value.Value{z})
	assert.NoError(t, err)
	assert.Len(t, found[0], len(bucketLists))
	for _, v := range found[0] {
		assert.Equal(t, z, v)
	}
}
