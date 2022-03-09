package counter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/test"
)

func TestFlatRedisStorage(t *testing.T) {
	t.Parallel()
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	scenarios := []struct {
		store   BucketStore
		buckets []Bucket
		z       value.Value
		v1      []value.Value
		v2      []value.Value
	}{
		{
			FlatRedisStorage{name: "name1"},
			[]Bucket{
				{Key: "k1", Window: ftypes.Window_DAY, Width: 1, Index: 5, Value: nil},
				{Key: "k2", Window: ftypes.Window_HOUR, Width: 7, Index: 8, Value: nil},
			},
			value.Int(0),
			[]value.Value{value.String("hi"), value.Int(5)},
			[]value.Value{value.Nil, value.Int(4)},
		},
	}
	for _, scene := range scenarios {
		// initially nothing is found
		found, err := scene.store.Get(ctx, tier, scene.buckets, scene.z)
		assert.NoError(t, err)
		assert.Len(t, found, len(scene.buckets))
		for _, v := range found {
			assert.Equal(t, scene.z, v)
		}
		// set values
		for i := range scene.buckets {
			scene.buckets[i].Value = scene.v1[i]
		}
		assert.NoError(t, scene.store.Set(ctx, tier, scene.buckets))

		// check it went through
		found, err = scene.store.Get(ctx, tier, scene.buckets, scene.z)
		assert.NoError(t, err)
		assert.Len(t, found, len(scene.buckets))
		for i, v := range found {
			assert.Equal(t, scene.v1[i], v)
		}

		// now only update odd buckets
		odd := make([]Bucket, 0)
		for i := range scene.buckets {
			if i%2 == 0 {
				continue
			}
			scene.buckets[i].Value = scene.v2[i]
			odd = append(odd, scene.buckets[i])
		}
		assert.NoError(t, scene.store.Set(ctx, tier, odd))
		found, err = scene.store.Get(ctx, tier, scene.buckets, scene.z)
		assert.NoError(t, err)
		assert.Len(t, found, len(scene.buckets))
		for i := range scene.buckets {
			if i%2 == 0 {
				assert.Equal(t, scene.v1[i], found[i])
			} else {
				assert.Equal(t, scene.v2[i], found[i])
			}
		}
	}
}
