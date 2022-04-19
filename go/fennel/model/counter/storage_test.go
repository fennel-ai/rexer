package counter

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/lib/value"
	"fennel/test"
)

func TestTwoLevelRedisStore_Get(t *testing.T) {
	t.Parallel()
	t.Run("test_basic", func(t *testing.T) {
		testStorage(t, twoLevelRedisStore{period: 24 * 3600, retention: 0})
	})
	t.Run("test_multi", func(t *testing.T) {
		testStorageMulti(t, twoLevelRedisStore{period: 24 * 3600, retention: 0})
	})
	t.Run("test_large", func(t *testing.T) {
		testLarge(t, twoLevelRedisStore{period: 24 * 3600, retention: 30 * 3600}, 20, 1000)
	})
}

func TestBadgerStorage(t *testing.T) {
	t.Parallel()
	t.Run("test_basic", func(t *testing.T) {
		testStorage(t, BadgerStorage{})
	})
	t.Run("test_multi", func(t *testing.T) {
		testStorageMulti(t, BadgerStorage{})
	})
	t.Run("test_large", func(t *testing.T) {
		testLarge(t, BadgerStorage{}, 20, 1000)
	})

	t.Run("test_encode_decode", func(t *testing.T) {
		buckets := []counter.Bucket{
			{"hello", ftypes.Window_HOUR, 11, 123, nil},
			{"hello", ftypes.Window_MINUTE, 11, 12323, nil},
			{"hello", ftypes.Window_DAY, 1, 123, nil},
		}
		for _, b := range buckets {
			aggId := ftypes.AggId(rand.Intn(1000000))
			buf, err := badgerEncode(aggId, b)
			assert.NoError(t, err)
			found_name, found_bucket, err := badgerDecode(buf)
			assert.NoError(t, err)
			assert.Equal(t, aggId, found_name)
			assert.Equal(t, b, found_bucket)
		}
	})
}

func testStorage(t *testing.T, store BucketStore) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	scenarios := []struct {
		buckets []counter.Bucket
		z       value.Value
		v1      []value.Value
		v2      []value.Value
	}{
		{
			[]counter.Bucket{
				{Key: "k1", Window: ftypes.Window_DAY, Width: 1, Index: 5, Value: nil},
				{Key: "k2", Window: ftypes.Window_HOUR, Width: 4, Index: 8, Value: nil},
			},
			value.Int(0),
			[]value.Value{value.String("hi"), value.Int(5)},
			[]value.Value{value.Nil, value.Int(4)},
		},
		{
			[]counter.Bucket{
				{Key: "k1", Window: ftypes.Window_DAY, Width: 1, Index: 5, Value: nil},
				{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 7, Value: nil},
				{Key: "k3", Window: ftypes.Window_HOUR, Width: 6, Index: 8, Value: nil},
			},
			value.Int(0),
			[]value.Value{value.String("hi"), value.Nil, value.Int(51)},
			[]value.Value{value.Nil, value.Int(4), value.Int(1)},
		},
		{
			[]counter.Bucket{
				{Key: "k1", Window: ftypes.Window_DAY, Width: 1, Index: 5, Value: nil},
				{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, Index: 9, Value: nil},
				{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, Index: 8, Value: nil},
				{Key: "k1", Window: ftypes.Window_MINUTE, Width: 6, Index: 480, Value: nil},
				{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 8, Value: nil},
			},
			value.Int(0),
			[]value.Value{value.String("hi"), value.Nil, value.Int(51), value.NewList(value.Int(1)), value.NewDict(map[string]value.Value{"hi": value.Nil})},
			[]value.Value{value.Nil, value.Int(4), value.Int(1), value.Int(2), value.Int(3)},
		},
	}
	for _, scene := range scenarios {
		// user random strings as names so that tests don't fail due to name collisions
		aggId := ftypes.AggId(rand.Intn(1000000))
		// initially nothing is found
		found, err := store.Get(ctx, tier, aggId, scene.buckets, scene.z)
		assert.NoError(t, err)
		assert.Len(t, found, len(scene.buckets))
		for _, v := range found {
			assert.Equal(t, scene.z, v)
		}
		// set values
		for i := range scene.buckets {
			scene.buckets[i].Value = scene.v1[i]
		}
		assert.NoError(t, store.Set(ctx, tier, aggId, scene.buckets))

		// check it went through
		found, err = store.Get(ctx, tier, aggId, scene.buckets, scene.z)
		assert.NoError(t, err)
		assert.Len(t, found, len(scene.buckets))
		for i, v := range found {
			assert.Equal(t, scene.v1[i], v)
		}

		// now only update odd buckets
		odd := make([]counter.Bucket, 0)
		for i := range scene.buckets {
			if i%2 == 0 {
				continue
			}
			scene.buckets[i].Value = scene.v2[i]
			odd = append(odd, scene.buckets[i])
		}
		assert.NoError(t, store.Set(ctx, tier, aggId, odd))
		found, err = store.Get(ctx, tier, aggId, scene.buckets, scene.z)
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

func testStorageMulti(t *testing.T, store BucketStore) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	ids := []ftypes.AggId{
		1,
		2,
		3,
		4,
		5,
	}
	buckets := [][]counter.Bucket{
		{},
		{
			{Key: "k1", Window: ftypes.Window_DAY, Width: 1, Index: 5, Value: nil},
			{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, Index: 7, Value: nil},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 8, Value: nil},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 9, Value: nil},
			{Key: "k3", Window: ftypes.Window_HOUR, Width: 6, Index: 406, Value: nil},
		},
		{
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 8, Value: nil},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 9, Value: nil},
			{Key: "k3", Window: ftypes.Window_HOUR, Width: 6, Index: 406, Value: nil},
		},
		{
			{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, Index: 7, Value: nil},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 8, Value: nil},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 9, Value: nil},
			{Key: "k3", Window: ftypes.Window_HOUR, Width: 6, Index: 406, Value: nil},
		},
		{
			{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, Index: 0, Value: nil},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 8, Value: nil},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 9, Value: nil},
			{Key: "k3", Window: ftypes.Window_HOUR, Width: 6, Index: 406, Value: nil},
		},
	}
	defaults := []value.Value{
		value.Nil,
		value.Int(0),
		value.Double(0.0),
		value.String(""),
		value.String(""),
	}
	// initially nothing to be found
	vals, err := store.GetMulti(ctx, tier, ids, buckets, defaults)
	assert.NoError(t, err)
	assert.Equal(t, len(buckets), len(vals))
	for i := range buckets {
		assert.Equal(t, len(vals[i]), len(buckets[i]))
		for j := range buckets[i] {
			assert.True(t, defaults[i].Equal(vals[i][j]))
		}
	}
	expected := [][]value.Value{
		{},
		{value.Int(1), value.Int(2), value.Int(3), value.Int(4), value.Int(5)},
		{value.Double(1.0), value.Double(2.0), value.Double(3.0)},
		{value.String("a"), value.String("b"), value.String("c"), value.String("d")},
		{value.String("z"), value.String("b"), value.String("c"), value.String("d")},
	}
	for i := range buckets {
		for j := range buckets[i] {
			buckets[i][j].Value = expected[i][j]
		}
	}
	err = store.SetMulti(ctx, tier, ids, buckets)
	assert.NoError(t, err)
	for i := range buckets {
		for j := range buckets[i] {
			buckets[i][j].Value = nil
		}
	}
	found, err := store.GetMulti(ctx, tier, ids, buckets, defaults)
	assert.NoError(t, err)
	assert.Equal(t, len(expected), len(found))
	for i := range found {
		assert.Equal(t, len(expected[i]), len(found[i]))
		for j := range found[i] {
			assert.True(t, expected[i][j].Equal(found[i][j]))
		}
	}
}

func testLarge(t *testing.T, store BucketStore, numAggs, numBuckets int) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	ids := make([]ftypes.AggId, numAggs)
	buckets := make([][]counter.Bucket, numAggs)
	for i := range ids {
		ids[i] = ftypes.AggId(rand.Intn(1000000))
		buckets[i] = make([]counter.Bucket, numBuckets)
		for j := range buckets[i] {
			buckets[i][j] = counter.Bucket{
				Key:    utils.RandString(30),
				Window: ftypes.Window_HOUR,
				Width:  3,
				Index:  uint64(j),
				Value:  value.NewList(value.Int(1), value.Int(2)),
			}
		}
	}
	assert.NoError(t, store.SetMulti(ctx, tier, ids, buckets))
	defaults := make([]value.Value, len(ids))
	for i := range defaults {
		defaults[i] = value.Nil
	}
	found, err := store.GetMulti(ctx, tier, ids, buckets, defaults)
	assert.NoError(t, err)
	for i := range ids {
		for j := range buckets[i] {
			assert.Equal(t, buckets[i][j].Value, found[i][j])
		}
	}
}

func TestTwoLevelRedisStore(t *testing.T) {
	var aggId ftypes.AggId = 202
	g := twoLevelRedisStore{
		period:    8 * 3600,
		retention: 3 * 24 * 3600,
	}
	k := "key"
	scenarios := []struct {
		b   counter.Bucket
		s   slot
		err bool
	}{
		{
			counter.Bucket{Key: k, Window: ftypes.Window_MINUTE, Width: 2, Index: 3, Value: value.Int(1)},
			slot{g: group{aggId: aggId, key: k, id: 0}, window: ftypes.Window_MINUTE, width: 2, idx: 3, val: value.Int(1)},
			false,
		},
		{
			counter.Bucket{Key: k, Window: ftypes.Window_DAY, Width: 2, Index: 3, Value: value.Int(1)},
			slot{},
			true,
		},
		{
			counter.Bucket{Key: k, Window: ftypes.Window_HOUR, Width: 2, Index: 30, Value: value.Int(1)},
			slot{g: group{aggId: aggId, key: k, id: 7}, window: ftypes.Window_HOUR, width: 2, idx: 2, val: value.Int(1)},
			false,
		},
		{
			counter.Bucket{Key: k, Window: ftypes.Window_HOUR, Width: 2, Index: 24 * 30, Value: value.Int(1)},
			slot{g: group{aggId: aggId, key: k, id: 180}, window: ftypes.Window_HOUR, width: 2, idx: 0, val: value.Int(1)},
			false,
		},
	}
	for _, scene := range scenarios {
		s, err := g.toSlot(aggId, &scene.b)
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, scene.s, s)
		}
	}
}

func benchmarkStorage(b *testing.B, store BucketStore) {
	fmt.Printf("inside benchmark...\n")
	tier, err := test.Tier()
	assert.NoError(b, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	buckets := make([]counter.Bucket, 0)
	groupKey := utils.RandString(30)
	for i := 0; i < 10000; i++ {
		b := counter.Bucket{
			Key:    fmt.Sprintf("%s:%d", groupKey, i/50),
			Window: ftypes.Window_MINUTE,
			Width:  6,
			Index:  uint64(i),
			Value:  value.Int(i),
		}
		buckets = append(buckets, b)
	}
	aggId := ftypes.AggId(rand.Intn(1000000))
	store.Set(ctx, tier, aggId, buckets)
	dummy := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vals, _ := store.Get(ctx, tier, aggId, buckets, value.Int(1))
		dummy += len(vals)
	}
}

func BenchmarkStorage(b *testing.B) {
	b.Run("two_level_redis_storage", func(b *testing.B) {
		benchmarkStorage(b, twoLevelRedisStore{period: 24 * 3600})
	})
	b.Run("badger_flat_storage", func(b *testing.B) {
		benchmarkStorage(b, BadgerStorage{})
	})
}
