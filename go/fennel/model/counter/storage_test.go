package counter

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/lib/value"
	"fennel/test"
)

func TestFlatRedisStorage(t *testing.T) {
	t.Parallel()
	t.Run("test_basic", func(t *testing.T) {
		testStorage(t, FlatRedisStorage{})
	})
	t.Run("test_multi", func(t *testing.T) {
		testStorageMulti(t, FlatRedisStorage{})
	})
	t.Run("test_large", func(t *testing.T) {
		testLarge(t, FlatRedisStorage{}, 20, 1000)
	})
}

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
		buckets := []Bucket{
			{"hello", ftypes.Window_HOUR, 11, 123, nil},
			{"hello", ftypes.Window_MINUTE, 11, 12323, nil},
			{"hello", ftypes.Window_DAY, 1, 123, nil},
		}
		for _, b := range buckets {
			name := ftypes.AggName(utils.RandString(5))
			buf, err := badgerEncode(name, b)
			assert.NoError(t, err)
			found_name, found_bucket, err := badgerDecode(buf)
			assert.NoError(t, err)
			assert.Equal(t, name, found_name)
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
		buckets []Bucket
		z       value.Value
		v1      []value.Value
		v2      []value.Value
	}{
		{
			[]Bucket{
				{Key: "k1", Window: ftypes.Window_DAY, Width: 1, Index: 5, Value: nil},
				{Key: "k2", Window: ftypes.Window_HOUR, Width: 4, Index: 8, Value: nil},
			},
			value.Int(0),
			[]value.Value{value.String("hi"), value.Int(5)},
			[]value.Value{value.Nil, value.Int(4)},
		},
		{
			[]Bucket{
				{Key: "k1", Window: ftypes.Window_DAY, Width: 1, Index: 5, Value: nil},
				{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 7, Value: nil},
				{Key: "k3", Window: ftypes.Window_HOUR, Width: 6, Index: 8, Value: nil},
			},
			value.Int(0),
			[]value.Value{value.String("hi"), value.Nil, value.Int(51)},
			[]value.Value{value.Nil, value.Int(4), value.Int(1)},
		},
		{
			[]Bucket{
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
		name := ftypes.AggName(utils.RandString(10))
		// initially nothing is found
		found, err := store.Get(ctx, tier, name, scene.buckets, scene.z)
		assert.NoError(t, err)
		assert.Len(t, found, len(scene.buckets))
		for _, v := range found {
			assert.Equal(t, scene.z, v)
		}
		// set values
		for i := range scene.buckets {
			scene.buckets[i].Value = scene.v1[i]
		}
		assert.NoError(t, store.Set(ctx, tier, name, scene.buckets))

		// check it went through
		found, err = store.Get(ctx, tier, name, scene.buckets, scene.z)
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
		assert.NoError(t, store.Set(ctx, tier, name, odd))
		found, err = store.Get(ctx, tier, name, scene.buckets, scene.z)
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

	names := []ftypes.AggName{
		"agg0",
		"agg1",
		"agg2",
		"agg3",
		"agg3",
	}
	buckets := [][]Bucket{
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
	vals, err := store.GetMulti(ctx, tier, names, buckets, defaults)
	assert.NoError(t, err)
	assert.Equal(t, len(buckets), len(vals))
	for i := range buckets {
		assert.Equal(t, len(vals[i]), len(buckets[i]))
		for j := range buckets[i] {
			assert.True(t, defaults[i].Equal(vals[i][j]))
		}
	}
	// set values and check with FlatRedisStorage
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
	err = store.SetMulti(ctx, tier, names, buckets)
	assert.NoError(t, err)
	for i := range buckets {
		for j := range buckets[i] {
			buckets[i][j].Value = nil
		}
	}
	found, err := store.GetMulti(ctx, tier, names, buckets, defaults)
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

	names := make([]ftypes.AggName, numAggs)
	buckets := make([][]Bucket, numAggs)
	for i := range names {
		names[i] = ftypes.AggName(utils.RandString(20))
		buckets[i] = make([]Bucket, numBuckets)
		for j := range buckets[i] {
			buckets[i][j] = Bucket{
				Key:    utils.RandString(30),
				Window: ftypes.Window_HOUR,
				Width:  3,
				Index:  uint64(j),
				Value:  value.NewList(value.Int(1), value.Int(2)),
			}
		}
	}
	assert.NoError(t, store.SetMulti(ctx, tier, names, buckets))
	found, err := store.GetMulti(ctx, tier, names, buckets, make([]value.Value, len(names)))
	assert.NoError(t, err)
	for i := range names {
		for j := range buckets[i] {
			assert.Equal(t, buckets[i][j].Value, found[i][j])
		}
	}
}

func TestTwoLevelRedisStore(t *testing.T) {
	var name ftypes.AggName = "something"
	g := twoLevelRedisStore{
		period:    8 * 3600,
		retention: 3 * 24 * 3600,
	}
	k := "key"
	scenarios := []struct {
		b       Bucket
		s       slot
		slotkey string
		rkey    string
		err     bool
	}{
		{
			Bucket{Key: k, Window: ftypes.Window_MINUTE, Width: 2, Index: 3, Value: value.Int(1)},
			slot{g: group{aggname: name, key: k, id: 0}, window: ftypes.Window_MINUTE, width: 2, idx: 3, val: value.Int(1)},
			fmt.Sprintf("%v%v", toBuf(2), toBuf(3)),
			fmt.Sprintf("agg_l2:%s:%s:%v%v", name, k, toBuf(8*3600), toBuf(0)),
			false,
		},
		{
			Bucket{Key: k, Window: ftypes.Window_DAY, Width: 2, Index: 3, Value: value.Int(1)},
			slot{},
			"",
			"",
			true,
		},
		{
			Bucket{Key: k, Window: ftypes.Window_HOUR, Width: 2, Index: 30, Value: value.Int(1)},
			slot{g: group{aggname: name, key: k, id: 7}, window: ftypes.Window_HOUR, width: 2, idx: 2, val: value.Int(1)},
			fmt.Sprintf("%d:%v%v", ftypes.Window_HOUR, toBuf(2), toBuf(2)),
			fmt.Sprintf("agg_l2:%s:%s:%v%v", name, k, toBuf(8*3600), toBuf(7)),
			false,
		},
		{
			Bucket{Key: k, Window: ftypes.Window_HOUR, Width: 2, Index: 24 * 30, Value: value.Int(1)},
			slot{g: group{aggname: name, key: k, id: 180}, window: ftypes.Window_HOUR, width: 2, idx: 0, val: value.Int(1)},
			fmt.Sprintf("%d:%v%v", ftypes.Window_HOUR, toBuf(2), toBuf(0)),
			fmt.Sprintf("agg_l2:%s:%s:%v%v", name, k, toBuf(8*3600), toBuf(180)),
			false,
		},
	}
	for _, scene := range scenarios {
		s, err := g.toSlot(name, &scene.b)
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, scene.s, s)
			assert.Equal(t, scene.slotkey, g.slotKey(s))
			assert.Equal(t, scene.rkey, g.redisKey(name, s.g))
		}
	}
}

func benchmarkStorage(b *testing.B, store BucketStore) {
	fmt.Printf("inside benchmark...\n")
	tier, err := test.Tier()
	assert.NoError(b, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	buckets := make([]Bucket, 0)
	groupKey := utils.RandString(30)
	for i := 0; i < 10000; i++ {
		b := Bucket{
			Key:    fmt.Sprintf("%s:%d", groupKey, i/50),
			Window: ftypes.Window_MINUTE,
			Width:  6,
			Index:  uint64(i),
			Value:  value.Int(i),
		}
		buckets = append(buckets, b)
	}
	name := ftypes.AggName(utils.RandString(30))
	store.Set(ctx, tier, name, buckets)
	dummy := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vals, _ := store.Get(ctx, tier, name, buckets, value.Int(1))
		dummy += len(vals)
	}
}

func BenchmarkStorage(b *testing.B) {
	b.Run("flat_redis", func(b *testing.B) {
		benchmarkStorage(b, FlatRedisStorage{})
	})
	b.Run("two_level_redis_storage", func(b *testing.B) {
		benchmarkStorage(b, twoLevelRedisStore{period: 24 * 3600})
	})
	b.Run("badger_flat_storage", func(b *testing.B) {
		benchmarkStorage(b, BadgerStorage{})
	})
}
