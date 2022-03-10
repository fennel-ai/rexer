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

func TestStorage(t *testing.T) {
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
		{
			twoLevelRedisStore{
				name:      "name2",
				period:    24 * 3600,
				retention: 0,
			},
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
			twoLevelRedisStore{
				name:      "name3",
				period:    24 * 3600,
				retention: 0,
			},
			[]Bucket{
				{Key: "k1", Window: ftypes.Window_DAY, Width: 1, Index: 5, Value: nil},
				{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, Index: 9, Value: nil},
				{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, Index: 8, Value: nil},
				{Key: "k1", Window: ftypes.Window_MINUTE, Width: 6, Index: 480, Value: nil},
				{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 8, Value: nil},
			},
			value.Int(0),
			[]value.Value{value.String("hi"), value.Nil, value.Int(51), value.List{value.Int(1)}, value.Dict{"hi": value.Nil}},
			[]value.Value{value.Nil, value.Int(4), value.Int(1), value.Int(2), value.Int(3)},
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

func TestTwoLevelRedisStore(t *testing.T) {
	g := twoLevelRedisStore{
		name:      "something",
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
			slot{g: group{key: k, id: 0}, window: ftypes.Window_MINUTE, width: 2, idx: 3, val: value.Int(1)},
			fmt.Sprintf("%v%v", toBuf(2), toBuf(3)),
			fmt.Sprintf("agg_l2:%s:%s:%v%v", g.name, k, toBuf(8*3600), toBuf(0)),
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
			slot{g: group{key: k, id: 7}, window: ftypes.Window_HOUR, width: 2, idx: 2, val: value.Int(1)},
			fmt.Sprintf("%d:%v%v", ftypes.Window_HOUR, toBuf(2), toBuf(2)),
			fmt.Sprintf("agg_l2:%s:%s:%v%v", g.name, k, toBuf(8*3600), toBuf(7)),
			false,
		},
		{
			Bucket{Key: k, Window: ftypes.Window_HOUR, Width: 2, Index: 24 * 30, Value: value.Int(1)},
			slot{g: group{key: k, id: 180}, window: ftypes.Window_HOUR, width: 2, idx: 0, val: value.Int(1)},
			fmt.Sprintf("%d:%v%v", ftypes.Window_HOUR, toBuf(2), toBuf(0)),
			fmt.Sprintf("agg_l2:%s:%s:%v%v", g.name, k, toBuf(8*3600), toBuf(180)),
			false,
		},
	}
	for _, scene := range scenarios {
		s, err := g.toSlot(&scene.b)
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, scene.s, s)
			assert.Equal(t, scene.slotkey, g.slotKey(s))
			assert.Equal(t, scene.rkey, g.redisKey(s.g))
		}
	}
}

func BenchmarkStorage(b *testing.B) {
	tier, err := test.Tier()
	assert.NoError(b, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	buckets := make([]Bucket, 0)
	groupKey := utils.RandString(30)
	for i := 0; i < 1000; i++ {
		b := Bucket{
			Key:    fmt.Sprintf("%s:%d", groupKey, i/50),
			Window: ftypes.Window_MINUTE,
			Width:  6,
			Index:  uint64(i),
			Value:  value.Int(i),
		}
		buckets = append(buckets, b)
	}
	s1 := FlatRedisStorage{name: ftypes.AggName(utils.RandString(30))}
	s2 := twoLevelRedisStore{
		name:      ftypes.AggName(utils.RandString(30)),
		period:    24 * 3600,
		retention: 0,
	}
	s1.Set(ctx, tier, buckets)
	s2.Set(ctx, tier, buckets)
}
