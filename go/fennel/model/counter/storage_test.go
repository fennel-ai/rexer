package counter

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/mtraver/base91"
	"github.com/stretchr/testify/assert"

	"fennel/lib/codex"
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/lib/utils/binary"

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
		testLarge(t, twoLevelRedisStore{period: 24 * 3600, retention: 30 * 3600}, 30, 1000)
	})
}

func TestRedisKeyEncodeDecode(t *testing.T) {
	g := twoLevelRedisStore{
		period:    8 * 3600,
		retention: 3 * 24 * 3600,
	}
	rKey, err := g.redisKey(group{aggId: ftypes.AggId(12), key: "foobar", id: 12345})
	assert.NoError(t, err)

	// TODO(mohit): Potentially move this to a library which could be helpful for tools which can use this functionality

	// decode aggId
	s := strings.Split(rKey, redisKeyDelimiter)
	assert.True(t, len(s) == 3)
	{
		b, err := base91.StdEncoding.DecodeString(s[0])
		assert.NoError(t, err)
		aggId, _, err := binary.ReadUvarint(b)
		assert.NoError(t, err)
		assert.Equal(t, aggId, uint64(ftypes.AggId(12)))
	}
	// decode codec
	{
		b, err := base91.StdEncoding.DecodeString(s[1])
		assert.NoError(t, err)
		codec, _, err := codex.Read(b)
		assert.NoError(t, err)
		assert.Equal(t, codec, counterCodec)
	}
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
				{Key: "k1", Window: ftypes.Window_DAY, Width: 1, Index: 5},
				{Key: "k2", Window: ftypes.Window_HOUR, Width: 4, Index: 8},
			},
			value.Int(0),
			[]value.Value{value.String("hi"), value.Int(5)},
			[]value.Value{value.Nil, value.Int(4)},
		},
		{
			[]counter.Bucket{
				{Key: "k1", Window: ftypes.Window_DAY, Width: 1, Index: 5},
				{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 7},
				{Key: "k3", Window: ftypes.Window_HOUR, Width: 6, Index: 8},
			},
			value.Int(0),
			[]value.Value{value.String("hi"), value.Nil, value.Int(51)},
			[]value.Value{value.Nil, value.Int(4), value.Int(1)},
		},
		{
			[]counter.Bucket{
				{Key: "k1", Window: ftypes.Window_DAY, Width: 1, Index: 5},
				{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, Index: 9},
				{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, Index: 8},
				{Key: "k1", Window: ftypes.Window_MINUTE, Width: 6, Index: 480},
				{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 8},
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
		found, err := store.GetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]counter.Bucket{scene.buckets}, []value.Value{scene.z})
		assert.NoError(t, err)
		assert.Len(t, found[0], len(scene.buckets))
		for _, v := range found[0] {
			assert.Equal(t, scene.z, v)
		}
		// set values
		var vals []value.Value
		for i := range scene.buckets {
			vals = append(vals, scene.v1[i])
		}
		assert.NoError(t, store.SetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]counter.Bucket{scene.buckets}, [][]value.Value{vals}))

		// check it went through
		found, err = store.GetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]counter.Bucket{scene.buckets}, []value.Value{scene.z})
		assert.NoError(t, err)
		assert.Len(t, found[0], len(scene.buckets))
		for i, v := range found[0] {
			assert.Equal(t, vals[i], v)
		}

		// now only update odd buckets
		odd := make([]counter.Bucket, 0)
		vals2 := make([]value.Value, 0)
		for i := range scene.buckets {
			if i%2 == 0 {
				continue
			}
			vals2 = append(vals2, scene.v2[i])
			odd = append(odd, scene.buckets[i])
		}
		assert.NoError(t, store.SetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]counter.Bucket{odd}, [][]value.Value{vals2}))
		found, err = store.GetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]counter.Bucket{scene.buckets}, []value.Value{scene.z})
		assert.NoError(t, err)
		assert.Len(t, found[0], len(scene.buckets))
		for i := range scene.buckets {
			if i%2 == 0 {
				assert.Equal(t, scene.v1[i], found[0][i])
			} else {
				assert.Equal(t, scene.v2[i], found[0][i])
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
			{Key: "k1", Window: ftypes.Window_DAY, Width: 1, Index: 5},
			{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, Index: 7},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 8},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 9},
			{Key: "k3", Window: ftypes.Window_HOUR, Width: 6, Index: 406},
		},
		{
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 8},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 9},
			{Key: "k3", Window: ftypes.Window_HOUR, Width: 6, Index: 406},
		},
		{
			{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, Index: 7},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 8},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 9},
			{Key: "k3", Window: ftypes.Window_HOUR, Width: 6, Index: 406},
		},
		{
			{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, Index: 0},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 8},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, Index: 9},
			{Key: "k3", Window: ftypes.Window_HOUR, Width: 6, Index: 406},
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
			assert.Len(t, vals[i], len(buckets[i]), fmt.Sprintf("i: %d, expected: %d, found: %d", i, len(buckets[i]), len(vals[i])))
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
	err = store.SetMulti(ctx, tier, ids, buckets, expected)
	assert.NoError(t, err)
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
	vals := make([][]value.Value, numAggs)
	for i := range ids {
		ids[i] = ftypes.AggId(rand.Intn(1000000))
		buckets[i] = make([]counter.Bucket, numBuckets)
		vals[i] = make([]value.Value, numBuckets)
		for j := range buckets[i] {
			buckets[i][j] = counter.Bucket{
				Key:    utils.RandString(30),
				Window: ftypes.Window_HOUR,
				Width:  3,
				Index:  uint64(j),
			}
			vals[i][j] = value.NewList(value.Int(1), value.Int(2))
		}
	}
	assert.NoError(t, store.SetMulti(ctx, tier, ids, buckets, vals))
	defaults := make([]value.Value, len(ids))
	for i := range defaults {
		defaults[i] = value.Nil
	}
	found, err := store.GetMulti(ctx, tier, ids, buckets, defaults)
	assert.NoError(t, err)
	for i := range ids {
		for j := range buckets[i] {
			assert.Equal(t, vals[i][j], found[i][j])
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
			counter.Bucket{Key: k, Window: ftypes.Window_MINUTE, Width: 2, Index: 3},
			slot{g: group{aggId: aggId, key: k, id: 0}, window: ftypes.Window_MINUTE, width: 2, idx: 3, val: value.Int(1)},
			false,
		},
		{
			counter.Bucket{Key: k, Window: ftypes.Window_DAY, Width: 2, Index: 3},
			slot{},
			true,
		},
		{
			counter.Bucket{Key: k, Window: ftypes.Window_HOUR, Width: 2, Index: 30},
			slot{g: group{aggId: aggId, key: k, id: 7}, window: ftypes.Window_HOUR, width: 2, idx: 2, val: value.Int(1)},
			false,
		},
		{
			counter.Bucket{Key: k, Window: ftypes.Window_HOUR, Width: 2, Index: 24 * 30},
			slot{g: group{aggId: aggId, key: k, id: 180}, window: ftypes.Window_HOUR, width: 2, idx: 0, val: value.Int(1)},
			false,
		},
	}
	for _, scene := range scenarios {
		s, err := g.toSlot(aggId, scene.s.val, &scene.b)
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, scene.s, s)
		}
	}
}
