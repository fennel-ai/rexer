package counter

import (
	"context"
	"math/rand"
	"strings"
	"testing"

	"fennel/lib/utils/encoding/base91"

	"github.com/stretchr/testify/assert"

	"fennel/lib/codex"
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/lib/utils/binary"

	"fennel/lib/value"
	"fennel/test"
)

func BenchmarkTwoLevelRedisStore(b *testing.B) {
	tier := test.BenchmarkTier()
	defer test.Teardown(tier)

	empty := false

	ctx := context.Background()

	store := twoLevelRedisStore{
		period:    24 * 3600,
		retention: 3 * 24 * 3600,
	}
	bzer := fixedWidthBucketizer{
		sizes:                  map[ftypes.Window]uint32{ftypes.Window_MINUTE: 6, ftypes.Window_DAY: 1},
		includeTrailingPartial: false,
	}

	numAggregates := 10000
	aggIds := make([]ftypes.AggId, numAggregates)
	bucketLists := make([][]counter.BucketList, numAggregates)
	defaults := make([]value.Value, numAggregates)
	//numUnique := 10000 // Case 1
	numUnique := 100 // Case 2, 3
	for i := 0; i < numAggregates; i++ {
		r := i / numUnique // Case 3
		//r := 0 // Case 1, 2
		start := ftypes.Timestamp(36*3600 + r*24*3600)
		finish := ftypes.Timestamp(10*24*3600 + 12*3600 + r*24*3600)
		aggIds[i] = ftypes.AggId(i % numUnique)
		bucketLists[i] = bzer.BucketizeDuration("key", start, finish)
		defaults[i] = value.Int(0)
	}
	if !empty {
		_, err := store.GetMulti(ctx, tier, aggIds, bucketLists, defaults)
		if err != nil {
			panic(err)
		}
	}
	b.ReportAllocs()
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
		testLarge(t, twoLevelRedisStore{period: 24 * 3600, retention: 30 * 3600}, 30, 1000)
	})
}

func TestRedisKeyEncodeDecode(t *testing.T) {
	g := twoLevelRedisStore{
		period:    8 * 3600,
		retention: 3 * 24 * 3600,
	}
	rKey, err := g.redisKey(Group{aggId: ftypes.AggId(12), key: "foobar", id: 12345})
	assert.NoError(t, err)

	// TODO(mohit): Potentially move this to a library which could be helpful for tools which can use this functionality

	// decode aggId
	s := strings.Split(rKey, redisKeyDelimiter)
	assert.True(t, len(s) == 3)
	{
		dest := make([]byte, 100)
		n, err := base91.StdEncoding.Decode(dest, []byte(s[0]))
		assert.NoError(t, err)
		aggId, _, err := binary.ReadUvarint(dest[:n])
		assert.NoError(t, err)
		assert.Equal(t, aggId, uint64(ftypes.AggId(12)))
	}
	// decode codec
	{
		dest := make([]byte, 100)
		n, err := base91.StdEncoding.Decode(dest, []byte(s[1]))
		assert.NoError(t, err)
		codec, _, err := codex.Read(dest[:n])
		assert.NoError(t, err)
		assert.Equal(t, codec, counterCodec)
	}
}

func testStorage(t *testing.T, store BucketStore) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()

	scenarios := []struct {
		bucketLists []counter.BucketList
		z           value.Value
		v1          []value.Value
		v2          []value.Value
	}{
		{
			[]counter.BucketList{
				{Key: "k1", Window: ftypes.Window_DAY, Width: 1, StartIndex: 5, EndIndex: 5},
				{Key: "k2", Window: ftypes.Window_HOUR, Width: 4, StartIndex: 8, EndIndex: 8},
			},
			value.Int(0),
			[]value.Value{value.String("hi"), value.Int(5)},
			[]value.Value{value.Nil, value.Int(4)},
		},
		{
			[]counter.BucketList{
				{Key: "k1", Window: ftypes.Window_DAY, Width: 1, StartIndex: 5, EndIndex: 5},
				{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 7, EndIndex: 7},
				{Key: "k3", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 8, EndIndex: 8},
			},
			value.Int(0),
			[]value.Value{value.String("hi"), value.Nil, value.Int(51)},
			[]value.Value{value.Nil, value.Int(4), value.Int(1)},
		},
		{
			[]counter.BucketList{
				{Key: "k1", Window: ftypes.Window_DAY, Width: 1, StartIndex: 5, EndIndex: 5},
				{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 9, EndIndex: 9},
				{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 8, EndIndex: 8},
				{Key: "k1", Window: ftypes.Window_MINUTE, Width: 6, StartIndex: 480, EndIndex: 480},
				{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 8, EndIndex: 8},
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
		found, err := store.GetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]counter.BucketList{scene.bucketLists}, []value.Value{scene.z})
		assert.NoError(t, err)
		assert.Len(t, found[0], len(scene.bucketLists))
		for _, v := range found[0] {
			assert.Equal(t, scene.z, v)
		}
		// set values
		var vals []value.Value
		for i := range scene.bucketLists {
			vals = append(vals, scene.v1[i])
		}
		assert.NoError(t, store.SetMulti(ctx, tier, []ftypes.AggId{aggId}, toBuckets([][]counter.BucketList{scene.bucketLists}), [][]value.Value{vals}))

		// check it went through
		found, err = store.GetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]counter.BucketList{scene.bucketLists}, []value.Value{scene.z})
		assert.NoError(t, err)
		assert.Len(t, found[0], len(scene.bucketLists))
		for i, v := range found[0] {
			assert.Equal(t, vals[i], v)
		}

		// now only update odd buckets
		odd := make([]counter.BucketList, 0)
		vals2 := make([]value.Value, 0)
		for i := range scene.bucketLists {
			if i%2 == 0 {
				continue
			}
			vals2 = append(vals2, scene.v2[i])
			odd = append(odd, scene.bucketLists[i])
		}
		assert.NoError(t, store.SetMulti(ctx, tier, []ftypes.AggId{aggId}, toBuckets([][]counter.BucketList{odd}), [][]value.Value{vals2}))
		found, err = store.GetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]counter.BucketList{scene.bucketLists}, []value.Value{scene.z})
		assert.NoError(t, err)
		assert.Len(t, found[0], len(scene.bucketLists))
		for i := range scene.bucketLists {
			if i%2 == 0 {
				assert.Equal(t, scene.v1[i], found[0][i])
			} else {
				assert.Equal(t, scene.v2[i], found[0][i])
			}
		}
	}
}

func testStorageMulti(t *testing.T, store BucketStore) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()

	ids := []ftypes.AggId{
		1,
		2,
		3,
		1,
		2,
	}
	bucketLists := [][]counter.BucketList{
		{},
		{
			{Key: "k1", Window: ftypes.Window_DAY, Width: 1, StartIndex: 5, EndIndex: 9},
			{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 7, EndIndex: 8},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 8, EndIndex: 8},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 9, EndIndex: 12},
			{Key: "k3", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 406, EndIndex: 412},
		},
		{
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 8, EndIndex: 10},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 9, EndIndex: 14},
			{Key: "k3", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 406, EndIndex: 412},
		},
		{
			{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 7, EndIndex: 13},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 8, EndIndex: 15},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 9, EndIndex: 9},
			{Key: "k3", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 406, EndIndex: 413},
		},
		{
			{Key: "k1", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 0, EndIndex: 1},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 8, EndIndex: 12},
			{Key: "k2", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 9, EndIndex: 13},
			{Key: "k3", Window: ftypes.Window_HOUR, Width: 6, StartIndex: 406, EndIndex: 415},
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
	vals, err := store.GetMulti(ctx, tier, ids, bucketLists, defaults)
	assert.NoError(t, err)
	assert.Equal(t, len(bucketLists), len(vals))
	for i := range bucketLists {
		expVals := 0
		for _, bl := range bucketLists[i] {
			expVals += int(bl.Count())
		}
		assert.Equal(t, len(vals[i]), expVals)
		for j := range vals[i] {
			assert.True(t, defaults[i].Equal(vals[i][j]))
		}
	}
	expected := toVals(bucketLists)
	err = store.SetMulti(ctx, tier, ids, toBuckets(bucketLists), expected)
	assert.NoError(t, err)
	found, err := store.GetMulti(ctx, tier, ids, bucketLists, defaults)
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
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()

	ids := make([]ftypes.AggId, numAggs)
	bucketLists := make([][]counter.BucketList, numAggs)
	vals := make([][]value.Value, numAggs)
	for i := range ids {
		ids[i] = ftypes.AggId(rand.Intn(1000000))
		bucketLists[i] = make([]counter.BucketList, numBuckets)
		vals[i] = make([]value.Value, numBuckets)
		for j := range bucketLists[i] {
			bucketLists[i][j] = counter.BucketList{
				Key:        utils.RandString(30),
				Window:     ftypes.Window_HOUR,
				Width:      3,
				StartIndex: uint32(j),
				EndIndex:   uint32(j),
			}
			vals[i][j] = value.NewList(value.Int(1), value.Int(2))
		}
	}
	assert.NoError(t, store.SetMulti(ctx, tier, ids, toBuckets(bucketLists), vals))
	defaults := make([]value.Value, len(ids))
	for i := range defaults {
		defaults[i] = value.Nil
	}
	found, err := store.GetMulti(ctx, tier, ids, bucketLists, defaults)
	assert.NoError(t, err)
	for i := range ids {
		for j := range bucketLists[i] {
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
			slot{g: Group{aggId: aggId, key: k, id: 0}, window: ftypes.Window_MINUTE, width: 2, idx: 3},
			false,
		},
		{
			counter.Bucket{Key: k, Window: ftypes.Window_DAY, Width: 2, Index: 3},
			slot{},
			true,
		},
		{
			counter.Bucket{Key: k, Window: ftypes.Window_HOUR, Width: 2, Index: 30},
			slot{g: Group{aggId: aggId, key: k, id: 7}, window: ftypes.Window_HOUR, width: 2, idx: 2},
			false,
		},
		{
			counter.Bucket{Key: k, Window: ftypes.Window_HOUR, Width: 2, Index: 24 * 30},
			slot{g: Group{aggId: aggId, key: k, id: 180}, window: ftypes.Window_HOUR, width: 2, idx: 0},
			false,
		},
	}
	for _, scene := range scenarios {
		var s slot
		err := g.toSlot(aggId, &scene.b, &s)
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, scene.s, s)
		}
	}
}

func toBuckets(bucketLists [][]counter.BucketList) [][]counter.Bucket {
	ret := make([][]counter.Bucket, len(bucketLists))
	for i := range bucketLists {
		for _, bl := range bucketLists[i] {
			for k := bl.StartIndex; k <= bl.EndIndex; k++ {
				ret[i] = append(ret[i], counter.Bucket{
					Key:    bl.Key,
					Window: bl.Window,
					Width:  bl.Width,
					Index:  k,
				})
			}
		}
	}
	return ret
}

func toVals(bucketLists [][]counter.BucketList) [][]value.Value {
	ret := make([][]value.Value, len(bucketLists))
	for i := range bucketLists {
		for _, bl := range bucketLists[i] {
			for k := bl.StartIndex; k <= bl.EndIndex; k++ {
				ret[i] = append(ret[i], value.Int(k))
			}
		}
	}
	return ret
}
