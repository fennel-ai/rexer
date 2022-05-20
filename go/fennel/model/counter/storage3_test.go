package counter

import (
	"context"
	"math/rand"
	"testing"

	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/test"
	"fennel/tier"
	"github.com/stretchr/testify/assert"
)

func TestThirdStore(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	s := thirdStore{
		bucketsPerSlot: 240,
		prefixSize:     2,
		retention:      30,
	}

	// prepare some buckets
	aggIDs := make([]ftypes.AggId, 11)
	buckets := make([][]counter.Bucket, 11)
	defaults := make([]value.Value, 11)
	for i := range buckets {
		aggIDs[i] = ftypes.AggId(i)
		defaults[i] = value.Int(0)
		buckets[i] = make([]counter.Bucket, 15)
		for j := range buckets[i] {
			buckets[i][j] = counter.Bucket{
				Key:    "key",
				Window: ftypes.Window_FOREVER,
				Width:  120,
				Index:  uint64(j),
				Value:  value.Int(i * j),
			}
		}
	}

	// setting nothing should not fail
	thirdStoreSet(t, ctx, &tier, s, nil, nil)

	// getting nothing should not fail
	found := thirdStoreGet(t, ctx, &tier, s, nil, nil, nil)
	assert.Empty(t, found)

	// should get nothing initially
	found = thirdStoreGet(t, ctx, &tier, s, aggIDs, buckets, defaults)
	for i := range found {
		for j := range found[i] {
			assert.True(t, defaults[i].Equal(found[i][j]))
		}
	}

	thirdStoreSet(t, ctx, &tier, s, aggIDs, buckets)

	found = thirdStoreGet(t, ctx, &tier, s, aggIDs, buckets, defaults)
	verifyFoundBucketValues(t, buckets, found)

	// now set some other buckets
	for i := range buckets {
		buckets[i] = append(buckets[i], counter.Bucket{
			Key:    "key",
			Window: ftypes.Window_FOREVER,
			Width:  120,
			Index:  17,
			Value:  value.Int(i * 17),
		})
		buckets[i] = append(buckets[i], counter.Bucket{
			Key:    "key",
			Window: ftypes.Window_FOREVER,
			Width:  120,
			Index:  23,
			Value:  value.Int(i * 23),
		})
	}
	thirdStoreSet(t, ctx, &tier, s, aggIDs, buckets)
	assert.NoError(t, err)

	// should get them
	found = thirdStoreGet(t, ctx, &tier, s, aggIDs, buckets, defaults)
	verifyFoundBucketValues(t, buckets, found)
}

func thirdStoreSet(t *testing.T, ctx context.Context, tier *tier.Tier, s thirdStore, aggIDs []ftypes.AggId, buckets [][]counter.Bucket) {
	if rand.Intn(2) == 0 {
		err := s.SetMulti(ctx, *tier, aggIDs, buckets)
		assert.NoError(t, err)
	} else {
		for i := range aggIDs {
			err := s.Set(ctx, *tier, aggIDs[i], buckets[i])
			assert.NoError(t, err)
		}
	}
}

func thirdStoreGet(t *testing.T, ctx context.Context, tier *tier.Tier, s thirdStore, aggIDs []ftypes.AggId, buckets [][]counter.Bucket, defaults []value.Value) [][]value.Value {
	if rand.Intn(2) == 0 {
		res, err := s.GetMulti(ctx, *tier, aggIDs, buckets, defaults)
		assert.NoError(t, err)
		return res
	} else {
		var res [][]value.Value
		for i := range aggIDs {
			vals, err := s.Get(ctx, *tier, aggIDs[i], buckets[i], defaults[i])
			assert.NoError(t, err)
			res = append(res, vals)
		}
		return res
	}
}
