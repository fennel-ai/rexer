//go:build exclude

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

func TestSplitStore(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()

	s := splitStore{
		bucketsPerGroup: 10,
		retention:       30,
	}

	// prepare some buckets
	aggIDs := make([]ftypes.AggId, 11)
	buckets := make([][]counter.Bucket, 11)
	vals := make([][]value.Value, 11)
	defaults := make([]value.Value, 11)
	for i := range buckets {
		aggIDs[i] = ftypes.AggId(i)
		defaults[i] = value.Int(0)
		buckets[i] = make([]counter.Bucket, 15)
		vals[i] = make([]value.Value, 15)
		for j := range buckets[i] {
			buckets[i][j] = counter.Bucket{
				Key:    "key",
				Window: ftypes.Window_FOREVER,
				Width:  120,
				Index:  uint32(j),
			}
			vals[i][j] = value.Int(i * j)
		}
	}

	// setting nothing should not fail
	splitStoreSet(t, ctx, &tier, s, nil, nil, nil)

	// getting nothing should not fail
	found := splitStoreGet(t, ctx, &tier, s, nil, nil, nil)
	assert.Empty(t, found)

	// should get nothing initially
	found = splitStoreGet(t, ctx, &tier, s, aggIDs, buckets, defaults)
	for i := range found {
		for j := range found[i] {
			assert.True(t, defaults[i].Equal(found[i][j]))
		}
	}

	splitStoreSet(t, ctx, &tier, s, aggIDs, buckets, vals)

	found = splitStoreGet(t, ctx, &tier, s, aggIDs, buckets, defaults)
	verifyFoundBucketValues(t, vals, found)

	// now set some other buckets
	for i := range buckets {
		buckets[i] = append(buckets[i], counter.Bucket{
			Key:    "key",
			Window: ftypes.Window_FOREVER,
			Width:  120,
			Index:  17,
		})
		vals[i] = append(vals[i], value.Int(i*17))
		buckets[i] = append(buckets[i], counter.Bucket{
			Key:    "key",
			Window: ftypes.Window_FOREVER,
			Width:  120,
			Index:  23,
		})
		vals[i] = append(vals[i], value.Int(i*23))
	}
	splitStoreSet(t, ctx, &tier, s, aggIDs, buckets, vals)

	// should get them
	found = splitStoreGet(t, ctx, &tier, s, aggIDs, buckets, defaults)
	verifyFoundBucketValues(t, vals, found)
}

func splitStoreSet(t *testing.T, ctx context.Context, tier *tier.Tier, s splitStore, aggIDs []ftypes.AggId, buckets [][]counter.Bucket, vals [][]value.Value) {
	if rand.Intn(2) == 0 {
		err := s.SetMulti(ctx, *tier, aggIDs, buckets, vals)
		assert.NoError(t, err)
	} else {
		for i := range aggIDs {
			err := s.SetMulti(ctx, *tier, []ftypes.AggId{aggIDs[i]}, [][]counter.Bucket{buckets[i]}, [][]value.Value{vals[i]})
			assert.NoError(t, err)
		}
	}
}

func splitStoreGet(t *testing.T, ctx context.Context, tier *tier.Tier, s splitStore, aggIDs []ftypes.AggId, buckets [][]counter.Bucket, defaults []value.Value) [][]value.Value {
	if rand.Intn(2) == 0 {
		res, err := s.GetMulti(ctx, *tier, aggIDs, buckets, defaults)
		assert.NoError(t, err)
		return res
	} else {
		var res [][]value.Value
		for i := range aggIDs {
			vals, err := s.GetMulti(ctx, *tier, []ftypes.AggId{aggIDs[i]}, [][]counter.Bucket{buckets[i]}, []value.Value{defaults[i]})
			assert.NoError(t, err)
			res = append(res, vals[0])
		}
		return res
	}
}

func verifyFoundBucketValues(t *testing.T, actual [][]value.Value, expected [][]value.Value) {
	for i := range actual {
		for j, v := range actual[i] {
			assert.True(t, expected[i][j].Equal(v))
		}
	}
}
