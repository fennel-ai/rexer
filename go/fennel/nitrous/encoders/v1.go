package encoders

import (
	"fmt"

	"fennel/lib/aggregate"
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	rpc "fennel/nitrous/rpc/v2"
	"fennel/nitrous/server/store"
	"fennel/nitrous/server/temporal"
	"fennel/plane"

	"github.com/raulk/clock"
)

// The v1 logical encoding has three salient components:
// 1. It uses the FixedWidthBucketizer with 100 buckets for bucketizing time.
// 2. Uses the counter.ToMergeReduce function to determine how intermediate or
// partial counter values are represented and merged.
// 3. Uses Closet store to store the aggregate values.
// If any of these need to be changed, we need to create a different encoding.
type V1Encoder struct{}

func (e V1Encoder) NewStore(plane plane.Plane, tierId ftypes.RealmID, aggId ftypes.AggId, options aggregate.Options) (TailingStore, error) {
	const numBuckets = 100
	bucketizer := temporal.NewFixedWidthBucketizer(numBuckets, clock.New())
	mr, err := counter.ToMergeReduce(aggId, options)
	if err != nil {
		return nil, fmt.Errorf("failed to create merge reduce for aggId %d in tier %d: %w", aggId, tierId, err)
	}
	ags, err := store.NewCloset(plane, tierId, aggId, rpc.AggCodec_V1, mr, bucketizer)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize aggregate store for new aggregate (%d) in tier (%d): %w", aggId, tierId, err)
	}
	return ags, nil
}
