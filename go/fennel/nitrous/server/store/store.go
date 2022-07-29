package store

import (
	"context"
	"fmt"

	"fennel/hangar"
	"fennel/lib/aggregate"
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/nitrous/rpc"
	"fennel/nitrous/server/tailer"
	"fennel/nitrous/server/temporal"

	"github.com/raulk/clock"
	"github.com/samber/mo"
)

type Table interface {
	tailer.EventProcessor
	Get(ctx context.Context, keys []string, kwargs []value.Dict, store hangar.Hangar) ([]value.Value, error)
	Options() aggregate.Options
}

func Make(tierId ftypes.RealmID, aggId ftypes.AggId, codec rpc.AggCodec, options aggregate.Options,
	clock clock.Clock) (Table, error) {
	switch codec {
	case rpc.AggCodec_V1:
		// The v1 logical encoding has three salient components:
		// 1. It uses the FixedWidthBucketizer with 100 buckets for bucketizing time.
		// 2. Uses the counter.ToMergeReduce function to determine how intermediate or
		// partial counter values are represented and merged.
		// 3. Uses the Closet store to store the aggregate values in a 2-level hierarchy
		//    with the the second level storing 25 buckets as fields under a hangar key.
		// If any of these need to be changed, we need to create a different encoding.
		const numBuckets = 100
		const secondLevelSize = 25
		bucketizer := temporal.NewFixedWidthBucketizer(numBuckets, clock)
		mr, err := counter.ToMergeReduce(aggId, options)
		if err != nil {
			return nil, fmt.Errorf("failed to create merge reduce for aggId %d in tier %d: %w", aggId, tierId, err)
		}
		table, err := NewCloset(tierId, aggId, rpc.AggCodec_V1, mr, bucketizer, secondLevelSize)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize aggregate store for new aggregate (%d) in tier (%d): %w", aggId, tierId, err)
		}
		return table, nil
	default:
		return nil, fmt.Errorf("unsupported codec %d", codec)
	}
}

func getRequestDuration(options aggregate.Options, kwargs value.Dict) (mo.Option[uint32], error) {
	if options.AggType == aggregate.TIMESERIES_SUM {
		return mo.None[uint32](), nil
	}
	d, err := extractDuration(kwargs, options.Durations)
	if err != nil {
		return mo.None[uint32](), err
	}
	return mo.Some(d), nil
}

func extractDuration(kwargs value.Dict, durations []uint32) (uint32, error) {
	v, ok := kwargs.Get("duration")
	if !ok {
		return 0, fmt.Errorf("error: no duration specified")
	}
	duration, ok := v.(value.Int)
	if !ok {
		return 0, fmt.Errorf("error: expected kwarg 'duration' to be an int but found: '%v'", v)
	}
	// check duration is positive so it can be typecast to uint32 safely
	if duration < 0 {
		return 0, fmt.Errorf("error: specified duration (%d) < 0", duration)
	}
	for _, d := range durations {
		if uint32(duration) == d {
			return d, nil
		}
	}
	return 0, fmt.Errorf("error: specified duration not found in aggregate")
}
