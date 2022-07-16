package counter

import (
	"context"
	"fmt"

	"fennel/lib/aggregate"
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/lib/value"
	"fennel/tier"

	"github.com/samber/mo"
)

type Bucketizer interface {
	BucketizeMoment(key string, ts ftypes.Timestamp) []counter.Bucket
	BucketizeDuration(key string, start, end ftypes.Timestamp) []counter.Bucket
}

type BucketStore interface {
	GetBucketStore() BucketStore
	GetMulti(ctx context.Context, tr tier.Tier, aggIds []ftypes.AggId, buckets [][]counter.Bucket, defaults_ []value.Value) ([][]value.Value, error)
	SetMulti(ctx context.Context, tr tier.Tier, aggIds []ftypes.AggId, buckets [][]counter.Bucket, values [][]value.Value) error
}

type Histogram struct {
	counter.MergeReduce
	BucketStore
	Bucketizer
}

func Start(mr counter.MergeReduce, end ftypes.Timestamp, duration mo.Option[uint32]) (ftypes.Timestamp, error) {
	switch mr.Options().AggType {
	case aggregate.TIMESERIES_SUM:
		var d uint32
		opts := mr.Options()
		switch opts.Window {
		case ftypes.Window_HOUR:
			d = uint32(1+opts.Limit) * 3600
		case ftypes.Window_DAY:
			d = uint32(1+opts.Limit) * 3600 * 24
		}
		return start(end, d), nil
	default:
		if duration.IsAbsent() {
			return 0, fmt.Errorf("duration is required for aggregate to ftype %v", mr.Options().AggType)
		}
		return start(end, duration.MustGet()), nil
	}
}

// Returns the default histogram that uses two-level store and 6-minutely
// buckets.
// TODO: implement aggregations that can support forever aggregations.
// https://linear.app/fennel-ai/issue/REX-1053/support-forever-aggregates
func ToHistogram(aggId ftypes.AggId, opts aggregate.Options) (Histogram, error) {
	var retention uint32
	mr, err := counter.ToMergeReduce(aggId, opts)
	if err != nil {
		return Histogram{}, err
	}
	bucketizer := sixMinutelyBucketizer
	if mr.Options().AggType == aggregate.TIMESERIES_SUM {
		d, err := utils.Duration(opts.Window)
		if err != nil {
			d = 0
		}
		if d > 0 {
			retention = opts.Limit * d
		}
		bucketizer = fixedWidthBucketizer{
			map[ftypes.Window]uint32{
				opts.Window: 1,
			},
			false, /* include trailing */
		}
	} else {
		retention = getMaxDuration(opts.Durations)
	}
	return Histogram{
		mr,
		// retain all keys for 1.1days(95040) + retention
		NewTwoLevelStorage(24*3600, retention+95040),
		bucketizer,
	}, nil
}
