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

type MergeReduce interface {
	Options() aggregate.Options
	Transform(v value.Value) (value.Value, error)
	Merge(a, b value.Value) (value.Value, error)
	Reduce(values []value.Value) (value.Value, error)
	Zero() value.Value
}

type Histogram struct {
	MergeReduce
	BucketStore
	Bucketizer
}

// This is only useful in bucketizing, so once bucketizer becomes mr-aware,
// we can move this out of histogram.
func Start(h Histogram, end ftypes.Timestamp, kwargs value.Dict) (ftypes.Timestamp, error) {
	switch t := h.MergeReduce.(type) {
	case timeseriesSum:
		return t.Start(end)
	default:
		d, err := extractDuration(kwargs, h.Options().Durations)
		if err != nil {
			return 0, err
		}
		return start(end, d), nil
	}
}

func ToMergeReduce(aggId ftypes.AggId, opts aggregate.Options) (MergeReduce, error) {
	var mr MergeReduce
	switch opts.AggType {
	case "timeseries_sum":
		mr = NewTimeseriesSum(opts)
	case "sum":
		mr = NewSum(opts)
	case "average":
		mr = NewAverage(opts)
	case "list":
		mr = NewList(opts)
	case "min":
		mr = NewMin(opts)
	case "max":
		mr = NewMax(opts)
	case "stddev":
		mr = NewStdDev(opts)
	case "rate":
		mr = NewRate(aggId, opts)
	case "topk":
		mr = NewTopK(opts)
	default:
		return nil, fmt.Errorf("invalid aggregate type: %v", opts.AggType)
	}
	return mr, nil
}

// Returns the default histogram that uses two-level store and 6-minutely
// buckets.
// TODO: implement aggregations that can support forever aggregations.
// https://linear.app/fennel-ai/issue/REX-1053/support-forever-aggregates
func ToHistogram(aggId ftypes.AggId, opts aggregate.Options) (Histogram, error) {
	var retention uint32
	mr, err := ToMergeReduce(aggId, opts)
	if err != nil {
		return Histogram{}, err
	}
	bucketizer := sixMinutelyBucketizer
	switch mr.(type) {
	case timeseriesSum:
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
	default:
		retention = getMaxDuration(opts.Durations)
	}

	return Histogram{
		mr,
		// retain all keys for 1.1days(95040) + retention
		NewTwoLevelStorage(24*3600, retention+95040),
		bucketizer,
	}, nil
}
