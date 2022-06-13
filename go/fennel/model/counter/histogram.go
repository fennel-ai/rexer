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
	SetMulti(ctx context.Context, tr tier.Tier, aggIds []ftypes.AggId, deltas [][]counter.Bucket, values [][]value.Value) error
}

type MergeReduce interface {
	Transform(v value.Value) (value.Value, error)
	Merge(a, b value.Value) (value.Value, error)
	Reduce(values []value.Value) (value.Value, error)
	Zero() value.Value
}

type Histogram struct {
	aggregate.Options
	MergeReduce
	BucketStore
	Bucketizer
}

func (h Histogram) Start(end ftypes.Timestamp, kwargs value.Dict) (ftypes.Timestamp, error) {
	switch mr := h.MergeReduce.(type) {
	case timeseriesSum:
		return mr.Start(end, kwargs)
	default:
		d, err := extractDuration(kwargs, h.Durations)
		if err != nil {
			return 0, err
		}
		return start(end, d), nil
	}
}

// Returns the default histogram that uses two-level store and 6-minutely
// buckets.
// TODO: implement aggregations that can support forever aggregations.
// https://linear.app/fennel-ai/issue/REX-1053/support-forever-aggregates
func ToHistogram(tr tier.Tier, aggId ftypes.AggId, opts aggregate.Options) (Histogram, error) {
	var retention uint32
	var mr MergeReduce
	bucketizer := sixMinutelyBucketizer
	switch opts.AggType {
	case "timeseries_sum":
		mr = NewTimeseriesSum(opts.Window, opts.Limit)
		d, err := utils.Duration(opts.Window)
		if err != nil {
			d = 0
		}
		if d > 0 {
			retention = opts.Limit * uint32(d)
		}
		bucketizer = fixedWidthBucketizer{
			map[ftypes.Window]uint32{
				opts.Window: 1,
			},
			false, /* include trailing */
		}
	case "sum":
		mr = NewSum()
		retention = getMaxDuration(opts.Durations)
	case "average":
		mr = NewAverage()
		retention = getMaxDuration(opts.Durations)
	case "list":
		mr = NewList()
		retention = getMaxDuration(opts.Durations)
	case "min":
		mr = NewMin()
		retention = getMaxDuration(opts.Durations)
	case "max":
		mr = NewMax()
		retention = getMaxDuration(opts.Durations)
	case "stddev":
		mr = NewStdDev()
		retention = getMaxDuration(opts.Durations)
	case "rate":
		mr = NewRate(tr, aggId, opts.Normalize)
		retention = getMaxDuration(opts.Durations)
	case "topk":
		mr = NewTopK()
		retention = getMaxDuration(opts.Durations)
	default:
		return Histogram{}, fmt.Errorf("invalid aggregate type: %v", opts.AggType)
	}

	return Histogram{
		opts,
		mr,
		// retain all keys for 1.1days(95040) + retention
		NewTwoLevelStorage(24*3600, retention+95040),
		bucketizer,
	}, nil
}
