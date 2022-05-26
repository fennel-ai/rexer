package counter

import (
	"context"
	"fmt"

	"fennel/lib/aggregate"
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/tier"
)

type Bucketizer interface {
	BucketizeMoment(key string, ts ftypes.Timestamp, v value.Value) []counter.Bucket
	BucketizeDuration(key string, start, end ftypes.Timestamp, v value.Value) []counter.Bucket
}

type BucketStore interface {
	GetBucketStore() BucketStore
	Get(ctx context.Context, tr tier.Tier, aggId ftypes.AggId, buckets []counter.Bucket, default_ value.Value) ([]value.Value, error)
	GetMulti(ctx context.Context, tr tier.Tier, aggIds []ftypes.AggId, buckets [][]counter.Bucket, defaults_ []value.Value) ([][]value.Value, error)
	Set(ctx context.Context, tr tier.Tier, aggId ftypes.AggId, deltas []counter.Bucket) error
	SetMulti(ctx context.Context, tr tier.Tier, aggIds []ftypes.AggId, deltas [][]counter.Bucket) error
}

type MergeReduce interface {
	Transform(v value.Value) (value.Value, error)
	Merge(a, b value.Value) (value.Value, error)
	Reduce(values []value.Value) (value.Value, error)
	Zero() value.Value
}

type Histogram interface {
	Start(end ftypes.Timestamp, kwargs value.Dict) (ftypes.Timestamp, error)
	Bucketizer
	MergeReduce
	BucketStore
}

func ToHistogram(tr tier.Tier, aggId ftypes.AggId, opts aggregate.Options) (Histogram, error) {
	switch opts.AggType {
	case "sum":
		return NewSum(opts.Durations), nil
	case "timeseries_sum":
		return NewTimeseriesSum(opts.Window, opts.Limit), nil
	case "average":
		return NewAverage(opts.Durations), nil
	case "list":
		return NewList(opts.Durations), nil
	case "min":
		return NewMin(opts.Durations), nil
	case "max":
		return NewMax(opts.Durations), nil
	case "stddev":
		return NewStdDev(opts.Durations), nil
	case "rate":
		return NewRate(tr, aggId, opts.Durations, opts.Normalize), nil
	case "topk":
		return NewTopK(opts.Durations), nil
	default:
		return nil, fmt.Errorf("invalid aggregate type: %v", opts.AggType)
	}
}
