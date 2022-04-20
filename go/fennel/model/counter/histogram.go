package counter

import (
	"context"

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
