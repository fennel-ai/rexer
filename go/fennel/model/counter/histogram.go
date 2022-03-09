package counter

import (
	"context"

	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/tier"
)

type Bucket struct {
	Key    string
	Window ftypes.Window
	Index  uint64
	Count  value.Value
}

type Bucketizer interface {
	BucketizeMoment(key string, ts ftypes.Timestamp, v value.Value) []Bucket
	BucketizeDuration(key string, start, end ftypes.Timestamp, v value.Value) []Bucket
}

type BucketStore interface {
	Get(ctx context.Context, tier tier.Tier, buckets []Bucket, default_ value.Value) ([]value.Value, error)
	Set(ctx context.Context, tier tier.Tier, buckets []Bucket) error
}

type MergeReduce interface {
	Transform(v value.Value) (value.Value, error)
	Merge(a, b value.Value) (value.Value, error)
	Reduce(values []value.Value) (value.Value, error)
	Zero() value.Value
}

type Histogram interface {
	Start(end ftypes.Timestamp) ftypes.Timestamp
	Reduce(values []value.Value) (value.Value, error)
	Merge(a, b value.Value) (value.Value, error)
	Zero() value.Value
	Bucketize(groupkey string, v value.Value, timestamp ftypes.Timestamp) ([]Bucket, error)
	Windows() []ftypes.Window
}
