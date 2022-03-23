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
	Width  uint64
	Index  uint64
	Value  value.Value
}

type Bucketizer interface {
	BucketizeMoment(key string, ts ftypes.Timestamp, v value.Value) []Bucket
	BucketizeDuration(key string, start, end ftypes.Timestamp, v value.Value) []Bucket
}

type BucketStore interface {
	GetBucketStore() BucketStore
	Get(ctx context.Context, tier tier.Tier, h Histogram, buckets []Bucket) ([]value.Value, error)
	GetMulti(ctx context.Context, tier tier.Tier, buckets map[Histogram][]Bucket) (map[Histogram][]value.Value, error)
	Set(ctx context.Context, tier tier.Tier, h Histogram, buckets []Bucket) error
	SetMulti(ctx context.Context, tier tier.Tier, buckets map[Histogram][]Bucket) error
}

type MergeReduce interface {
	Transform(v value.Value) (value.Value, error)
	Merge(a, b value.Value) (value.Value, error)
	Reduce(values []value.Value) (value.Value, error)
	Zero() value.Value
}

type Histogram interface {
	Name() ftypes.AggName
	Start(end ftypes.Timestamp, kwargs value.Dict) (ftypes.Timestamp, error)
	Bucketizer
	MergeReduce
	BucketStore
}
