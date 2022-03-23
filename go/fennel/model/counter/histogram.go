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
	Get(context.Context, tier.Tier, ftypes.AggName, []Bucket, value.Value) ([]value.Value, error)
	GetMulti(context.Context, tier.Tier, []ftypes.AggName, [][]Bucket, []value.Value) ([][]value.Value, error)
	Set(context.Context, tier.Tier, ftypes.AggName, []Bucket) error
	SetMulti(context.Context, tier.Tier, []ftypes.AggName, [][]Bucket) error
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
