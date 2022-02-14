package counter

import (
	"fennel/lib/ftypes"
	"fennel/lib/value"
)

type Histogram interface {
	Start(end ftypes.Timestamp) ftypes.Timestamp
	Reduce(values []value.Value) (value.Value, error)
	Merge(a, b value.Value) (value.Value, error)
	Zero() value.Value
	Bucketize(actions value.Table) ([]Bucket, error)
	Windows() []ftypes.Window
}
