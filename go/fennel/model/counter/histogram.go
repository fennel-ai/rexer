package counter

import (
	"fennel/lib/ftypes"
	"fennel/lib/value"
)

type Histogram interface {
	Start(end ftypes.Timestamp) ftypes.Timestamp
	Reduce(values []int64) (value.Value, error)
	Merge(a, b int64) int64
	Empty() int64
	Bucketize(actions value.Table) ([]Bucket, error)
	Windows() []ftypes.Window
	Marshal(v int64) (string, error)
	Unmarshal(s string) (int64, error)
}
