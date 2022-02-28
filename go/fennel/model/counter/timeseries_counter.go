package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

type TimeseriesCounter struct {
	Window ftypes.Window
	Limit  uint64
}

func (r TimeseriesCounter) Start(end ftypes.Timestamp) ftypes.Timestamp {
	var d ftypes.Timestamp
	switch r.Window {
	case ftypes.Window_HOUR:
		d = ftypes.Timestamp(1+r.Limit) * 3600
	case ftypes.Window_DAY:
		d = ftypes.Timestamp(1+r.Limit) * 3600 * 24
	}
	if end > d {
		return end - d
	}
	return ftypes.Timestamp(0)
}

func (r TimeseriesCounter) Reduce(values []value.Value) (value.Value, error) {
	// we have to take the last Limit values only and if there are fewer than that
	// available we pad a few entries with zeros.
	limit := int(r.Limit)
	last := len(values) - 1
	ret := make([]value.Value, r.Limit)
	var i int
	for i = 0; i < limit && i < len(values); i++ {
		ret[limit-1-i] = values[last-i]
	}
	for ; i < limit; i++ {
		ret[limit-1-i] = value.Int(0)
	}
	return value.List(ret), nil
}

func (r TimeseriesCounter) Merge(a, b value.Value) (value.Value, error) {
	if _, ok := a.(value.Int); !ok {
		return nil, fmt.Errorf("expected int but got: %v", a)
	}
	return a.Op("+", b)
}

func (r TimeseriesCounter) Zero() value.Value {
	return value.Int(0)
}
func (r TimeseriesCounter) Bucketize(groupkey string, v value.Value, timestamp ftypes.Timestamp) ([]Bucket, error) {
	if _, ok := v.(value.Int); !ok {
		return nil, fmt.Errorf("expected value to be an int but got: '%s' instead", v)
	}
	return BucketizeMoment(groupkey, timestamp, v, r.Windows()), nil
}

func (r TimeseriesCounter) Windows() []ftypes.Window {
	return []ftypes.Window{r.Window}
}

var _ Histogram = TimeseriesCounter{}
