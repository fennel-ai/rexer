package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/lib/value"
)

type timeseriesSum struct {
	Window ftypes.Window
	Limit  uint64
	Bucketizer
	BucketStore
}

func NewTimeseriesSum(name ftypes.AggName, window ftypes.Window, limit uint64) Histogram {
	d, err := utils.Duration(window)
	if err != nil {
		d = 0
	}
	retention := uint64(0)
	if d > 0 {
		// retain all keys for 1.5days + duration
		retention = limit*d + 24*3600*1.5
	}
	return timeseriesSum{
		Window: window,
		Limit:  limit,
		Bucketizer: fixedWidthBucketizer{map[ftypes.Window]uint64{
			window: 1,
		}, false},
		// retain all keys for 1.5days + duration
		BucketStore: NewTwoLevelStorage(24*3600, retention),
	}
}

func (r timeseriesSum) Start(end ftypes.Timestamp, _ value.Dict) (ftypes.Timestamp, error) {
	var d uint64
	switch r.Window {
	case ftypes.Window_HOUR:
		d = (1 + r.Limit) * 3600
	case ftypes.Window_DAY:
		d = (1 + r.Limit) * 3600 * 24
	}
	return start(end, d), nil
}

func (r timeseriesSum) Reduce(values []value.Value) (value.Value, error) {
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
	return value.NewList(ret...), nil
}

func (r timeseriesSum) Merge(a, b value.Value) (value.Value, error) {
	if _, ok := a.(value.Int); !ok {
		return nil, fmt.Errorf("expected int but got: %v", a)
	}
	return a.Op("+", b)
}

func (r timeseriesSum) Zero() value.Value {
	return value.Int(0)
}

func (r timeseriesSum) Transform(v value.Value) (value.Value, error) {
	if _, ok := v.(value.Int); !ok {
		return nil, fmt.Errorf("expected value to be an int but got: '%s' instead", v)
	}
	return v, nil
}

var _ Histogram = timeseriesSum{}
