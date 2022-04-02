package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

/*
	rollingMax maintains maximum of a bucket with two vars (maxv and empty).
	Maxv is the maximum value. If empty is true, the bucket is empty so maxv is ignored.
*/
type rollingMax struct {
	Durations []uint64
	Bucketizer
	BucketStore
}

func (m rollingMax) Transform(v value.Value) (value.Value, error) {
	v_int, ok := v.(value.Int)
	if !ok {
		return nil, fmt.Errorf("expected value to be an int but got: '%s' instead", v)
	}
	return value.NewList(v_int, value.Bool(false)), nil
}

func NewMax(name ftypes.AggName, durations []uint64) Histogram {
	maxDuration := getMaxDuration(durations)
	return rollingMax{
		Durations: durations,
		Bucketizer: fixedWidthBucketizer{map[ftypes.Window]uint64{
			ftypes.Window_MINUTE: 6,
			ftypes.Window_DAY:    1,
		}, true},
		// retain all keys for 1.5days + duration
		BucketStore: NewTwoLevelStorage(24*3600, maxDuration+24*3600*1.5),
	}
}

func max(a int64, b int64) int64 {
	if a < b {
		return b
	} else {
		return a
	}
}

func (m rollingMax) Start(end ftypes.Timestamp, kwargs value.Dict) (ftypes.Timestamp, error) {
	d, err := extractDuration(kwargs, m.Durations)
	if err != nil {
		return 0, err
	}
	return start(end, d), nil
}

func (m rollingMax) extract(v value.Value) (int64, bool, error) {
	l, ok := v.(value.List)
	if !ok || l.Len() != 2 {
		return 0, false, fmt.Errorf("expected list of two elements but got: %v", v)
	}
	e, _ := l.At(1)
	empty, ok := e.(value.Bool)
	if !ok {
		return 0, false, fmt.Errorf("expected boolean but found: %v", e)
	}
	if empty {
		return 0, true, nil
	}
	e, _ = l.At(0)
	maxv, ok := e.(value.Int)
	if !ok {
		return 0, false, fmt.Errorf("expected integer but found: %v", maxv)
	}
	return int64(maxv), false, nil
}

func (m rollingMax) merge(v1 int64, e1 bool, v2 int64, e2 bool) (int64, bool) {
	if e1 {
		return v2, e2
	}
	if e2 {
		return v1, e1
	}
	return max(v1, v2), false
}

func (m rollingMax) Reduce(values []value.Value) (value.Value, error) {
	var maxv int64 = 0
	empty := true
	for _, v := range values {
		v, e, err := m.extract(v)
		if err != nil {
			return nil, err
		}
		maxv, empty = m.merge(maxv, empty, v, e)
	}
	return value.Int(maxv), nil
}

func (m rollingMax) Merge(a, b value.Value) (value.Value, error) {
	v1, e1, err := m.extract(a)
	if err != nil {
		return nil, err
	}
	v2, e2, err := m.extract(b)
	if err != nil {
		return nil, err
	}
	v, e := m.merge(v1, e1, v2, e2)
	return value.NewList(value.Int(v), value.Bool(e)), nil
}

func (m rollingMax) Zero() value.Value {
	return value.NewList(value.Int(0), value.Bool(true))
}

var _ Histogram = rollingMax{}
