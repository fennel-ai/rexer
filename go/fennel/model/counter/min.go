package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

/*
	rollingMin maintains minimum of a bucket with two vars (minv and empty).
	Minv is the minimum value. If empty is true, the bucket is empty so minv is ignored.
*/
type rollingMin struct {
	Duration uint64
	Bucketizer
	BucketStore
}

func NewMin(name ftypes.AggName, duration uint64) Histogram {
	return rollingMin{
		Duration: duration,
		Bucketizer: fixedWidthBucketizer{map[ftypes.Window]uint64{
			ftypes.Window_MINUTE: 6,
			ftypes.Window_DAY:    1,
		}, true},
		// retain all keys for 1.5days + duration
		BucketStore: NewTwoLevelStorage(name, 24*3600, duration+24*3600*1.5),
	}
}

func min(a int64, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func (m rollingMin) Start(end ftypes.Timestamp, kwargs value.Dict) (ftypes.Timestamp, error) {
	d, err := extractDuration(kwargs, m.Duration)
	if err != nil {
		return 0, err
	}
	return start(end, d), nil
}

func (m rollingMin) extract(v value.Value) (int64, bool, error) {
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
	minv, ok := e.(value.Int)
	if !ok {
		return 0, false, fmt.Errorf("expected integer but found: %v", minv)
	}
	return int64(minv), false, nil
}

func (m rollingMin) merge(v1 int64, e1 bool, v2 int64, e2 bool) (int64, bool) {
	if e1 {
		return v2, e2
	}
	if e2 {
		return v1, e1
	}
	return min(v1, v2), false
}

func (m rollingMin) Reduce(values []value.Value) (value.Value, error) {
	var minv int64 = 0
	empty := true
	for _, v := range values {
		v, e, err := m.extract(v)
		if err != nil {
			return nil, err
		}
		minv, empty = m.merge(minv, empty, v, e)
	}
	return value.Int(minv), nil
}

func (m rollingMin) Merge(a, b value.Value) (value.Value, error) {
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

func (m rollingMin) Zero() value.Value {
	return value.NewList(value.Int(0), value.Bool(true))
}

func (m rollingMin) Transform(v value.Value) (value.Value, error) {
	v_int, ok := v.(value.Int)
	if !ok {
		return nil, fmt.Errorf("expected value to be an int but got: '%s' instead", v)
	}
	return value.NewList(v_int, value.Bool(false)), nil
}

var _ Histogram = rollingMin{}
