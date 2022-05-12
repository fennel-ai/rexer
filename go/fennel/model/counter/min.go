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
	Durations []uint64
	Bucketizer
	BucketStore
}

func NewMin(durations []uint64) Histogram {
	maxDuration := getMaxDuration(durations)
	return rollingMin{
		Durations: durations,
		Bucketizer: fixedWidthBucketizer{map[ftypes.Window]uint64{
			ftypes.Window_MINUTE: 6,
			ftypes.Window_DAY:    1,
		}, true},
		// retain all keys for 1.5days + duration
		BucketStore: NewTwoLevelStorage(24*3600, maxDuration+24*3600*1.5),
	}
}

func min(a value.Value, b value.Value) (value.Value, error) {
	lt, err := a.Op("<", b)
	if err != nil {
		return value.Double(0), err
	}
	if lt.(value.Bool) {
		return a, nil
	}
	return b, nil
}

func (m rollingMin) Start(end ftypes.Timestamp, kwargs value.Dict) (ftypes.Timestamp, error) {
	d, err := extractDuration(kwargs, m.Durations)
	if err != nil {
		return 0, err
	}
	return start(end, d), nil
}

func (m rollingMin) extract(v value.Value) (value.Value, bool, error) {
	l, ok := v.(value.List)
	if !ok || l.Len() != 2 {
		return value.Double(0), false, fmt.Errorf("expected list of two elements but got: %v", v)
	}
	e, _ := l.At(1)
	empty, ok := e.(value.Bool)
	if !ok {
		return value.Double(0), false, fmt.Errorf("expected boolean but found: %v", e)
	}
	if empty {
		return value.Double(0), true, nil
	}
	e, _ = l.At(0)
	return e, false, nil
}

func (m rollingMin) merge(v1 value.Value, e1 bool, v2 value.Value, e2 bool) (value.Value, bool, error) {
	if e1 {
		return v2, e2, nil
	}
	if e2 {
		return v1, e1, nil
	}
	minVal, err := min(v1, v2)
	return minVal, false, err
}

func (m rollingMin) Reduce(values []value.Value) (value.Value, error) {
	var minv value.Value
	empty := true
	for _, v := range values {
		v, e, err := m.extract(v)
		if err != nil {
			return nil, err
		}
		minv, empty, err = m.merge(minv, empty, v, e)
		if err != nil {
			return value.Double(0), nil
		}
	}
	if minv == nil {
		return value.Double(0), nil
	}
	return minv, nil
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
	v, e, err := m.merge(v1, e1, v2, e2)
	return value.NewList(v, value.Bool(e)), err
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
