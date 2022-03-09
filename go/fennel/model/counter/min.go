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
		Bucketizer: FixedWidthBucketizer{windows: []ftypes.Window{
			ftypes.Window_MINUTE, ftypes.Window_DAY, ftypes.Window_HOUR,
		}},
		BucketStore: FlatRedisStorage{name: name},
	}
}

func min(a int64, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func (m rollingMin) Start(end ftypes.Timestamp) ftypes.Timestamp {
	return start(end, m.Duration)
}

func (m rollingMin) extract(v value.Value) (int64, bool, error) {
	l, ok := v.(value.List)
	if !ok || len(l) != 2 {
		return 0, false, fmt.Errorf("expected list of two elements but got: %v", v)
	}
	empty, ok := l[1].(value.Bool)
	if !ok {
		return 0, false, fmt.Errorf("expected boolean but found: %v", l[1])
	}
	if empty {
		return 0, true, nil
	}
	minv, ok := l[0].(value.Int)
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
	return value.List{value.Int(v), value.Bool(e)}, nil
}

func (m rollingMin) Zero() value.Value {
	return value.List{value.Int(0), value.Bool(true)}
}

func (m rollingMin) Transform(v value.Value) (value.Value, error) {
	v_int, ok := v.(value.Int)
	if !ok {
		return nil, fmt.Errorf("expected value to be an int but got: '%s' instead", v)
	}
	return value.List{v_int, value.Bool(false)}, nil
}

var _ Histogram = rollingMin{}
