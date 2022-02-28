package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

/*
	Max maintains maximum of a bucket with two vars (maxv and empty).
	Maxv is the maximum value. If empty is true, the bucket is empty so maxv is ignored.
*/
type Max struct {
	Duration uint64
}

func (m Max) Bucketize(groupkey string, v value.Value, timestamp ftypes.Timestamp) ([]Bucket, error) {
	v_int, ok := v.(value.Int)
	if !ok {
		return nil, fmt.Errorf("expected value to be an int but got: '%s' instead", v)
	}
	c := value.List{v_int, value.Bool(false)}
	return BucketizeMoment(groupkey, timestamp, c, m.Windows()), nil
}

func max(a int64, b int64) int64 {
	if a < b {
		return b
	} else {
		return a
	}
}

func (m Max) Start(end ftypes.Timestamp) ftypes.Timestamp {
	return start(end, m.Duration)
}

func (m Max) extract(v value.Value) (int64, bool, error) {
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
	maxv, ok := l[0].(value.Int)
	if !ok {
		return 0, false, fmt.Errorf("expected integer but found: %v", maxv)
	}
	return int64(maxv), false, nil
}

func (m Max) merge(v1 int64, e1 bool, v2 int64, e2 bool) (int64, bool) {
	if e1 {
		return v2, e2
	}
	if e2 {
		return v1, e1
	}
	return max(v1, v2), false
}

func (m Max) Reduce(values []value.Value) (value.Value, error) {
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

func (m Max) Merge(a, b value.Value) (value.Value, error) {
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

func (m Max) Zero() value.Value {
	return value.List{value.Int(0), value.Bool(true)}
}

func (m Max) Windows() []ftypes.Window {
	return []ftypes.Window{
		ftypes.Window_MINUTE, ftypes.Window_HOUR, ftypes.Window_DAY,
	}
}

var _ Histogram = Max{}
