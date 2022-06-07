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

var zeroMax value.Value = value.NewList(value.Double(0), value.Bool(true))

func (m rollingMax) Transform(v value.Value) (value.Value, error) {
	if err := value.Types.Number.Validate(v); err != nil {
		return nil, fmt.Errorf("value [%s] is not a number", v.String())
	}

	return value.NewList(v, value.Bool(false)), nil
}

func NewMax(durations []uint64) Histogram {
	maxDuration := getMaxDuration(durations)
	return rollingMax{
		Durations: durations,
		Bucketizer: fixedWidthBucketizer{map[ftypes.Window]uint64{
			ftypes.Window_MINUTE: 6,
			ftypes.Window_DAY:    1,
		}, true},
		// retain all keys for 1.1days (95040) + duration
		BucketStore: NewTwoLevelStorage(24*3600, maxDuration+95040),
	}
}

func (m rollingMax) Start(end ftypes.Timestamp, kwargs value.Dict) (ftypes.Timestamp, error) {
	d, err := extractDuration(kwargs, m.Durations)
	if err != nil {
		return 0, err
	}
	return start(end, d), nil
}

func (m rollingMax) extract(v value.Value) (value.Value, bool, error) {
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

func max(a value.Value, b value.Value) (value.Value, error) {
	lt, err := a.Op("<", b)
	if err != nil {
		return value.Double(0), err
	}
	if lt.(value.Bool) {
		return b, nil
	}
	return a, nil
}

func (m rollingMax) merge(v1 value.Value, e1 bool, v2 value.Value, e2 bool) (value.Value, bool, error) {
	if e1 {
		return v2, e2, nil
	}
	if e2 {
		return v1, e1, nil
	}
	maxVal, err := max(v1, v2)
	return maxVal, false, err
}

func (m rollingMax) Reduce(values []value.Value) (value.Value, error) {
	var maxv value.Value
	empty := true
	for _, v := range values {
		v, e, err := m.extract(v)
		if err != nil {
			return nil, err
		}
		maxv, empty, err = m.merge(maxv, empty, v, e)

		if err != nil {
			return value.Double(0), nil
		}
	}
	if maxv == nil {
		return value.Double(0), nil
	}
	return maxv, nil
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
	v, e, err := m.merge(v1, e1, v2, e2)
	return value.NewList(v, value.Bool(e)), err
}

func (m rollingMax) Zero() value.Value {
	return zeroMax
}

var _ Histogram = rollingMax{}
