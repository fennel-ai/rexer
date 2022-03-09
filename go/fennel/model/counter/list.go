package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

type list struct {
	Duration uint64
	Bucketizer
	BucketStore
}

func (s list) Transform(v value.Value) (value.Value, error) {
	return value.List{v}, nil
}

func NewList(name ftypes.AggName, duration uint64) Histogram {
	return list{
		Duration: duration,
		Bucketizer: FixedWidthBucketizer{windows: []ftypes.Window{
			ftypes.Window_MINUTE, ftypes.Window_DAY, ftypes.Window_HOUR,
		}},
		BucketStore: FlatRedisStorage{name: name},
	}
}

func (s list) extract(v value.Value) (value.List, error) {
	l, ok := v.(value.List)
	if !ok {
		return value.List{}, fmt.Errorf("value expected to be list but instead found: %v", v)
	}
	return l, nil
}

func (s list) Start(end ftypes.Timestamp) ftypes.Timestamp {
	return start(end, s.Duration)
}

// Reduce just appends all the lists to an empty list
func (s list) Reduce(values []value.Value) (value.Value, error) {
	z := s.Zero().(value.List)
	for i := range values {
		l, err := s.extract(values[i])
		if err != nil {
			return nil, err
		}
		z = append(z, l...)
	}
	return z, nil
}

func (s list) Merge(a, b value.Value) (value.Value, error) {
	la, err := s.extract(a)
	if err != nil {
		return nil, err
	}
	lb, err := s.extract(b)
	if err != nil {
		return nil, err
	}
	ret := make([]value.Value, 0, len(la)+len(lb))
	ret = append(ret, la...)
	ret = append(ret, lb...)
	return value.List(ret), nil
}

func (s list) Zero() value.Value {
	return value.List{}
}

var _ Histogram = list{}
