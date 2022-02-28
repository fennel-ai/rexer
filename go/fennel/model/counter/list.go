package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

type List struct {
	Duration uint64
}

func (s List) extract(v value.Value) (value.List, error) {
	l, ok := v.(value.List)
	if !ok {
		return value.List{}, fmt.Errorf("value expected to be list but instead found: %v", v)
	}
	return l, nil
}

func (s List) Start(end ftypes.Timestamp) ftypes.Timestamp {
	return start(end, s.Duration)
}

// Reduce just appends all the lists to an empty list
func (s List) Reduce(values []value.Value) (value.Value, error) {
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

func (s List) Merge(a, b value.Value) (value.Value, error) {
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

func (s List) Zero() value.Value {
	return value.List{}
}

func (s List) Bucketize(groupkey string, v value.Value, timestamp ftypes.Timestamp) ([]Bucket, error) {
	return BucketizeMoment(groupkey, timestamp, value.List{v}, s.Windows()), nil
}

func (s List) Windows() []ftypes.Window {
	return []ftypes.Window{
		ftypes.Window_MINUTE, ftypes.Window_HOUR, ftypes.Window_DAY,
	}
}

var _ Histogram = List{}
