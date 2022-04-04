package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

type list struct {
	Durations []uint64
	Bucketizer
	BucketStore
}

func (s list) Transform(v value.Value) (value.Value, error) {
	return value.NewList(v), nil
}

func NewList(name ftypes.AggName, durations []uint64) Histogram {
	maxDuration := getMaxDuration(durations)
	return list{
		Durations: durations,
		Bucketizer: fixedWidthBucketizer{map[ftypes.Window]uint64{
			ftypes.Window_MINUTE: 6,
			ftypes.Window_DAY:    1,
		}, true},
		// retain all keys for 1.5days + duration
		BucketStore: NewTwoLevelStorage(24*3600, maxDuration+24*3600*1.5),
	}
}

func (s list) extract(v value.Value) (value.List, error) {
	l, ok := v.(value.List)
	if !ok {
		return value.List{}, fmt.Errorf("value expected to be list but instead found: %v", v)
	}
	return l, nil
}

func (s list) Start(end ftypes.Timestamp, kwargs value.Dict) (ftypes.Timestamp, error) {
	d, err := extractDuration(kwargs, s.Durations)
	if err != nil {
		return 0, err
	}
	return start(end, d), nil
}

// Reduce just appends all the lists to an empty list
func (s list) Reduce(values []value.Value) (value.Value, error) {
	z := s.Zero().(value.List)
	for i := range values {
		l, err := s.extract(values[i])
		if err != nil {
			return nil, err
		}
		for j := 0; j < l.Len(); j++ {
			v, err := l.At(j)
			if err != nil {
				return nil, err
			}
			z.Append(v)
		}
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
	ret := value.NewList()
	for i := 0; i < la.Len(); i++ {
		v, err := la.At(i)
		if err != nil {
			return nil, err
		}
		ret.Append(v)
	}
	for i := 0; i < lb.Len(); i++ {
		v, err := lb.At(i)
		if err != nil {
			return nil, err
		}
		ret.Append(v)
	}
	return ret, nil
}

func (s list) Zero() value.Value {
	return value.NewList()
}

var _ Histogram = list{}
