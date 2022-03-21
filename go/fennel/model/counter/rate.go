package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/utils/math"
	"fennel/lib/value"
)

/*
	rollingRate maintains a rate (say actions per click)
	It stores two numbers - num (numerator) and den (denominator)
*/
type rollingRate struct {
	Duration  uint64
	Normalize bool
	Bucketizer
	BucketStore
}

func NewRate(name ftypes.AggName, duration uint64, normalize bool) Histogram {
	return rollingRate{
		Duration:  duration,
		Normalize: normalize,
		Bucketizer: fixedWidthBucketizer{map[ftypes.Window]uint64{
			ftypes.Window_MINUTE: 6,
			ftypes.Window_DAY:    1,
		}, true},
		// retain all keys for 1.5days + duration
		BucketStore: NewTwoLevelStorage(name, 24*3600, duration+24*3600*1.5),
	}
}

func (r rollingRate) Transform(v value.Value) (value.Value, error) {
	a, b, err := r.extract(v)
	if err != nil {
		return nil, err
	}
	return value.NewList(value.Int(a), value.Int(b)), nil
}

func (r rollingRate) Start(end ftypes.Timestamp, kwargs value.Dict) (ftypes.Timestamp, error) {
	d, err := extractDuration(kwargs, r.Duration)
	if err != nil {
		return 0, err
	}
	return start(end, d), nil
}

func (r rollingRate) extract(v value.Value) (int64, int64, error) {
	l, ok := v.(value.List)
	if !ok || l.Len() != 2 {
		return 0, 0, fmt.Errorf("expected list of two elements but got: %v", v)
	}
	e, _ := l.At(0)
	first, ok := e.(value.Int)
	if !ok {
		return 0, 0, fmt.Errorf("expected int but found: %v", e)
	}
	e, _ = l.At(1)
	second, ok := e.(value.Int)
	if !ok {
		return 0, 0, fmt.Errorf("expected int but found: %v", e)
	}
	if first < 0 || second < 0 {
		return 0, 0, fmt.Errorf("numerator & denominator should be non-negative but found: '%s', '%s' instead", first, second)
	}
	return int64(first), int64(second), nil
}

func (r rollingRate) Reduce(values []value.Value) (value.Value, error) {
	var num, den int64 = 0, 0
	for _, v := range values {
		n, d, err := r.extract(v)
		if err != nil {
			return nil, err
		}
		num += n
		den += d
	}
	if den == 0 {
		return value.Double(0), nil
	}
	if r.Normalize && num > den {
		return nil, fmt.Errorf("normalized rate requires numerator to be <= denominator but found '%d', '%d'", num, den)
	}
	var ratio float64
	var err error
	if r.Normalize {
		ratio, err = math.Wilson(uint64(num), uint64(den), true)
		if err != nil {
			return nil, err
		}
	} else {
		ratio = float64(num) / float64(den)
	}
	return value.Double(ratio), nil
}

func (r rollingRate) Merge(a, b value.Value) (value.Value, error) {
	n1, d1, err := r.extract(a)
	if err != nil {
		return nil, err
	}
	n2, d2, err := r.extract(b)
	if err != nil {
		return nil, err
	}
	return value.NewList(value.Int(n1+n2), value.Int(d1+d2)), nil
}

func (r rollingRate) Zero() value.Value {
	return value.NewList(value.Int(0), value.Int(0))
}

var _ Histogram = rollingRate{}
