package counter

import (
	"fmt"
	"math"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

type rollingStdDev struct {
	Durations []uint64
	Bucketizer
	BucketStore
}

func NewStdDev(durations []uint64) Histogram {
	maxDuration := getMaxDuration(durations)
	return rollingStdDev{
		Durations: durations,
		Bucketizer: fixedWidthBucketizer{map[ftypes.Window]uint64{
			ftypes.Window_MINUTE: 6,
			ftypes.Window_DAY:    1,
		}, true},
		// retain all keys for 1.5days + duration
		BucketStore: NewTwoLevelStorage(24*3600, maxDuration+24*3600*1.5),
	}
}

func (s rollingStdDev) Start(end ftypes.Timestamp, kwargs value.Dict) (ftypes.Timestamp, error) {
	d, err := extractDuration(kwargs, s.Durations)
	if err != nil {
		return 0, err
	}
	return start(end, d), nil
}

func (s rollingStdDev) eval(sum, sumsq, num int64) value.Double {
	if num == 0 {
		return value.Double(0)
	} else {
		a := float64(sumsq) / float64(num)
		b := float64(sum) / float64(num)
		return value.Double(math.Sqrt(a - b*b))
	}
}

func (s rollingStdDev) extract(v value.Value) (int64, int64, int64, error) {
	l, ok := v.(value.List)
	if !ok || l.Len() != 3 {
		return 0, 0, 0, fmt.Errorf("expected list of three elements but got: %v", v)
	}
	e, _ := l.At(0)
	sum, ok := e.(value.Int)
	if !ok {
		return 0, 0, 0, fmt.Errorf("expected integer but found: %v", e)
	}
	e, _ = l.At(1)
	sumSq, ok := e.(value.Int)
	if !ok {
		return 0, 0, 0, fmt.Errorf("expected integer but found: %v", e)
	}
	e, _ = l.At(2)
	num, ok := e.(value.Int)
	if !ok {
		return 0, 0, 0, fmt.Errorf("expected integer but found: %v", e)
	}
	return int64(sum), int64(sumSq), int64(num), nil
}

func (s rollingStdDev) merge(s1, ssq1, n1, s2, ssq2, n2 int64) (int64, int64, int64) {
	return s1 + s2, ssq1 + ssq2, n1 + n2
}

func (s rollingStdDev) Reduce(values []value.Value) (value.Value, error) {
	var sum, sumsq, num int64 = 0, 0, 0
	for _, v := range values {
		sum_, sumsq_, num_, err := s.extract(v)
		if err != nil {
			return nil, err
		}
		sum, sumsq, num = s.merge(sum, sumsq, num, sum_, sumsq_, num_)
	}
	return s.eval(sum, sumsq, num), nil
}

func (s rollingStdDev) Merge(a, b value.Value) (value.Value, error) {
	s1, ssq1, n1, err := s.extract(a)
	if err != nil {
		return nil, err
	}
	s2, ssq2, n2, err := s.extract(b)
	if err != nil {
		return nil, err
	}
	sum, sumsq, num := s.merge(s1, ssq1, n1, s2, ssq2, n2)
	return value.NewList(value.Int(sum), value.Int(sumsq), value.Int(num)), nil
}

func (s rollingStdDev) Zero() value.Value {
	return value.NewList(value.Int(0), value.Int(0), value.Int(0))
}

func (s rollingStdDev) Transform(v value.Value) (value.Value, error) {
	v_int, ok := v.(value.Int)
	if !ok {
		return nil, fmt.Errorf("expected value to be an int but got: '%s' instead", v)
	}
	return value.NewList(v_int, v_int*v_int, value.Int(1)), nil
}

var _ Histogram = rollingStdDev{}
