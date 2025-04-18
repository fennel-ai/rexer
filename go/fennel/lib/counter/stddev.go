package counter

import (
	"fmt"
	"math"

	"fennel/lib/aggregate"
	"fennel/lib/value"
)

type rollingStdDev struct {
	opts aggregate.Options
}

var _ MergeReduce = rollingStdDev{}

var zeroStddev value.Value = value.NewList(value.Double(0), value.Double(0), value.Int(0))

func NewStdDev(opts aggregate.Options) rollingStdDev {
	return rollingStdDev{opts}
}

func (r rollingStdDev) Options() aggregate.Options {
	return r.opts
}

func (s rollingStdDev) eval(sum, sumsq float64, num int64) value.Double {
	if num == 0 {
		return value.Double(0)
	} else {
		a := sumsq / float64(num)
		b := sum / float64(num)
		return value.Double(math.Sqrt(a - b*b))
	}
}

func (s rollingStdDev) extract(v value.Value) (float64, float64, int64, error) {
	l, ok := v.(value.List)
	if !ok || l.Len() != 3 {
		return 0, 0, 0, fmt.Errorf("expected list of three elements but got: %v", v)
	}
	e, _ := l.At(0)
	sum, err := getDouble(e)
	if err != nil {
		return 0, 0, 0, err
	}
	e, _ = l.At(1)
	sumSq, err := getDouble(e)
	if err != nil {
		return 0, 0, 0, err
	}
	e, _ = l.At(2)
	num, ok := e.(value.Int)
	if !ok {
		return 0, 0, 0, fmt.Errorf("expected integer but found: %v", e)
	}
	return sum, sumSq, int64(num), nil
}

func (s rollingStdDev) merge(s1, ssq1 float64, n1 int64, s2, ssq2 float64, n2 int64) (float64, float64, int64) {
	return s1 + s2, ssq1 + ssq2, n1 + n2
}

func (s rollingStdDev) Reduce(values []value.Value) (value.Value, error) {
	var sum, sumsq float64 = 0, 0
	var num int64 = 0
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
	return value.NewList(value.Double(sum), value.Double(sumsq), value.Int(num)), nil
}

func (s rollingStdDev) Zero() value.Value {
	return zeroStddev
}

func (s rollingStdDev) Transform(v value.Value) (value.Value, error) {
	vDouble, err := getDouble(v)
	if err != nil {
		return nil, fmt.Errorf("%w", err)
	}
	return value.NewList(value.Double(vDouble), value.Double(vDouble*vDouble), value.Int(1)), nil
}
