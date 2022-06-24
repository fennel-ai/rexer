package counter

import (
	"fmt"

	"fennel/lib/aggregate"
	"fennel/lib/value"
)

/*
	Maintains a rolling average by storing a pair of ints (denoting sum and count)
	in each bucket representing the total sum / count of events within that bucket.
*/
type average struct {
	opts aggregate.Options
}

var _ MergeReduce = average{}

var zeroAvg value.Value = value.NewList(value.Int(0), value.Int(0))

func NewAverage(options aggregate.Options) average {
	return average{options}
}

func (r average) Options() aggregate.Options {
	return r.opts
}

func (r average) Transform(v value.Value) (value.Value, error) {
	if err := value.Types.Number.Validate(v); err != nil {
		return nil, fmt.Errorf("value [%s] is not a number", v.String())
	}
	return value.NewList(v, value.Int(1)), nil
}

func (r average) extract(v value.Value) (value.Value, int64, error) {
	l, ok := v.(value.List)
	if !ok || l.Len() != 2 {
		return nil, 0, fmt.Errorf("expected list of two elements but got: %v", v)
	}
	a, _ := l.At(0)
	f, _ := l.At(1)
	b, ok := f.(value.Int)
	if !ok {
		return nil, 0, fmt.Errorf("expected integer but found: %v", f)
	}
	return a, int64(b), nil
}

func (r average) ratio(sum value.Value, num int64) value.Value {
	if num == 0 {
		return value.Double(0)
	} else {
		ret, _ := sum.Op("/", value.Int(num))
		return ret
	}
}

func (r average) Reduce(values []value.Value) (value.Value, error) {
	num := int64(0)
	sum := value.Value(value.Int(0))
	for i := range values {
		a, b, err := r.extract(values[i])
		if err != nil {
			return nil, err
		}
		if sum, err = sum.Op("+", a); err != nil {
			return nil, err
		}
		num += b
	}
	return r.ratio(sum, num), nil
}

func (r average) Merge(a, b value.Value) (value.Value, error) {
	s1, n1, err := r.extract(a)
	if err != nil {
		return nil, err
	}
	s2, n2, err := r.extract(b)
	if err != nil {
		return nil, err
	}
	ret, err := s1.Op("+", s2)
	if err != nil {
		return nil, err
	}
	return value.NewList(ret, value.Int(n1+n2)), nil
}

func (r average) Zero() value.Value {
	return zeroAvg
}
