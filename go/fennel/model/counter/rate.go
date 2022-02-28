package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/utils/math"
	"fennel/lib/value"
)

/*
	Rate maintains a rate (say actions per click)
	It stores two numbers - num (numerator) and den (denominator)
*/
type Rate struct {
	Duration  uint64
	normalize bool
}

func (r Rate) Bucketize(groupkey string, v value.Value, timestamp ftypes.Timestamp) ([]Bucket, error) {
	a, b, err := r.extract(v)
	if err != nil {
		return nil, err
	}
	c := value.List{value.Int(a), value.Int(b)}
	return BucketizeMoment(groupkey, timestamp, c, r.Windows()), nil
}

func (r Rate) Start(end ftypes.Timestamp) ftypes.Timestamp {
	return start(end, r.Duration)
}

func (r Rate) extract(v value.Value) (int64, int64, error) {
	l, ok := v.(value.List)
	if !ok || len(l) != 2 {
		return 0, 0, fmt.Errorf("expected list of two elements but got: %v", v)
	}
	first, ok := l[0].(value.Int)
	if !ok {
		return 0, 0, fmt.Errorf("expected int but found: %v", l[1])
	}
	second, ok := l[1].(value.Int)
	if !ok {
		return 0, 0, fmt.Errorf("expected int but found: %v", l[1])
	}
	if first < 0 || second < 0 {
		return 0, 0, fmt.Errorf("numerator & denominator should be non-negative but found: '%s', '%s' instead", first, second)
	}
	return int64(first), int64(second), nil
}

func (r Rate) Reduce(values []value.Value) (value.Value, error) {
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
	if r.normalize && num > den {
		return nil, fmt.Errorf("normalized rate requires numerator to be <= denominator but found '%d', '%d'", num, den)
	}
	var ratio float64
	var err error
	if r.normalize {
		ratio, err = math.Wilson(uint64(num), uint64(den), true)
		if err != nil {
			return nil, err
		}
	} else {
		ratio = float64(num) / float64(den)
	}
	return value.Double(ratio), nil
}

func (r Rate) Merge(a, b value.Value) (value.Value, error) {
	n1, d1, err := r.extract(a)
	if err != nil {
		return nil, err
	}
	n2, d2, err := r.extract(b)
	if err != nil {
		return nil, err
	}
	return value.List{value.Int(n1 + n2), value.Int(d1 + d2)}, nil
}

func (r Rate) Zero() value.Value {
	return value.List{value.Int(0), value.Int(0)}
}

func (r Rate) Windows() []ftypes.Window {
	return []ftypes.Window{
		ftypes.Window_MINUTE, ftypes.Window_HOUR, ftypes.Window_DAY,
	}
}

var _ Histogram = Rate{}
