package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

/*
	Maintains a rolling average by storing a pair of ints (denoting sum and count)
	in each bucket representing the total sum / count of events within that bucket.
*/
type RollingAverage struct {
	Duration uint64
}

func (r RollingAverage) Bucketize(groupkey string, v value.Value, timestamp ftypes.Timestamp) ([]Bucket, error) {
	v_int, ok := v.(value.Int)
	if !ok {
		return nil, fmt.Errorf("expected value to be an int but got: '%s' instead", v)
	}
	c := value.List{v_int, value.Int(1)}
	return BucketizeMoment(groupkey, timestamp, c, r.Windows()), nil
}

func (r RollingAverage) Start(end ftypes.Timestamp) ftypes.Timestamp {
	d := ftypes.Timestamp(r.Duration)
	if end > d {
		return end - d
	}
	return ftypes.Timestamp(0)
}

func (r RollingAverage) extract(v value.Value) (int64, int64, error) {
	l, ok := v.(value.List)
	if !ok || len(l) != 2 {
		return 0, 0, fmt.Errorf("expected list of two elements but got: %v", v)
	}
	a, ok := l[0].(value.Int)
	if !ok {
		return 0, 0, fmt.Errorf("expected integer but found: %v", l[0])
	}
	b, ok := l[1].(value.Int)
	if !ok {
		return 0, 0, fmt.Errorf("expected integer but found: %v", l[1])
	}
	return int64(a), int64(b), nil
}

func (r RollingAverage) ratio(sum, num int64) value.Double {
	if num == 0 {
		return value.Double(0)
	} else {
		d := float64(sum) / float64(num)
		return value.Double(d)
	}
}

func (r RollingAverage) Reduce(values []value.Value) (value.Value, error) {
	var num, sum int64
	for i := range values {
		a, b, err := r.extract(values[i])
		if err != nil {
			return nil, err
		}
		sum += a
		num += b
	}
	return r.ratio(sum, num), nil
}

func (r RollingAverage) Merge(a, b value.Value) (value.Value, error) {
	s1, n1, err := r.extract(a)
	if err != nil {
		return nil, err
	}
	s2, n2, err := r.extract(b)
	if err != nil {
		return nil, err
	}
	return value.List{value.Int(s1 + s2), value.Int(n1 + n2)}, nil
}

func (r RollingAverage) Zero() value.Value {
	return value.List{value.Int(0), value.Int(0)}
}

func (r RollingAverage) Windows() []ftypes.Window {
	return []ftypes.Window{
		ftypes.Window_MINUTE, ftypes.Window_HOUR, ftypes.Window_DAY,
	}
}

var _ Histogram = RollingAverage{}
