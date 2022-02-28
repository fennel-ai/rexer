package counter

import (
	"fmt"
	"math"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

type Stddev struct {
	Duration uint64
}

func (s Stddev) Start(end ftypes.Timestamp) ftypes.Timestamp {
	d := ftypes.Timestamp(s.Duration)
	if end > d {
		return end - d
	}
	return ftypes.Timestamp(0)
}

func (s Stddev) eval(sum, sumsq, num int64) value.Double {
	if num == 0 {
		return value.Double(0)
	} else {
		a := float64(sumsq) / float64(num)
		b := float64(sum) / float64(num)
		return value.Double(math.Sqrt(a - b*b))
	}
}

func (s Stddev) extract(v value.Value) (int64, int64, int64, error) {
	l, ok := v.(value.List)
	if !ok || len(l) != 3 {
		return 0, 0, 0, fmt.Errorf("expected list of three elements but got: %v", v)
	}
	sum, ok := l[0].(value.Int)
	if !ok {
		return 0, 0, 0, fmt.Errorf("expected integer but found: %v", l[0])
	}
	sumSq, ok := l[1].(value.Int)
	if !ok {
		return 0, 0, 0, fmt.Errorf("expected integer but found: %v", l[1])
	}
	num, ok := l[2].(value.Int)
	if !ok {
		return 0, 0, 0, fmt.Errorf("expected integer but found: %v", l[2])
	}
	return int64(sum), int64(sumSq), int64(num), nil
}

func (s Stddev) merge(s1, ssq1, n1, s2, ssq2, n2 int64) (int64, int64, int64) {
	return s1 + s2, ssq1 + ssq2, n1 + n2
}

func (s Stddev) Reduce(values []value.Value) (value.Value, error) {
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

func (s Stddev) Merge(a, b value.Value) (value.Value, error) {
	s1, ssq1, n1, err := s.extract(a)
	if err != nil {
		return nil, err
	}
	s2, ssq2, n2, err := s.extract(b)
	if err != nil {
		return nil, err
	}
	sum, sumsq, num := s.merge(s1, ssq1, n1, s2, ssq2, n2)
	return value.List{value.Int(sum), value.Int(sumsq), value.Int(num)}, nil
}

func (s Stddev) Zero() value.Value {
	return value.List{value.Int(0), value.Int(0), value.Int(0)}
}

func (s Stddev) Bucketize(groupkey string, v value.Value, timestamp ftypes.Timestamp) ([]Bucket, error) {
	v_int, ok := v.(value.Int)
	if !ok {
		return nil, fmt.Errorf("expected value to be an int but got: '%s' instead", v)
	}
	c := value.List{v_int, v_int * v_int, value.Int(1)}
	return BucketizeMoment(groupkey, timestamp, c, s.Windows()), nil
}

func (s Stddev) Windows() []ftypes.Window {
	return []ftypes.Window{
		ftypes.Window_MINUTE, ftypes.Window_HOUR, ftypes.Window_DAY,
	}
}

var _ Histogram = Stddev{}
