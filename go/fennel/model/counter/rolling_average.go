package counter

import (
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fmt"
	"strconv"
	"strings"
)

/*
	Maintains a rolling average by storing a pair of ints (denoting sum and count)
	in each bucket representing the total sum / count of events within that bucket.
*/
type RollingAverage struct {
	Duration uint64
}

func (r RollingAverage) Start(end ftypes.Timestamp) ftypes.Timestamp {
	return end - ftypes.Timestamp(r.Duration)
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

func (r RollingAverage) Bucketize(actions value.Table) ([]Bucket, error) {
	schema := actions.Schema()
	type_, ok := schema["key"]
	if !ok {
		return nil, fmt.Errorf("query does not create column called 'key'")
	}
	type_, ok = schema["timestamp"]
	if !ok || type_ != value.Types.Int {
		return nil, fmt.Errorf("query does not create column called 'timestamp' with datatype of 'int'")
	}
	type_, ok = schema["amount"]
	if !ok || type_ != value.Types.Int {
		return nil, fmt.Errorf("query does not create column called 'amount' with datatype of 'int'")
	}
	buckets := make([]Bucket, 0, actions.Len())
	for _, row := range actions.Pull() {
		ts := row["timestamp"].(value.Int)
		key := row["key"].String()
		amount := row["amount"].(value.Int)
		c := value.List{amount, value.Int(1)}
		buckets = append(buckets, BucketizeMoment(key, ftypes.Timestamp(ts), c, r.Windows())...)
	}
	return buckets, nil
}

func (r RollingAverage) Windows() []ftypes.Window {
	return []ftypes.Window{
		ftypes.Window_MINUTE, ftypes.Window_HOUR, ftypes.Window_DAY,
	}
}

func (r RollingAverage) Marshal(v value.Value) (string, error) {
	sum, num, err := r.extract(v)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d,%d", sum, num), nil
}

func (r RollingAverage) Unmarshal(s string) (value.Value, error) {
	l := strings.Split(s, ",")
	if len(l) != 2 {
		return nil, fmt.Errorf("expected two comma separated ints, but found: %v", s)
	}
	sum, err := strconv.ParseInt(l[0], 10, 64)
	if err != nil {
		return nil, err
	}
	num, err := strconv.ParseInt(l[1], 10, 64)
	if err != nil {
		return nil, err
	}
	return value.List{value.Int(sum), value.Int(num)}, nil
}

var _ Histogram = RollingAverage{}
