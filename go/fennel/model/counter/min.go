package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

/*
	Min maintains minimum of a bucket with two vars (minv and empty).
	Minv is the minimum value. If empty is true, the bucket is empty so minv is ignored.
*/
type Min struct {
	Duration uint64
}

func min(a int64, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func (m Min) Start(end ftypes.Timestamp) ftypes.Timestamp {
	return end - ftypes.Timestamp(m.Duration)
}

func (m Min) extract(v value.Value) (int64, bool, error) {
	l, ok := v.(value.List)
	if !ok || len(l) != 2 {
		return 0, false, fmt.Errorf("expected list of two elements but got: %v", v)
	}
	empty, ok := l[1].(value.Bool)
	if !ok {
		return 0, false, fmt.Errorf("expected boolean but found: %v", l[1])
	}
	if empty {
		return 0, true, nil
	}
	minv, ok := l[0].(value.Int)
	if !ok {
		return 0, false, fmt.Errorf("expected integer but found: %v", minv)
	}
	return int64(minv), false, nil
}

func (m Min) merge(v1 int64, e1 bool, v2 int64, e2 bool) (int64, bool) {
	if e1 {
		return v2, e2
	}
	if e2 {
		return v1, e1
	}
	return min(v1, v2), false
}

func (m Min) Reduce(values []value.Value) (value.Value, error) {
	var minv int64 = 0
	empty := true
	for _, v := range values {
		v, e, err := m.extract(v)
		if err != nil {
			return nil, err
		}
		minv, empty = m.merge(minv, empty, v, e)
	}
	return value.Int(minv), nil
}

func (m Min) Merge(a, b value.Value) (value.Value, error) {
	v1, e1, err := m.extract(a)
	if err != nil {
		return nil, err
	}
	v2, e2, err := m.extract(b)
	if err != nil {
		return nil, err
	}
	v, e := m.merge(v1, e1, v2, e2)
	return value.List{value.Int(v), value.Bool(e)}, nil
}

func (m Min) Zero() value.Value {
	return value.List{value.Int(0), value.Bool(true)}
}

func (m Min) Bucketize(actions value.Table) ([]Bucket, error) {
	schema := actions.Schema()
	type_, ok := schema["groupkey"]
	if !ok {
		return nil, fmt.Errorf("query does not create column called 'groupkey'")
	}
	type_, ok = schema["timestamp"]
	if !ok || type_ != value.Types.Int {
		return nil, fmt.Errorf("query does not create column called 'timestamp' with datatype of 'int'")
	}
	type_, ok = schema["value"]
	if !ok || type_ != value.Types.Int {
		return nil, fmt.Errorf("query does not create column called 'amount' with datatype of 'int'")
	}
	buckets := make([]Bucket, 0, actions.Len())
	for _, row := range actions.Pull() {
		ts := row["timestamp"].(value.Int)
		key := row["groupkey"].String()
		amount := row["value"].(value.Int)
		c := value.List{amount, value.Bool(false)}
		buckets = append(buckets, BucketizeMoment(key, ftypes.Timestamp(ts), c, m.Windows())...)
	}
	return buckets, nil
}

func (m Min) Windows() []ftypes.Window {
	return []ftypes.Window{
		ftypes.Window_MINUTE, ftypes.Window_HOUR, ftypes.Window_DAY,
	}
}

var _ Histogram = Min{}
