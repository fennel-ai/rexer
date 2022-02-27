package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

/*
	Max maintains maximum of a bucket with two vars (maxv and empty).
	Maxv is the maximum value. If empty is true, the bucket is empty so maxv is ignored.
*/
type Max struct {
	Duration uint64
}

func max(a int64, b int64) int64 {
	if a < b {
		return b
	} else {
		return a
	}
}

func (m Max) Start(end ftypes.Timestamp) ftypes.Timestamp {
	return end - ftypes.Timestamp(m.Duration)
}

func (m Max) extract(v value.Value) (int64, bool, error) {
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
	maxv, ok := l[0].(value.Int)
	if !ok {
		return 0, false, fmt.Errorf("expected integer but found: %v", maxv)
	}
	return int64(maxv), false, nil
}

func (m Max) merge(v1 int64, e1 bool, v2 int64, e2 bool) (int64, bool) {
	if e1 {
		return v2, e2
	}
	if e2 {
		return v1, e1
	}
	return max(v1, v2), false
}

func (m Max) Reduce(values []value.Value) (value.Value, error) {
	var maxv int64 = 0
	empty := true
	for _, v := range values {
		v, e, err := m.extract(v)
		if err != nil {
			return nil, err
		}
		maxv, empty = m.merge(maxv, empty, v, e)
	}
	return value.Int(maxv), nil
}

func (m Max) Merge(a, b value.Value) (value.Value, error) {
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

func (m Max) Zero() value.Value {
	return value.List{value.Int(0), value.Bool(true)}
}

func (m Max) Bucketize(actions value.Table) ([]Bucket, error) {
	schema := actions.Schema()
	_, ok := schema["groupkey"]
	if !ok {
		return nil, fmt.Errorf("query does not create column called 'groupkey'")
	}
	type_, ok := schema["timestamp"]
	if !ok || type_ != value.Types.Int {
		return nil, fmt.Errorf("query does not create column called 'timestamp' with datatype of 'int'")
	}
	type_, ok = schema["value"]
	if !ok || type_ != value.Types.Int {
		return nil, fmt.Errorf("query does not create column called 'value' with datatype of 'int'")
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

func (m Max) Windows() []ftypes.Window {
	return []ftypes.Window{
		ftypes.Window_MINUTE, ftypes.Window_HOUR, ftypes.Window_DAY,
	}
}

var _ Histogram = Max{}
