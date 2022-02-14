package counter

import (
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fmt"
)

type RollingCounter struct {
	Duration uint64
}

func (r RollingCounter) Start(end ftypes.Timestamp) ftypes.Timestamp {
	return end - ftypes.Timestamp(r.Duration)
}

func (r RollingCounter) Reduce(values []value.Value) (value.Value, error) {
	var total value.Value = value.Int(0)
	var err error
	for i := range values {
		total, err = total.Op("+", values[i])
		if err != nil {
			return nil, err
		}
	}
	return total, nil
}

func (r RollingCounter) Merge(a, b value.Value) (value.Value, error) {
	if _, ok := a.(value.Int); !ok {
		return nil, fmt.Errorf("expected int but got: %v", a)
	}
	return a.Op("+", b)
}

func (r RollingCounter) Zero() value.Value {
	return value.Int(0)
}

func (r RollingCounter) Bucketize(table value.Table) ([]Bucket, error) {
	schema := table.Schema()
	type_, ok := schema["key"]
	if !ok {
		return nil, fmt.Errorf("query does not create column called 'key'")
	}
	type_, ok = schema["timestamp"]
	if !ok || type_ != value.Types.Int {
		return nil, fmt.Errorf("query does not create column called 'timestamp' with datatype of 'int'")
	}
	buckets := make([]Bucket, 0, table.Len())
	for _, row := range table.Pull() {
		ts := row["timestamp"].(value.Int)
		key := row["key"].String()
		buckets = append(buckets, BucketizeMoment(key, ftypes.Timestamp(ts), value.Int(1), r.Windows())...)
	}
	return buckets, nil
}

func (r RollingCounter) Windows() []ftypes.Window {
	return []ftypes.Window{
		ftypes.Window_MINUTE, ftypes.Window_HOUR, ftypes.Window_DAY,
	}
}

var _ Histogram = RollingCounter{}
