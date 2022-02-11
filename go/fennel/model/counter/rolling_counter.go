package counter

import (
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fmt"
	"strconv"
)

type RollingCounter struct {
	Duration uint64
}

func (r RollingCounter) Start(end ftypes.Timestamp) ftypes.Timestamp {
	return end - ftypes.Timestamp(r.Duration)
}

func (r RollingCounter) Reduce(values []int64) (value.Value, error) {
	var total int64 = 0
	for i, _ := range values {
		total += values[i]
	}
	return value.Int(total), nil
}

func (r RollingCounter) Merge(a, b int64) int64 {
	return a + b
}

func (r RollingCounter) Empty() int64 {
	return 0
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
		buckets = append(buckets, BucketizeMoment(key, ftypes.Timestamp(ts), 1, r.Windows())...)
	}
	return buckets, nil
}

func (r RollingCounter) Windows() []ftypes.Window {
	return []ftypes.Window{
		ftypes.Window_MINUTE, ftypes.Window_HOUR, ftypes.Window_DAY,
	}
}

func (r RollingCounter) Marshal(v int64) (string, error) {
	return fmt.Sprintf("%d", v), nil
}

func (r RollingCounter) Unmarshal(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

var _ Histogram = RollingCounter{}
