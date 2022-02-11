package counter

import (
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fmt"
	"strconv"
)

type TimeseriesCounter struct {
	Window ftypes.Window
	Limit  uint64
}

func (r TimeseriesCounter) Start(end ftypes.Timestamp) ftypes.Timestamp {
	var start ftypes.Timestamp
	switch r.Window {
	case ftypes.Window_HOUR:
		start = end - ftypes.Timestamp(1+r.Limit)*3600
	case ftypes.Window_DAY:
		start = end - ftypes.Timestamp(1+r.Limit)*3600*24
	}
	if start < 0 {
		start = 0
	}
	return start
}

func (r TimeseriesCounter) Reduce(values []int64) (value.Value, error) {
	// we have to take the last Limit values only and if there are fewer than that
	// available we pad a few entries with zeros.
	limit := int(r.Limit)
	last := len(values) - 1
	ret := make([]value.Value, r.Limit)
	var i int
	for i = 0; i < limit && i < len(values); i++ {
		ret[limit-1-i] = value.Int(values[last-i])
	}
	for ; i < limit; i++ {
		ret[limit-1-i] = value.Int(0)
	}
	return value.List(ret), nil
}

func (r TimeseriesCounter) Merge(a, b int64) int64 {
	return a + b
}

func (r TimeseriesCounter) Empty() int64 {
	return 0
}

func (r TimeseriesCounter) Bucketize(table value.Table) ([]Bucket, error) {
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

func (r TimeseriesCounter) Windows() []ftypes.Window {
	return []ftypes.Window{r.Window}
}

func (r TimeseriesCounter) Marshal(v int64) (string, error) {
	return fmt.Sprintf("%d", v), nil
}

func (r TimeseriesCounter) Unmarshal(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

var _ Histogram = TimeseriesCounter{}
