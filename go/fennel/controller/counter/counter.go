package counter

import (
	"fennel/instance"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/model/counter"
	"fmt"
)

func RollingValue(instance instance.Instance, agg aggregate.Aggregate, key value.Value) (value.Int, error) {
	end := ftypes.Timestamp(instance.Clock.Now())
	start := end - ftypes.Timestamp(agg.Options.Duration)
	buckets := counter.BucketizeDuration(makeKey(key), start, end)
	counts, err := counter.GetMulti(instance, agg.Name, buckets)
	if err != nil {
		return value.Int(0), err
	}
	var total int64
	for i, _ := range counts {
		total += counts[i]
	}
	return value.Int(total), nil
}

func TimeseriesValue(instance instance.Instance, agg aggregate.Aggregate, key value.Value) (value.List, error) {
	end := ftypes.Timestamp(instance.Clock.Now())
	var start ftypes.Timestamp
	switch agg.Options.Window {
	case ftypes.Window_HOUR:
		start = end - ftypes.Timestamp(1+agg.Options.Limit)*3600
	case ftypes.Window_DAY:
		start = end - ftypes.Timestamp(1+agg.Options.Limit)*3600*24
	default:
		return value.List{}, fmt.Errorf("invalid aggregate window")
	}
	if start < 0 {
		start = 0
	}
	buckets, err := counter.BucketizeTimeseries(makeKey(key), start, end, agg.Options.Window)
	if err != nil {
		return value.List{}, err
	}
	counts, err := counter.GetMulti(instance, agg.Name, buckets)
	if err != nil {
		return value.List{}, err
	}

	// we have to take the last limit values only and if there are fewer than that
	// available we pad a few entries with zeros.
	limit := int(agg.Options.Limit)

	last := len(counts) - 1
	ret := make([]value.Value, limit)
	for i := 0; i < limit && i < len(counts); i++ {
		ret[limit-1-i] = value.Int(counts[last-i])
	}
	return ret, nil
}

func Update(instance instance.Instance, aggname ftypes.AggName, table value.Table) error {
	schema := table.Schema()
	type_, ok := schema["key"]
	if !ok {
		return fmt.Errorf("query does not create column called 'key'")
	}
	type_, ok = schema["timestamp"]
	if !ok || type_ != value.Types.Int {
		return fmt.Errorf("query does not create column called 'timestamp' with datatype of 'int'")
	}
	buckets := make([]counter.Bucket, 0, table.Len())
	for _, row := range table.Pull() {
		ts := row["timestamp"].(value.Int)
		key := makeKey(row["key"])
		buckets = append(buckets, counter.BucketizeMoment(key, ftypes.Timestamp(ts), 1)...)
	}
	buckets = counter.MergeBuckets(buckets)
	return counter.IncrementMulti(instance, aggname, buckets)
}

func makeKey(v value.Value) string {
	return v.String()
}
