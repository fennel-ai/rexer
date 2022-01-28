package aggregate

import (
	"fennel/instance"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/model/rcounter"
	"fmt"
	"strings"
)

func rollingValue(instance instance.Instance, agg aggregate.Aggregate, key string) (value.Int, error) {
	end := ftypes.Timestamp(instance.Clock.Now())
	start := end - ftypes.Timestamp(agg.Options.Duration)
	buckets := rcounter.BucketizeDuration(key, start, end)
	counts, err := rcounter.GetMulti(instance, agg.Name, buckets)
	if err != nil {
		return value.Int(0), err
	}
	var total int64
	for i, _ := range counts {
		total += counts[i]
	}
	return value.Int(total), nil
}

func timeseriesValue(instance instance.Instance, agg aggregate.Aggregate, key string) (value.List, error) {
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
	buckets, err := rcounter.BucketizeTimeseries(key, start, end, agg.Options.Window)
	if err != nil {
		return value.List{}, err
	}
	counts, err := rcounter.GetMulti(instance, agg.Name, buckets)
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

func counterUpdate(instance instance.Instance, aggname ftypes.AggName, table value.Table) error {
	schema := table.Schema()
	type_, ok := schema["key"]
	if !ok || type_ != value.Types.List {
		return fmt.Errorf("query does not create column called 'key' with datatype of 'list'")
	}
	type_, ok = schema["timestamp"]
	if !ok || type_ != value.Types.Int {
		return fmt.Errorf("query does not create column called 'timestamp' with datatype of 'int'")
	}
	buckets := make([]rcounter.Bucket, 0, table.Len())
	for _, row := range table.Pull() {
		ts := row["timestamp"].(value.Int)
		key, err := makeKey(row["key"])
		if err != nil {
			return err
		}
		buckets = append(buckets, rcounter.BucketizeMoment(key, ftypes.Timestamp(ts), 1)...)
	}
	buckets = rcounter.MergeBuckets(buckets)
	return rcounter.IncrementMulti(instance, aggname, buckets)
}

func makeKey(oids value.Value) (string, error) {
	aslist, ok := oids.(value.List)
	if !ok {
		return "", fmt.Errorf("key column does not contain list")
	}
	var sb strings.Builder
	for _, v := range aslist {
		asint, ok := v.(value.Int)
		if !ok {
			return "", fmt.Errorf("key column should contain list of ints, but instead found: '%v'", oids)
		}
		sb.WriteString(fmt.Sprintf("%d", int64(asint)))
	}
	return sb.String(), nil
}
