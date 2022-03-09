package counter

import (
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
)

func Bucketize(histogram Histogram, actions value.List) ([]Bucket, error) {
	buckets := make([]Bucket, 0, len(actions))
	for _, rowVal := range actions {
		row, ok := rowVal.(value.Dict)
		if !ok {
			return nil, fmt.Errorf("action expected to be dict but found: '%v'", rowVal)
		}
		groupkey, ok := row["groupkey"]
		if !ok {
			return nil, fmt.Errorf("action '%v' does not have a field called 'groupkey'", rowVal)
		}
		ts, ok := row["timestamp"]
		if !ok || value.Types.Int.Validate(ts) != nil {
			return nil, fmt.Errorf("action '%v' does not have a field called 'timestamp' with datatype of 'int'", row)
		}
		v, ok := row["value"]
		if !ok {
			return nil, fmt.Errorf("action '%v' does not have a field called 'value'", row)
		}
		ts_int := ts.(value.Int)
		key := groupkey.String()
		v, err := histogram.Transform(v)
		if err != nil {
			return nil, err
		}
		b := histogram.BucketizeMoment(key, ftypes.Timestamp(ts_int), v)
		buckets = append(buckets, b...)
	}
	return buckets, nil
}

// BucketizeDuration bucketizes the [start, end] only using the given window types
func BucketizeDuration(key string, start ftypes.Timestamp, end ftypes.Timestamp, windows []ftypes.Window, zero value.Value) []Bucket {
	sizes := make(map[ftypes.Window]uint64)
	for _, w := range windows {
		sizes[w] = 1
	}
	return fixedWidthBucketizer{sizes}.BucketizeDuration(key, start, end, zero)
}

func BucketizeMoment(key string, ts ftypes.Timestamp, count value.Value, windows []ftypes.Window) []Bucket {
	sizes := make(map[ftypes.Window]uint64)
	for _, w := range windows {
		sizes[w] = 1
	}
	return fixedWidthBucketizer{sizes}.BucketizeMoment(key, ts, count)
}

// MergeBuckets takes a list of buckets and "merges" their counts if rest of their properties
// are identical this reduces the number of keys to touch in storage
func MergeBuckets(histogram Histogram, buckets []Bucket) ([]Bucket, error) {
	seen := make(map[Bucket]value.Value, 0)
	var err error
	for i := range buckets {
		mapkey := buckets[i]
		mapkey.Value = value.Nil // note, for hashmap to be hashable, this needs to be hashable as well
		current, ok := seen[mapkey]
		if !ok {
			current = histogram.Zero()
		}
		seen[mapkey], err = histogram.Merge(current, buckets[i].Value)
		if err != nil {
			return nil, err
		}
	}
	ret := make([]Bucket, 0, len(seen))
	for b, c := range seen {
		b.Value = c
		ret = append(ret, b)
	}
	return ret, nil
}

//===========================
// Private helpers below
//===========================

// given start, end, returns indices of [startIdx, endIdx) periods that are fully enclosed within [start, end]
func boundary(start, end ftypes.Timestamp, period uint64) (uint64, uint64) {
	startBoundary := (uint64(start) + period - 1) / period
	endBoundary := uint64(end) / period
	return startBoundary, endBoundary
}

// bucketizeTimeseries returns a list of buckets of size 'Window' that begin at or after 'start'
// and go until at or before 'end'. Each bucket's count is left at 0
func bucketizeTimeseries(key string, start, end ftypes.Timestamp, window ftypes.Window, width uint64, zero value.Value) ([]Bucket, ftypes.Timestamp, ftypes.Timestamp) {
	var period uint64
	switch window {
	case ftypes.Window_MINUTE:
		period = 60 * width
	case ftypes.Window_HOUR:
		period = 3600 * width
	case ftypes.Window_DAY:
		period = 24 * 3600 * width
	default:
		panic("this should never happen")
	}
	startBoundary, endBoundary := boundary(start, end, period)
	if endBoundary <= startBoundary {
		return []Bucket{}, start, start
	}
	bucketStart := ftypes.Timestamp(startBoundary * period)
	bucketEnd := ftypes.Timestamp(endBoundary * period)
	ret := make([]Bucket, endBoundary-startBoundary)
	for i := startBoundary; i < endBoundary; i++ {
		ret[i-startBoundary] = Bucket{Key: key, Window: window, Index: i, Width: width, Value: zero}
	}
	return ret, bucketStart, bucketEnd
}

func contains(windows []ftypes.Window, window ftypes.Window) bool {
	for _, w := range windows {
		if w == window {
			return true
		}
	}
	return false
}

type period struct {
	start ftypes.Timestamp
	end   ftypes.Timestamp
}

func start(end ftypes.Timestamp, duration uint64) ftypes.Timestamp {
	d := ftypes.Timestamp(duration)
	if end > d {
		return end - d
	}
	return 0
}
