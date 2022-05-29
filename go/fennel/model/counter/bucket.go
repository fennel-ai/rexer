package counter

import (
	"fmt"

	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/lib/value"
)

func Bucketize(histogram Histogram, actions value.List) ([]counter.Bucket, error) {
	buckets := make([]counter.Bucket, 0, actions.Len())
	for i := 0; i < actions.Len(); i++ {
		rowVal, _ := actions.At(i)
		row, ok := rowVal.(*value.Dict)
		if !ok {
			return nil, fmt.Errorf("action expected to be dict but found: '%v'", rowVal)
		}
		groupkey, ok := row.Get("groupkey")
		if !ok {
			return nil, fmt.Errorf("action '%v' does not have a field called 'groupkey'", rowVal)
		}
		ts, ok := row.Get("timestamp")
		if !ok || value.Types.Int.Validate(ts) != nil {
			return nil, fmt.Errorf("action '%v' does not have a field called 'timestamp' with datatype of 'int'", row)
		}
		v, ok := row.Get("value")
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

// MergeBuckets takes a list of buckets and "merges" their counts if rest of their properties
// are identical this reduces the number of keys to touch in storage
func MergeBuckets(histogram Histogram, buckets []counter.Bucket) ([]counter.Bucket, error) {
	seen := make(map[counter.Bucket]value.Value, 0)
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
	ret := make([]counter.Bucket, 0, len(seen))
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
func bucketizeTimeseries(key string, start, end ftypes.Timestamp, window ftypes.Window, width uint64, zero value.Value) ([]counter.Bucket, ftypes.Timestamp, ftypes.Timestamp) {
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
		return []counter.Bucket{}, start, start
	}
	bucketStart := ftypes.Timestamp(startBoundary * period)
	bucketEnd := ftypes.Timestamp(endBoundary * period)
	ret := make([]counter.Bucket, endBoundary-startBoundary)
	for i := startBoundary; i < endBoundary; i++ {
		ret[i-startBoundary] = counter.Bucket{Key: key, Window: window, Index: i, Width: width, Value: zero}
	}
	return ret, bucketStart, bucketEnd
}

// trailingPartial returns the partial bucket that started within [start, end] but didn't fully finish before end
// if there is no such bucket, ok (2nd return value) is false
func trailingPartial(key string, start, end ftypes.Timestamp, window ftypes.Window, width uint64, zero value.Value) (counter.Bucket, bool) {
	d, err := utils.Duration(window)
	if err != nil {
		return counter.Bucket{}, false
	}
	period := d * width
	startBoundary, endBoundary := boundary(start, end, period)
	if endBoundary < startBoundary {
		return counter.Bucket{}, false
	}
	if endBoundary*period == uint64(end) {
		// last bucket perfectly lines up with the end so there is no trailing partial bucket
		return counter.Bucket{}, false
	}
	return counter.Bucket{
		Key:    key,
		Window: window,
		Width:  width,
		Index:  endBoundary,
		Value:  zero,
	}, true
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

func extractDuration(kwargs *value.Dict, durations []uint64) (uint64, error) {
	v, ok := kwargs.Get("duration")
	if !ok {
		return 0, fmt.Errorf("error: no duration specified")
	}
	duration, ok := v.(value.Int)
	if !ok {
		return 0, fmt.Errorf("error: expected kwarg 'duration' to be an int but found: '%v'", v)
	}
	// check duration is positive so it can be typecast to uint64 safely
	if duration >= 0 {
		for _, d := range durations {
			if uint64(duration) == d {
				return d, nil
			}
		}
	}
	return 0, fmt.Errorf("error: specified duration not found in aggregate")
}

func getMaxDuration(durations []uint64) uint64 {
	var maxDuration uint64 = 0
	for _, d := range durations {
		if d > maxDuration {
			maxDuration = d
		}
	}
	return maxDuration
}
