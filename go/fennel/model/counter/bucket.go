package counter

import (
	"fmt"

	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/lib/utils/slice"
	"fennel/lib/value"
)

func Bucketize(bz Bucketizer, actions value.List) ([]counter.Bucket, []value.Value, error) {
	buckets := make([]counter.Bucket, 0, actions.Len())
	values := make([]value.Value, 0, actions.Len())
	for i := 0; i < actions.Len(); i++ {
		rowVal, _ := actions.At(i)
		row, ok := rowVal.(value.Dict)
		if !ok {
			return nil, nil, fmt.Errorf("action expected to be dict but found: '%v'", rowVal)
		}
		groupkey, ok := row.Get("groupkey")
		if !ok {
			return nil, nil, fmt.Errorf("action '%v' does not have a field called 'groupkey'", rowVal)
		}
		ts, ok := row.Get("timestamp")
		if !ok || value.Types.Int.Validate(ts) != nil {
			return nil, nil, fmt.Errorf("action '%v' does not have a field called 'timestamp' with datatype of 'int'", row)
		}
		v, ok := row.Get("value")
		if !ok {
			return nil, nil, fmt.Errorf("action '%v' does not have a field called 'value'", row)
		}
		ts_int := ts.(value.Int)
		b := bz.BucketizeMoment(groupkey.String(), ftypes.Timestamp(ts_int))
		buckets = append(buckets, b...)
		vals := make([]value.Value, len(b))
		slice.Fill(vals, v)
		values = append(values, vals...)
	}
	return buckets, values, nil
}

// MergeBuckets takes a list of buckets and "merges" their counts if rest of their properties
// are identical this reduces the number of keys to touch in storage
func MergeBuckets(mr MergeReduce, buckets []counter.Bucket, values []value.Value) ([]counter.Bucket, []value.Value, error) {
	seen := make(map[counter.Bucket]value.Value, 0)
	for i := range buckets {
		mapkey := buckets[i]
		current, ok := seen[mapkey]
		if !ok {
			current = mr.Zero()
		}
		v, err := mr.Transform(values[i])
		if err != nil {
			return nil, nil, err
		}
		seen[mapkey], err = mr.Merge(current, v)
		if err != nil {
			return nil, nil, err
		}
	}
	ret := make([]counter.Bucket, 0, len(seen))
	retVal := make([]value.Value, 0, len(seen))
	for b, c := range seen {
		ret = append(ret, b)
		retVal = append(retVal, c)
	}
	return ret, retVal, nil
}

// ===========================
// Private helpers below
// ===========================

// given start, end, returns indices of [startIdx, endIdx) periods that are fully enclosed within [start, end]
func boundary(start, end ftypes.Timestamp, period uint32) (uint32, uint32) {
	startBoundary := (uint32(start) + period - 1) / period
	endBoundary := uint32(end) / period
	return startBoundary, endBoundary
}

// trailingPartial returns the partial bucket that started within [start, end] but didn't fully finish before end
// if there is no such bucket, ok (2nd return value) is false
func trailingPartial(key string, start, end ftypes.Timestamp, window ftypes.Window, width uint32) (counter.Bucket, bool) {
	d, err := utils.Duration(window)
	if err != nil {
		return counter.Bucket{}, false
	}
	// this will always fit in uint32
	period := d * width
	startBoundary, endBoundary := boundary(start, end, period)
	if endBoundary < startBoundary {
		return counter.Bucket{}, false
	}
	if endBoundary*period == uint32(end) {
		// last bucket perfectly lines up with the end so there is no trailing partial bucket
		return counter.Bucket{}, false
	}
	return counter.Bucket{
		Key:    key,
		Window: window,
		Width:  width,
		Index:  endBoundary,
	}, true
}

type period struct {
	start ftypes.Timestamp
	end   ftypes.Timestamp
}

func start(end ftypes.Timestamp, duration uint32) ftypes.Timestamp {
	d := ftypes.Timestamp(duration)
	if end > d {
		return end - d
	}
	return 0
}

func extractDuration(kwargs value.Dict, durations []uint32) (uint32, error) {
	v, ok := kwargs.Get("duration")
	if !ok {
		return 0, fmt.Errorf("error: no duration specified")
	}
	duration, ok := v.(value.Int)
	if !ok {
		return 0, fmt.Errorf("error: expected kwarg 'duration' to be an int but found: '%v'", v)
	}
	// check duration is positive so it can be typecast to uint32 safely
	if duration >= 0 {
		for _, d := range durations {
			if uint32(duration) == d {
				return d, nil
			}
		}
	}
	return 0, fmt.Errorf("error: specified duration not found in aggregate")
}

func getMaxDuration(durations []uint32) uint32 {
	var maxDuration uint32 = 0
	for _, d := range durations {
		if d > maxDuration {
			maxDuration = d
		}
	}
	return maxDuration
}
