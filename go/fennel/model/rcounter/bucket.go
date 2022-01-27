package rcounter

import (
	"fennel/lib/ftypes"
	"fmt"
)

// global version of counter namespace - increment to invalidate all data stored in redis
const version = 1

type Bucket struct {
	Key    string
	Window ftypes.Window
	Index  uint64
	Count  int64
}

// BucketizeDuration bucketizes the [start, end] using minute/hour/day windows
func BucketizeDuration(key string, start ftypes.Timestamp, end ftypes.Timestamp) []Bucket {
	days, dayStart, dayEnd := bucketizeTimeseries(key, start, end, ftypes.Window_DAY)
	hours1, hourStart, _ := bucketizeTimeseries(key, start, dayStart, ftypes.Window_HOUR)
	hours2, _, hoursEnd := bucketizeTimeseries(key, dayEnd, end, ftypes.Window_HOUR)

	mins1, _, _ := bucketizeTimeseries(key, start, hourStart, ftypes.Window_MINUTE)
	mins2, _, _ := bucketizeTimeseries(key, hoursEnd, end, ftypes.Window_MINUTE)
	ret := append(mins1, hours1...)
	ret = append(ret, days...)
	ret = append(ret, hours2...)
	ret = append(ret, mins2...)
	return ret
}

func BucketizeTimeseries(key string, start, end ftypes.Timestamp, window ftypes.Window) ([]Bucket, error) {
	if window != ftypes.Window_HOUR && window != ftypes.Window_DAY {
		return nil, fmt.Errorf("unsupported window type - only hours & days are supported")
	}
	ret, _, _ := bucketizeTimeseries(key, start, end, window)
	return ret, nil
}

func BucketizeMoment(key string, ts ftypes.Timestamp, count int64) []Bucket {
	return []Bucket{
		{Key: key, Window: ftypes.Window_MINUTE, Index: uint64(ts) / 60, Count: count},
		{Key: key, Window: ftypes.Window_HOUR, Index: uint64(ts) / 3600, Count: count},
		{Key: key, Window: ftypes.Window_DAY, Index: uint64(ts) / (24 * 3600), Count: count},
	}
}

// MergeBuckets takes a list of buckets and "merges" their counts if rest of their properties
// are identical this reduces the number of keys to touch in storage
func MergeBuckets(buckets []Bucket) []Bucket {
	seen := make(map[Bucket]int64, 0)
	for i, _ := range buckets {
		mapkey := buckets[i]
		mapkey.Count = 0
		current, ok := seen[mapkey]
		if !ok {
			current = 0
		}
		seen[mapkey] = current + buckets[i].Count
	}
	ret := make([]Bucket, 0, len(seen))
	for b, c := range seen {
		b.Count = c
		ret = append(ret, b)
	}
	return ret
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

// bucketizeTimeseries returns a list of buckets of size 'window' that begin at or after 'start'
// and go until at or before 'end'. Each bucket's count is left at 0
func bucketizeTimeseries(key string, start, end ftypes.Timestamp, window ftypes.Window) ([]Bucket, ftypes.Timestamp, ftypes.Timestamp) {
	var period uint64
	switch window {
	case ftypes.Window_MINUTE:
		period = 60
	case ftypes.Window_HOUR:
		period = 3600
	case ftypes.Window_DAY:
		period = 24 * 3600
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
		ret[i-startBoundary] = Bucket{Key: key, Window: window, Index: i, Count: 0}
	}
	return ret, bucketStart, bucketEnd
}
