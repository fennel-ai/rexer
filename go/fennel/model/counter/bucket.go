package counter

import (
	"fennel/lib/ftypes"
	"fennel/lib/value"
)

type Bucket struct {
	Key    string
	Window ftypes.Window
	Index  uint64
	Count  value.Value
}

// BucketizeDuration bucketizes the [start, end] only using the given window types
func BucketizeDuration(key string, start ftypes.Timestamp, end ftypes.Timestamp, windows []ftypes.Window, zero value.Value) []Bucket {
	periods := []period{{start, end}}
	ret := make([]Bucket, 0)
	// iterate through in the right order - first day, then hour, then minute
	for _, w := range []ftypes.Window{ftypes.Window_DAY, ftypes.Window_HOUR, ftypes.Window_MINUTE} {
		if contains(windows, w) {
			nextPeriods := make([]period, 0)
			for _, p := range periods {
				buckets, bucketStart, bucketEnd := bucketizeTimeseries(key, p.start, p.end, w, zero)
				ret = append(ret, buckets...)
				nextPeriods = append(nextPeriods, period{p.start, bucketStart}, period{bucketEnd, p.end})
			}
			periods = nextPeriods
		}
	}
	return ret
}

func BucketizeMoment(key string, ts ftypes.Timestamp, count value.Value, windows []ftypes.Window) []Bucket {
	ret := make([]Bucket, len(windows))
	for i, w := range windows {
		switch w {
		case ftypes.Window_MINUTE:
			ret[i] = Bucket{Key: key, Window: ftypes.Window_MINUTE, Index: uint64(ts) / 60, Count: count}
		case ftypes.Window_HOUR:
			ret[i] = Bucket{Key: key, Window: ftypes.Window_HOUR, Index: uint64(ts) / 3600, Count: count}
		case ftypes.Window_DAY:
			ret[i] = Bucket{Key: key, Window: ftypes.Window_DAY, Index: uint64(ts) / (24 * 3600), Count: count}
		}
	}
	return ret
}

// MergeBuckets takes a list of buckets and "merges" their counts if rest of their properties
// are identical this reduces the number of keys to touch in storage
func MergeBuckets(histogram Histogram, buckets []Bucket) ([]Bucket, error) {
	seen := make(map[Bucket]value.Value, 0)
	var err error
	for i, _ := range buckets {
		mapkey := buckets[i]
		mapkey.Count = value.Nil // note, for hashmap to be hashable, this needs to be hashable as well
		current, ok := seen[mapkey]
		if !ok {
			current = histogram.Zero()
		}
		seen[mapkey], err = histogram.Merge(current, buckets[i].Count)
		if err != nil {
			return nil, err
		}
	}
	ret := make([]Bucket, 0, len(seen))
	for b, c := range seen {
		b.Count = c
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
func bucketizeTimeseries(key string, start, end ftypes.Timestamp, window ftypes.Window, zero value.Value) ([]Bucket, ftypes.Timestamp, ftypes.Timestamp) {
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
		ret[i-startBoundary] = Bucket{Key: key, Window: window, Index: i, Count: zero}
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
