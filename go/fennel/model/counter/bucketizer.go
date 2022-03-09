package counter

import (
	"fennel/lib/ftypes"
	"fennel/lib/value"
)

type FixedWidthBucketizer struct {
	windows []ftypes.Window
}

func (f FixedWidthBucketizer) BucketizeMoment(key string, ts ftypes.Timestamp, v value.Value) []Bucket {
	ret := make([]Bucket, len(f.windows))
	for i, w := range f.windows {
		switch w {
		case ftypes.Window_MINUTE:
			ret[i] = Bucket{Key: key, Window: ftypes.Window_MINUTE, Index: uint64(ts) / 60, Count: v}
		case ftypes.Window_HOUR:
			ret[i] = Bucket{Key: key, Window: ftypes.Window_HOUR, Index: uint64(ts) / 3600, Count: v}
		case ftypes.Window_DAY:
			ret[i] = Bucket{Key: key, Window: ftypes.Window_DAY, Index: uint64(ts) / (24 * 3600), Count: v}
		}
	}
	return ret
}

func (f FixedWidthBucketizer) BucketizeDuration(key string, start, end ftypes.Timestamp, v value.Value) []Bucket {
	periods := []period{{start, end}}
	ret := make([]Bucket, 0)
	// iterate through in the right order - first day, then hour, then minute
	for _, w := range []ftypes.Window{ftypes.Window_DAY, ftypes.Window_HOUR, ftypes.Window_MINUTE} {
		if contains(f.windows, w) {
			nextPeriods := make([]period, 0)
			for _, p := range periods {
				buckets, bucketStart, bucketEnd := bucketizeTimeseries(key, p.start, p.end, w, v)
				ret = append(ret, buckets...)
				nextPeriods = append(nextPeriods, period{p.start, bucketStart}, period{bucketEnd, p.end})
			}
			periods = nextPeriods
		}
	}
	return ret
}

var _ Bucketizer = FixedWidthBucketizer{}
