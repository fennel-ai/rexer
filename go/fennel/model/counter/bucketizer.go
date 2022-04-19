package counter

import (
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/value"
)

type fixedWidthBucketizer struct {
	sizes                  map[ftypes.Window]uint64
	includeTrailingPartial bool
}

func (f fixedWidthBucketizer) BucketizeMoment(key string, ts ftypes.Timestamp, v value.Value) []counter.Bucket {
	ret := make([]counter.Bucket, 0, len(f.sizes))
	for window, width := range f.sizes {
		if width == 0 {
			continue
		}
		switch window {
		case ftypes.Window_MINUTE:
			ret = append(ret, counter.Bucket{Key: key, Window: ftypes.Window_MINUTE, Index: uint64(ts) / 60 / width, Width: width, Value: v})
		case ftypes.Window_HOUR:
			ret = append(ret, counter.Bucket{Key: key, Window: ftypes.Window_HOUR, Index: uint64(ts) / 3600 / width, Width: width, Value: v})
		case ftypes.Window_DAY:
			ret = append(ret, counter.Bucket{Key: key, Window: ftypes.Window_DAY, Index: uint64(ts) / (24 * 3600) / width, Width: width, Value: v})
		}
	}
	return ret
}

func (f fixedWidthBucketizer) BucketizeDuration(key string, start, end ftypes.Timestamp, v value.Value) []counter.Bucket {
	periods := []period{{start, end}}
	ret := make([]counter.Bucket, 0)
	// iterate through in the right order - first day, then hour, then minute
	for _, w := range []ftypes.Window{ftypes.Window_DAY, ftypes.Window_HOUR, ftypes.Window_MINUTE} {
		width, ok := f.sizes[w]
		if !ok || width == 0 {
			continue
		}
		nextPeriods := make([]period, 0)
		for _, p := range periods {
			buckets, bucketStart, bucketEnd := bucketizeTimeseries(key, p.start, p.end, w, width, v)
			ret = append(ret, buckets...)
			nextPeriods = append(nextPeriods, period{p.start, bucketStart}, period{bucketEnd, p.end})
		}
		periods = nextPeriods
	}
	if f.includeTrailingPartial {
		// append a window of the smallest possible size (which is why we iterate in the order of minute -> hour -> day)
		for _, w := range []ftypes.Window{ftypes.Window_MINUTE, ftypes.Window_HOUR, ftypes.Window_DAY} {
			width, ok := f.sizes[w]
			if !ok || width == 0 {
				continue
			}
			if partial, ok := trailingPartial(key, start, end, w, width, v); ok {
				ret = append(ret, partial)
			}
			// we break because we only want to find the trailing partial of the smallest size
			break
		}
	}
	return ret
}

var _ Bucketizer = fixedWidthBucketizer{}
