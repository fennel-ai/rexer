package counter

import (
	"fmt"

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

// fixedSplitBucketizer splits each duration into numBuckets buckets
// when duration is 0, it provides an infinite bucket instead
// unlike fixedWidthBucketizer, where each duration is split into a variable number of constant sized buckets
// fixedSplitBucketizer splits each duration equally into a constant number of buckets
type fixedSplitBucketizer struct {
	numBuckets []uint64
	durations  []uint64
	widths     []uint64
}

// numBuckets must always be > 0;
// set duration = 0 for infinite bucket
// 0 width is interpreted as infinite bucket
func newFixedSplitBucketizer(numBuckets []uint64, durations []uint64) (f fixedSplitBucketizer, err error) {
	if len(numBuckets) != len(durations) {
		return f, fmt.Errorf("error: length of arrays 'numBuckets' and 'durations' not equal")
	}
	f = fixedSplitBucketizer{
		numBuckets: numBuckets,
		durations:  durations,
	}
	f.widths = make([]uint64, len(durations))
	for i := range numBuckets {
		if numBuckets[i] == 0 {
			return f, fmt.Errorf("error: numBuckets[%d] is 0; numBuckets must be a postive integer", i)
		}
		if durations[i] == 0 {
			// if duration = 0, bucket should be of infinite width so 0
			f.widths[i] = 0
		} else {
			if durations[i] < numBuckets[i] {
				// if duration is less than number of buckets, reduce number of buckets to match duration otherwise,
				// width will be zero, which is interpreted as an infinite bucket and is not correct in any case
				numBuckets[i] = durations[i]
			}
			f.widths[i] = durations[i] / numBuckets[i]
		}
	}
	return f, nil
}

func (f fixedSplitBucketizer) BucketizeMoment(key string, ts ftypes.Timestamp, v value.Value) []counter.Bucket {
	buckets := make([]counter.Bucket, len(f.durations))
	for i := range f.durations {
		buckets[i] = counter.Bucket{
			Key:    key,
			Window: ftypes.Window_FOREVER,
			Width:  f.widths[i],
			Index:  f.getIndex(ts, f.widths[i]),
			Value:  v,
		}
	}
	return buckets
}

func (f fixedSplitBucketizer) BucketizeDuration(
	key string, start, finish ftypes.Timestamp, v value.Value,
) []counter.Bucket {
	// Find which width, duration of the Bucketizer we are working with
	d := finish - start
	var w uint64
	for i := range f.durations {
		if f.durations[i] == uint64(d) {
			w = f.widths[i]
		}
	}
	// Get buckets
	beg := f.getIndex(start, w)
	end := f.getIndex(finish-1, w)
	buckets := make([]counter.Bucket, end-beg+1)
	for i := beg; i <= end; i++ {
		buckets[i-beg] = counter.Bucket{
			Key:    key,
			Window: ftypes.Window_FOREVER,
			Width:  w,
			Index:  i,
			Value:  v,
		}
	}
	return buckets
}

func (f fixedSplitBucketizer) getIndex(ts ftypes.Timestamp, w uint64) uint64 {
	// If w is 0, width is infinite so we return 0
	if w == 0 {
		return 0
	}
	return uint64(ts) / w
}

// thirdBucketizer splits the entire time space into buckets of size each.
// Bucket 0, covers the time range [0, size) and so on.
// When size = 0, there is one infinite bucket which covers the entire time space.
type thirdBucketizer struct {
	size uint64
}

func (t thirdBucketizer) BucketizeMoment(key string, ts ftypes.Timestamp, v value.Value) []counter.Bucket {
	return []counter.Bucket{
		{
			Key:    key,
			Window: ftypes.Window_FOREVER,
			Width:  t.size,
			Index:  t.getIndex(ts),
			Value:  v,
		},
	}
}

func (t thirdBucketizer) BucketizeDuration(
	key string, start, finish ftypes.Timestamp, v value.Value,
) []counter.Bucket {
	beg := t.getIndex(start)
	end := t.getIndex(finish - 1)
	buckets := make([]counter.Bucket, end-beg+1)
	for i := beg; i <= end; i++ {
		buckets[i-beg] = counter.Bucket{
			Key:    key,
			Window: ftypes.Window_FOREVER,
			Width:  t.size,
			Index:  i,
			Value:  v,
		}
	}
	return buckets
}

func (t thirdBucketizer) getIndex(ts ftypes.Timestamp) uint64 {
	// If size is 0, width is infinite so we return 0
	if t.size == 0 {
		return 0
	}
	return uint64(ts) / t.size
}

var _ Bucketizer = fixedWidthBucketizer{}
var _ Bucketizer = fixedSplitBucketizer{}
var _ Bucketizer = thirdBucketizer{}
