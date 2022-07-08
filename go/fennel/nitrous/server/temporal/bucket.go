package temporal

import (
	"fmt"

	"fennel/lib/aggregate"
	"fennel/lib/utils"

	"github.com/raulk/clock"
)

// TimeBucket represents the time interval [Index*Width, (Index+1)*Width)
// seconds since t=0.
type TimeBucket struct {
	// Width of the bucket in seconds.
	Width uint32
	// Index of the bucket.
	Index uint32
}

// TimeBucketizer is an interface for placing timestamps and durations into time
// buckets.
type TimeBucketizer interface {
	BucketizeDuration(duration uint32) (buckets []TimeBucket, err error)
	BucketizeMoment(ts uint32) (buckets []TimeBucket, ttls []int64, err error)
}

// FixedWidthBucketizer bucketizes timestamps and durations into fixed-width
// buckets determined by durations used by the given aggregate and numbuckets.
// For each duration used by the aggregate, a fixed-width bucket is created.
type FixedWidthBucketizer struct {
	numbuckets uint32
	opts       aggregate.Options
	clock      clock.Clock
}

func NewFixedWidthBucketizer(opts aggregate.Options, numbuckets uint32, clock clock.Clock) FixedWidthBucketizer {
	return FixedWidthBucketizer{
		numbuckets,
		opts,
		clock,
	}
}

var _ TimeBucketizer = FixedWidthBucketizer{}

func (fwb FixedWidthBucketizer) BucketizeMoment(ts uint32) ([]TimeBucket, []int64, error) {
	// TODO: Handle forever aggregates.
	if len(fwb.opts.Durations) == 0 {
		if fwb.opts.AggType != aggregate.TIMESERIES_SUM {
			return nil, nil, fmt.Errorf("empty durations only supported for '%v' aggregate type", aggregate.TIMESERIES_SUM)
		}
		d, err := utils.Duration(fwb.opts.Window)
		if err != nil || d == 0 {
			return nil, nil, fmt.Errorf("error parsing window duration (%s): %w", fwb.opts.Window.String(), err)
		}
		buckets := []TimeBucket{
			{Width: d, Index: ts / d},
		}
		ttls := []int64{int64(d * fwb.opts.Limit)}
		return buckets, ttls, nil
	} else {
		buckets := make([]TimeBucket, len(fwb.opts.Durations))
		ttls := make([]int64, len(fwb.opts.Durations))
		i := 0
		for _, d := range fwb.opts.Durations {
			width := d / fwb.numbuckets
			buckets[i].Width = width
			buckets[i].Index = ts / width
			ttls[i] = int64(ts + d + width)
			i++
		}
		return buckets, ttls, nil
	}
}

func (fwb FixedWidthBucketizer) isValid(duration uint32) bool {
	valid := false
	// Note: We do a scan over fwb.opts.Durations to see if duration is in there.
	// For small slices, this is faster than using a map.
	for _, d := range fwb.opts.Durations {
		if d == duration {
			valid = true
			break
		}
	}
	return valid
}

func (fwb FixedWidthBucketizer) BucketizeDuration(duration uint32) ([]TimeBucket, error) {
	if !fwb.isValid(duration) {
		return nil, fmt.Errorf("incorrect duration value (%d) for aggregate of type (%s). Allowed values: %v", duration, fwb.opts.AggType, fwb.opts.Durations)
	}
	width := duration / fwb.numbuckets
	end := uint32(fwb.clock.Now().Unix())
	start := end - duration
	first := start / width
	last := end / width
	buckets := make([]TimeBucket, last-first+1)
	for i := first; i <= last; i++ {
		buckets[i-first].Width = width
		buckets[i-first].Index = i
	}
	return buckets, nil
}
