package temporal

import (
	"errors"
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
	durations  map[uint32]struct{}
}

func NewFixedWidthBucketizer(opts aggregate.Options, numbuckets uint32, clock clock.Clock) FixedWidthBucketizer {
	durations := make(map[uint32]struct{}, len(opts.Durations))
	for _, d := range opts.Durations {
		durations[d] = struct{}{}
	}
	return FixedWidthBucketizer{
		numbuckets,
		opts,
		clock,
		durations,
	}
}

var _ TimeBucketizer = FixedWidthBucketizer{}

func (fwb FixedWidthBucketizer) BucketizeMoment(ts uint32) ([]TimeBucket, []int64, error) {
	// TODO: Handle forever aggregates.
	if len(fwb.durations) == 0 {
		if fwb.opts.AggType != "timeseries_sum" {
			return nil, nil, errors.New("empty durations only supported for 'timeseries_sum' aggregate type")
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
		buckets := make([]TimeBucket, len(fwb.durations))
		ttls := make([]int64, len(fwb.durations))
		i := 0
		for d := range fwb.durations {
			width := d / fwb.numbuckets
			buckets[i].Width = width
			buckets[i].Index = ts / width
			ttls[i] = int64(ts + d + width)
			i++
		}
		return buckets, ttls, nil
	}
}

func (fwb FixedWidthBucketizer) BucketizeDuration(duration uint32) ([]TimeBucket, error) {
	if _, ok := fwb.durations[duration]; !ok {
		return nil, fmt.Errorf("incorrect duration value (%d) for aggregate of type (%s)", duration, fwb.opts.AggType)
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
