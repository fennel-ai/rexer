package temporal

import (
	"fmt"

	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/model/counter"

	"github.com/raulk/clock"
	"github.com/samber/mo"
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
	Bucketize(mr counter.MergeReduce, duration mo.Option[uint32]) (buckets []TimeBucket, err error)
	BucketizeMoment(mr counter.MergeReduce, ts uint32) (buckets []TimeBucket, ttls []int64, err error)
	NumBucketsHint() int
}

// FixedWidthBucketizer bucketizes timestamps and durations into fixed-width
// buckets determined by durations used by the given aggregate and numbuckets.
// For each duration used by the aggregate, a fixed-width bucket is created.
type FixedWidthBucketizer struct {
	numbuckets uint32
	clock      clock.Clock
}

func NewFixedWidthBucketizer(numbuckets uint32, clock clock.Clock) FixedWidthBucketizer {
	return FixedWidthBucketizer{
		numbuckets,
		clock,
	}
}

var _ TimeBucketizer = FixedWidthBucketizer{}

func (fwb FixedWidthBucketizer) NumBucketsHint() int {
	return int(fwb.numbuckets)
}

func (fwb FixedWidthBucketizer) BucketizeMoment(mr counter.MergeReduce, ts uint32) ([]TimeBucket, []int64, error) {
	opts := mr.Options()
	// TODO: Handle forever aggregates.
	if len(opts.Durations) == 0 {
		if opts.AggType != aggregate.TIMESERIES_SUM {
			return nil, nil, fmt.Errorf("empty durations only supported for '%v' aggregate type", aggregate.TIMESERIES_SUM)
		}
		d, err := utils.Duration(opts.Window)
		if err != nil || d == 0 {
			return nil, nil, fmt.Errorf("error parsing window duration (%s): %w", opts.Window.String(), err)
		}
		buckets := []TimeBucket{
			{Width: d, Index: ts / d},
		}
		ttls := []int64{int64(d * opts.Limit)}
		return buckets, ttls, nil
	} else {
		buckets := make([]TimeBucket, len(opts.Durations))
		ttls := make([]int64, len(opts.Durations))
		i := 0
		for _, d := range opts.Durations {
			width := d / fwb.numbuckets
			buckets[i].Width = width
			buckets[i].Index = ts / width
			ttls[i] = int64(ts + d + width)
			i++
		}
		return buckets, ttls, nil
	}
}

func (fwb FixedWidthBucketizer) Bucketize(mr counter.MergeReduce, duration mo.Option[uint32]) ([]TimeBucket, error) {
	end := ftypes.Timestamp(fwb.clock.Now().Unix())
	start, err := counter.Start(mr, ftypes.Timestamp(end), duration)
	if err != nil {
		return nil, err
	}
	width := uint32(end-start) / fwb.numbuckets
	first := uint32(start) / width
	last := uint32(end) / width
	buckets := make([]TimeBucket, last-first+1)
	for i := first; i <= last; i++ {
		buckets[i-first].Width = width
		buckets[i-first].Index = i
	}
	return buckets, nil
}
