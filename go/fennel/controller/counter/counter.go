package counter

import (
	"context"
	"fmt"

	"fennel/lib/aggregate"
	"fennel/lib/arena"
	libcounter "fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/lib/value"
	"fennel/model/counter"
	"fennel/tier"

	"github.com/samber/mo"
)

func Value(
	ctx context.Context, tier tier.Tier,
	aggId ftypes.AggId, aggOptions aggregate.Options, key value.Value, kwargs value.Dict,
) (value.Value, error) {
	vals, err := BatchValue(ctx, tier,
		[]ftypes.AggId{aggId}, []aggregate.Options{aggOptions}, []value.Value{key}, []value.Dict{kwargs})
	if err != nil {
		return value.Nil, err
	}
	return vals[0], nil
}

// TODO(Mohit): Fix this code if we decide to still use BucketStore
// BucketStore instances are created per histogram - the list `indices` created is always a single element list
func BatchValue(
	ctx context.Context, tier tier.Tier,
	aggIds []ftypes.AggId, aggOptions []aggregate.Options, keys []value.Value, kwargs []value.Dict,
) ([]value.Value, error) {
	histograms := make([]counter.Histogram, len(aggIds))
	end := ftypes.Timestamp(tier.Clock.Now())
	unique := make(map[counter.BucketStore][]int)
	ret := make([]value.Value, len(aggIds))
	for i, aggId := range aggIds {
		h, err := counter.ToHistogram(aggId, aggOptions[i])
		if err != nil {
			return nil, fmt.Errorf("failed to make histogram from aggregate at index %d of batch: %v", i, err)
		}
		histograms[i] = h
		bs := h.GetBucketStore()
		unique[bs] = append(unique[bs], i)
	}
	for bs, indices := range unique {
		n := len(indices)
		ids_ := make([]ftypes.AggId, n)
		bucketLists := make([][]libcounter.BucketList, n)
		defaults := make([]value.Value, n)
		for i, index := range indices {
			h := histograms[index]
			duration, err := getRequestDuration(h.Options(), kwargs[index])
			if err != nil {
				return nil, fmt.Errorf("failed to get duration of aggregate (id): %d, err: %w", aggIds[index], err)
			}
			start, err := counter.Start(h, end, duration)
			if err != nil {
				return nil, fmt.Errorf("failed to get start timestamp of aggregate (id): %d, err: %w", aggIds[index], err)
			}
			ids_[i] = aggIds[index]
			bucketLists[i] = h.BucketizeDuration(keys[index].String(), start, end)
			defaults[i] = h.Zero()
		}
		counts, err := bs.GetMulti(ctx, tier, ids_, bucketLists, defaults)
		if err != nil {
			return nil, err
		}
		for cur, index := range indices {
			ret[index], err = histograms[index].Reduce(counts[cur])
			if err != nil {
				return nil, fmt.Errorf("failed to reduce aggregate (id): %d, err: %v", aggIds[index], err)
			}
		}
		// Explicitly free the counter slices back to the arena.
		for _, v := range counts {
			arena.Values.Free(v)
		}
	}
	return ret, nil
}

func Update(
	ctx context.Context, tier tier.Tier, aggId ftypes.AggId, aggOptions aggregate.Options,
	table value.List) error {
	ctx, tmr := timer.Start(ctx, tier.ID, "counter.update")
	defer tmr.Stop()
	histogram, err := counter.ToHistogram(aggId, aggOptions)
	if err != nil {
		return fmt.Errorf("failed to make histogram from aggregate: %w", err)
	}
	buckets, values, err := counter.Bucketize(histogram, table)
	if err != nil {
		return err
	}
	// Merge buckets before reads to reduce number of keys fetched.
	buckets, values, err = counter.MergeBuckets(histogram, buckets, values)
	if err != nil {
		return err
	}
	bucketLists := bucketsToBucketLists(buckets)
	cur, err := histogram.GetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]libcounter.BucketList{bucketLists}, []value.Value{histogram.Zero()})
	if err != nil {
		return err
	}
	for _, c := range cur {
		defer arena.Values.Free(c)
		for i := range c {
			values[i], err = histogram.Merge(c[i], values[i])
			if err != nil {
				return err
			}
		}
	}
	return histogram.SetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]libcounter.Bucket{buckets}, [][]value.Value{values})
}

func getRequestDuration(options aggregate.Options, kwargs value.Dict) (mo.Option[uint32], error) {
	if options.AggType == aggregate.TIMESERIES_SUM {
		return mo.None[uint32](), nil
	}
	d, err := extractDuration(kwargs, options.Durations)
	if err != nil {
		return mo.None[uint32](), err
	}
	return mo.Some(d), nil
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
	if duration < 0 {
		return 0, fmt.Errorf("error: specified duration (%d) < 0", duration)
	}
	for _, d := range durations {
		if uint32(duration) == d {
			return d, nil
		}
	}
	return 0, fmt.Errorf("error: specified duration not found in aggregate")
}

func bucketsToBucketLists(buckets []libcounter.Bucket) []libcounter.BucketList {
	bLists := make([]libcounter.BucketList, len(buckets))
	for i, b := range buckets {
		bLists[i] = libcounter.BucketList{
			Key:        b.Key,
			Window:     b.Window,
			Width:      b.Width,
			StartIndex: b.Index,
			EndIndex:   b.Index,
		}
	}
	return bLists
}
