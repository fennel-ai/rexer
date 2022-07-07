package counter

import (
	"context"
	"fennel/lib/arena"
	"fennel/lib/timer"
	"fmt"

	libcounter "fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/model/counter"
	"fennel/tier"
)

func Value(
	ctx context.Context, tier tier.Tier,
	aggId ftypes.AggId, key value.Value, histogram counter.Histogram, kwargs value.Dict,
) (value.Value, error) {
	vals, err := BatchValue(ctx, tier,
		[]ftypes.AggId{aggId}, []value.Value{key}, []counter.Histogram{histogram}, []value.Dict{kwargs})
	if err != nil {
		return value.Nil, err
	}
	return vals[0], nil
}

// TODO(Mohit): Fix this code if we decide to still use BucketStore
// BucketStore instances are created per histogram - the list `indices` created is always a single element list
func BatchValue(
	ctx context.Context, tier tier.Tier,
	aggIds []ftypes.AggId, keys []value.Value, histograms []counter.Histogram, kwargs []value.Dict,
) ([]value.Value, error) {
	end := ftypes.Timestamp(tier.Clock.Now())
	unique := make(map[counter.BucketStore][]int)
	ret := make([]value.Value, len(aggIds))
	for i, h := range histograms {
		bs := h.GetBucketStore()
		unique[bs] = append(unique[bs], i)
	}
	for bs, indices := range unique {
		n := len(indices)
		ids_ := make([]ftypes.AggId, n)
		buckets := make([][]libcounter.Bucket, n)
		defaults := make([]value.Value, n)
		for i, index := range indices {
			h := histograms[index]
			start, err := counter.Start(h, end, kwargs[index])
			if err != nil {
				return nil, fmt.Errorf("failed to get start timestamp of aggregate (id): %d, err: %v", aggIds[index], err)
			}
			ids_[i] = aggIds[index]
			buckets[i] = h.BucketizeDuration(keys[index].String(), start, end)
			defer arena.Buckets.Free(buckets[i])
			defaults[i] = h.Zero()
		}
		counts, err := bs.GetMulti(ctx, tier, ids_, buckets, defaults)
		if err != nil {
			return nil, err
		}
		// Explicitly free the counter slices back to the arena.
		for _, v := range counts {
			defer arena.Values.Free(v)
		}
		for cur, index := range indices {
			ret[index], err = histograms[index].Reduce(counts[cur])
			if err != nil {
				return nil, fmt.Errorf("failed to reduce aggregate (id): %d, err: %v", aggIds[index], err)
			}
		}
	}
	return ret, nil
}

func Update(
	ctx context.Context, tier tier.Tier, aggId ftypes.AggId, table value.List, histogram counter.Histogram,
) error {
	ctx, tmr := timer.Start(ctx, tier.ID, "counter.update")
	defer tmr.Stop()
	buckets, values, err := counter.Bucketize(histogram, table)
	if err != nil {
		return err
	}
	// Merge buckets before reads to reduce number of keys fetched.
	buckets, values, err = counter.MergeBuckets(histogram, buckets, values)
	if err != nil {
		return err
	}
	cur, err := histogram.GetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]libcounter.Bucket{buckets}, []value.Value{histogram.Zero()})
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
