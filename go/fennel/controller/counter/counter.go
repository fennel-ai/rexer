package counter

import (
	"context"
	"fennel/lib/arena"
	"fmt"
	"time"

	"fennel/lib/aggregate"
	libcounter "fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/model/counter"
	"fennel/tier"
)

var cacheValueDuration = 30 * time.Minute

func Value(
	ctx context.Context, tier tier.Tier,
	aggId ftypes.AggId, key value.Value, histogram counter.Histogram, kwargs value.Dict,
) (value.Value, error) {
	end := ftypes.Timestamp(tier.Clock.Now())
	start, err := histogram.Start(end, kwargs)
	if err != nil {
		return nil, err
	}
	bucketizer := counter.GetFixedWidthBucketizer(histogram)
	buckets := bucketizer.BucketizeDuration(key.String(), start, end)
	defer arena.Buckets.Free(buckets)
	counts, err := histogram.GetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]libcounter.Bucket{buckets}, []value.Value{histogram.Zero()})
	if err != nil {
		return nil, err
	}
	return histogram.Reduce(counts[0])
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
			start, err := h.Start(end, kwargs[index])
			if err != nil {
				return nil, fmt.Errorf("failed to get start timestamp of aggregate (id): %d, err: %v", aggIds[index], err)
			}
			ids_[i] = aggIds[index]
			bucketizer := counter.GetFixedWidthBucketizer(h)
			buckets[i] = bucketizer.BucketizeDuration(keys[index].String(), start, end)
			defer arena.Buckets.Free(buckets[i])
			defaults[i] = h.Zero()
		}
		counts, err := bs.GetMulti(ctx, tier, ids_, buckets, defaults)
		if err != nil {
			return nil, err
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
	ctx context.Context, tier tier.Tier, agg aggregate.Aggregate, table value.List, histogram counter.Histogram,
) error {
	bucketizer := counter.GetFixedWidthBucketizer(histogram)
	buckets, values, err := counter.Bucketize(bucketizer, table)
	if err != nil {
		return err
	}
	buckets, values, err = counter.MergeBuckets(histogram, buckets, values)
	if err != nil {
		return err
	}
	return counter.Update(ctx, tier, agg.Id, buckets, values, histogram)
}
