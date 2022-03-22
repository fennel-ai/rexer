package counter

import (
	"context"
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/model/counter"
	"fennel/tier"
)

func Value(
	ctx context.Context, tier tier.Tier,
	name ftypes.AggName, key value.Value, histogram counter.Histogram, kwargs value.Dict,
) (value.Value, error) {
	end := ftypes.Timestamp(tier.Clock.Now())
	start, err := histogram.Start(end, kwargs)
	if err != nil {
		return nil, err
	}
	buckets := histogram.BucketizeDuration(key.String(), start, end, histogram.Zero())
	counts, err := histogram.Get(ctx, tier, name, buckets, histogram.Zero())
	if err != nil {
		return nil, err
	}
	return histogram.Reduce(counts)
}

func BatchValue(
	ctx context.Context, tier tier.Tier,
	names []ftypes.AggName, keys []value.Value, histograms []counter.Histogram, kwargs []value.Dict,
) ([]value.Value, error) {
	end := ftypes.Timestamp(tier.Clock.Now())
	unique := make(map[counter.BucketStore][]int)
	ret := make([]value.Value, len(names))
	for i, h := range histograms {
		bs := h.GetBucketStore()
		unique[bs] = append(unique[bs], i)
	}
	for bs, indices := range unique {
		n := len(indices)
		pos := make([]int, n)
		names_ := make([]ftypes.AggName, n)
		buckets := make([][]counter.Bucket, n)
		defaults := make([]value.Value, n)
		cur := 0
		for _, i := range indices {
			start, err := histograms[i].Start(end, kwargs[i])
			if err != nil {
				return nil, fmt.Errorf("failed to get start timestamp of aggregate at index %d of batch: %v", i, err)
			}
			pos[cur] = i
			names_[cur] = names[i]
			defaults[cur] = histograms[i].Zero()
			buckets[cur] = histograms[i].BucketizeDuration(keys[i].String(), start, end, defaults[cur])
			cur++
		}
		counts, err := bs.GetMulti(ctx, tier, names_, buckets, defaults)
		if err != nil {
			return nil, err
		}
		for i_ := range counts {
			i := pos[i_]
			ret[i], err = histograms[i].Reduce(counts[i_])
			if err != nil {
				return nil, fmt.Errorf("failed to reduce aggregate at index %d of batch: %v", i, err)
			}
		}
	}
	return ret, nil
}

func Update(
	ctx context.Context, tier tier.Tier, name ftypes.AggName, table value.List, histogram counter.Histogram,
) error {
	buckets, err := counter.Bucketize(histogram, table)
	if err != nil {
		return err
	}
	buckets, err = counter.MergeBuckets(histogram, buckets)
	if err != nil {
		return err
	}
	return counter.Update(ctx, tier, name, buckets, histogram)
}
