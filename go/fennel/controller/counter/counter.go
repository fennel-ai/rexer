//go:build !badger

package counter

import (
	"context"
	"fmt"

	"fennel/lib/aggregate"
	libcounter "fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/model/counter"
	"fennel/tier"
)

func Value(
	ctx context.Context, tier tier.Tier,
	aggId ftypes.AggId, key value.Value, histogram, hOld counter.Histogram, kwargs value.Dict,
) (value.Value, error) {
	end := ftypes.Timestamp(tier.Clock.Now())
	start, err := histogram.Start(end, kwargs)
	if err != nil {
		return nil, err
	}
	buckets := histogram.BucketizeDuration(key.String(), start, end, histogram.Zero())
	counts, err := histogram.Get(ctx, tier, aggId, buckets, histogram.Zero())
	if err != nil {
		return nil, err
	}
	if hOld != nil {
		bucketsOld := hOld.BucketizeDuration(key.String(), start, end, hOld.Zero())
		countsOld, err := hOld.Get(ctx, tier, aggId, bucketsOld, hOld.Zero())
		if err != nil {
			return nil, err
		}
		counts = append(countsOld, counts...)
	}
	return histogram.Reduce(counts)
}

// TODO(Mohit): Fix this code if we decide to still use BucketStore
// BucketStore instances are created per histogram - the list `indices` created is always a single element list
func BatchValue(
	ctx context.Context, tier tier.Tier,
	aggIds []ftypes.AggId, keys []value.Value, histograms, hsOld []counter.Histogram, kwargs []value.Dict,
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
		bucketsOld := make([][]libcounter.Bucket, n)
		defaults := make([]value.Value, n)
		for i, index := range indices {
			h := histograms[index]
			start, err := h.Start(end, kwargs[index])
			if err != nil {
				return nil, fmt.Errorf("failed to get start timestamp of aggregate (id): %d, err: %v", aggIds[index], err)
			}
			ids_[i] = aggIds[index]
			buckets[i] = h.BucketizeDuration(keys[index].String(), start, end, h.Zero())
			if hsOld != nil {
				bucketsOld[i] = hsOld[index].BucketizeDuration(keys[index].String(), start, end, hsOld[index].Zero())
			}
			defaults[i] = h.Zero()
		}
		counts, err := bs.GetMulti(ctx, tier, ids_, buckets, defaults)
		if err != nil {
			return nil, err
		}
		if hsOld != nil {
			countsOld, err := hsOld[indices[0]].GetBucketStore().GetMulti(ctx, tier, ids_, bucketsOld, defaults)
			if err != nil {
				return nil, err
			}
			for i := range counts {
				counts[i] = append(counts[i], countsOld[i]...)
			}
		}
		cur := 0
		for _, index := range indices {
			ret[index], err = histograms[index].Reduce(counts[cur])
			if err != nil {
				return nil, fmt.Errorf("failed to reduce aggregate (id): %d, err: %v", aggIds[index], err)
			}
			cur++
		}
	}
	return ret, nil
}

func Update(
	ctx context.Context, tier tier.Tier, agg aggregate.Aggregate, table value.List, histogram counter.Histogram,
) error {
	buckets, err := counter.Bucketize(histogram, table)
	if err != nil {
		return err
	}
	buckets, err = counter.MergeBuckets(histogram, buckets)
	if err != nil {
		return err
	}
	// log the deltas to be consumed by the tailer
	ad, err := libcounter.ToProtoAggregateDelta(agg.Id, agg.Options, buckets)
	if err != nil {
		return err
	}
	deltaProducer := tier.Producers[libcounter.AGGREGATE_DELTA_TOPIC_NAME]
	err = deltaProducer.LogProto(ctx, &ad, nil)
	if err != nil {
		return err
	}
	return counter.Update(ctx, tier, agg.Id, buckets, histogram)
}
