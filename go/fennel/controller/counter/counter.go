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
	aggId ftypes.AggId, key value.Value, histogram counter.Histogram, kwargs value.Dict,
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
	return histogram.Reduce(counts)
}

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
				return nil, fmt.Errorf("failed to get start timestamp of aggregate at index %d of batch: %v", i, err)
			}
			ids_[i] = aggIds[index]
			buckets[i] = h.BucketizeDuration(keys[i].String(), start, end, h.Zero())
			defaults[i] = h.Zero()
		}
		counts, err := bs.GetMulti(ctx, tier, ids_, buckets, defaults)
		if err != nil {
			return nil, err
		}
		cur := 0
		for i, index := range indices {
			ret[index], err = histograms[index].Reduce(counts[cur])
			if err != nil {
				return nil, fmt.Errorf("failed to reduce aggregate at index %d of batch: %v", i, err)
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
