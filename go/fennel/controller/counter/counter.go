package counter

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"os"
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
	buckets := histogram.BucketizeDuration(key.String(), start, end, histogram.Zero())
	counts, err := histogram.Get(ctx, tier, aggId, buckets, histogram.Zero())
	if err != nil {
		return nil, err
	}
	return histogram.Reduce(counts)
}

func makeCacheKey(aggId ftypes.AggId, b libcounter.Bucket) string {
	return fmt.Sprintf("%d:%s:%d:%d:%d", aggId, b.Key, b.Window, b.Width, b.Index)
}

func fetchFromPCache(tier tier.Tier, aggId ftypes.AggId, buckets []libcounter.Bucket) ([]value.Value, []libcounter.Bucket) {
	unfilledBuckets := make([]libcounter.Bucket, 0, len(buckets))
	cachedVals := make([]value.Value, 0, len(buckets))

	for _, b := range buckets {
		if b.Window != ftypes.Window_DAY {
			unfilledBuckets = append(unfilledBuckets, b)
			continue
		}

		found := false
		ckey := makeCacheKey(aggId, b)

		if v, ok := tier.PCache.Get(ckey); ok {
			if val, ok2 := fromCacheValue(tier, v); ok2 {
				cachedVals = append(cachedVals, val)
				found = true
			}
		}

		if disableCache, present := os.LookupEnv("DISABLE_CACHE"); present && disableCache == "1" {
			found = false
		}

		if !found {
			unfilledBuckets = append(unfilledBuckets, b)
		}
	}
	return cachedVals, unfilledBuckets
}

func fillPCache(tier tier.Tier, aggIds []ftypes.AggId, buckets [][]libcounter.Bucket, bucketVal [][]value.Value) {
	if disableCache, present := os.LookupEnv("DISABLE_CACHE"); present && disableCache == "1" {
		return
	}

	for i := range buckets {
		aggId := aggIds[i]
		for j := range buckets[i] {
			if buckets[i][j].Window != ftypes.Window_DAY {
				continue
			}
			ckey := makeCacheKey(aggId, buckets[i][j])
			if ok := tier.PCache.SetWithTTL(ckey, bucketVal[i][j], int64(len(ckey)+len(bucketVal[i][j].String())), cacheValueDuration); !ok {
				tier.Logger.Debug(fmt.Sprintf("failed to set bucket aggregate value in cache: key: '%s' value: '%s'", ckey, bucketVal[i][j].String()))
			}
		}
	}
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
		cachedBuckets := make([][]value.Value, n)
		for i, index := range indices {
			h := histograms[index]
			start, err := h.Start(end, kwargs[index])
			if err != nil {
				return nil, fmt.Errorf("failed to get start timestamp of aggregate (id): %d, err: %v", aggIds[index], err)
			}
			ids_[i] = aggIds[index]
			bucketsForAggKey := h.BucketizeDuration(keys[index].String(), start, end, h.Zero())
			// Find buckets in a cache, if not found, fetch from the bucket store
			cachedBuckets[i], buckets[i] = fetchFromPCache(tier, ids_[i], bucketsForAggKey)
			defaults[i] = h.Zero()
		}
		counts, err := bs.GetMulti(ctx, tier, ids_, buckets, defaults)
		if err != nil {
			return nil, err
		}
		// fill in the cache with the fetched buckets in a seperate goroutine.
		fillPCache(tier, ids_, buckets, counts)
		for cur, index := range indices {
			counts[cur] = append(counts[cur], cachedBuckets[cur]...)
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

func fromCacheValue(tier tier.Tier, v interface{}) (value.Value, bool) {
	switch v := v.(type) {
	case value.Value:
		return v, true
	default:
		// log unexpected error
		err := fmt.Errorf("value not of type value.Value: %v", v)
		tier.Logger.Error("aggregate value cache error: ", zap.Error(err))
		return nil, false
	}
}
