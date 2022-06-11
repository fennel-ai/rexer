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
	storageBuckets := convertBuckets(histogram.BucketizeDuration(key.String(), start, end))
	defer arena.Buckets.Free(storageBuckets)
	counts, err := histogram.GetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]libcounter.StorageBucket{buckets}, []value.Value{histogram.Zero()})
	if err != nil {
		return nil, err
	}
	defer arena.Values.Free(counts[0])
	return histogram.Reduce(counts[0])
}

type BucketCompressor struct {
	HInfo          map[ftypes.HistId]counter.HistogramInfo
	SeenHistograms map[counter.HistogramInfo]ftypes.HistId
	HIdCntr        ftypes.HistId
}

func NewBucketCompressor() *BucketCompressor {
	return &BucketCompressor{
		HInfo:          make(map[ftypes.HistId]counter.HistogramInfo),
		SeenHistograms: make(map[counter.HistogramInfo]ftypes.HistId),
		HIdCntr:        0,
	}
}

func (c *BucketCompressor) Compress(buckets []libcounter.Bucket, zero value.Value, aggId ftypes.AggId) []libcounter.StorageBucket {
	n := len(buckets)
	ret := make([]libcounter.StorageBucket, n)
	for i, b := range buckets {
		hInfo := counter.HistogramInfo{
			aggId,
			zero,
			b.Window,
			b.Width,
		}
		hId, ok := c.SeenHistograms[hInfo]
		if !ok {
			c.HIdCntr++
			c.SeenHistograms[hInfo] = c.HIdCntr
			c.HInfo[c.HIdCntr] = hInfo
		}

		ret[i] = libcounter.StorageBucket{
			Key:   b.Key,
			Index: b.Index,
			HId:   hId,
		}
	}
	return ret
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
	bucketCompressor := NewBucketCompressor()
	for i, h := range histograms {
		bs := h.GetBucketStore()
		unique[bs] = append(unique[bs], i)
	}
	for bs, indices := range unique {
		n := len(indices)
		ids_ := make([]ftypes.AggId, n)
		// buckets := make([][]libcounter.Bucket, n)
		buckets := make([][]libcounter.StorageBucket, n)
		defaults := make([]value.Value, n)
		for i, index := range indices {
			h := histograms[index]
			start, err := h.Start(end, kwargs[index])
			if err != nil {
				return nil, fmt.Errorf("failed to get start timestamp of aggregate (id): %d, err: %v", aggIds[index], err)
			}
			ids_[i] = aggIds[index]
			bkts := h.BucketizeDuration(keys[index].String(), start, end)
			buckets[i] = bucketCompressor.Compress(bkts, h.Zero(), aggIds[index])
			defer arena.Buckets.Free(buckets[i])
			defaults[i] = h.Zero()
		}

		counts, err := bs.GetMulti(ctx, tier, ids_, buckets, bucketCompressor.HInfo)
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
	ctx context.Context, tier tier.Tier, agg aggregate.Aggregate, table value.List, histogram counter.Histogram,
) error {
	buckets, values, err := counter.Bucketize(histogram, table)
	if err != nil {
		return err
	}
	buckets, values, err = counter.MergeBuckets(histogram, buckets, values)
	if err != nil {
		return err
	}
	return counter.Update(ctx, tier, agg.Id, buckets, values, histogram)
}
