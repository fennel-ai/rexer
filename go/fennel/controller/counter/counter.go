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
	nitrous "fennel/nitrous/client"
	"fennel/tier"

	"github.com/Unleash/unleash-client-go/v3"
	"github.com/samber/mo"
	"go.uber.org/zap"
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

func NitrousBatchValue(
	ctx context.Context, tier tier.Tier, aggIds []ftypes.AggId, keys []value.Value, kwargs []value.Dict,
) ([]value.Value, error) {
	ctx, t := timer.Start(ctx, tier.ID, "counter.NitrousBatchValue")
	defer t.Stop()
	ret := make([]value.Value, len(keys))
	idxByAgg := make(map[ftypes.AggId][]int)
	for i, aggId := range aggIds {
		idxByAgg[aggId] = append(idxByAgg[aggId], i)
	}
	// Note: we make the calls serially because for the most part, we will only
	// have one aggregate per call.
	// TODO(abhay): Call nitrous in parallel if/when we have multiple aggregates.
	for aggId, indices := range idxByAgg {
		aggkeys := arena.Values.Alloc(len(indices), len(indices))
		defer arena.Values.Free(aggkeys)
		aggkwargs := arena.DictValues.Alloc(len(indices), len(indices))
		defer arena.DictValues.Free(aggkwargs)
		for i, index := range indices {
			aggkeys[i] = keys[index]
			aggkwargs[i] = kwargs[index]
		}
		output := arena.Values.Alloc(len(indices), len(indices))
		defer arena.Values.Free(output)

		// TODO(mohit): We should send the Get request based on the groupkey ('aggkeys') since the binlog is sharded
		err := tier.NitrousClient.MustGet().GetMulti(ctx, aggId, aggkeys, aggkwargs, output)
		if err != nil {
			return nil, err
		}
		for i, index := range indices {
			ret[index] = output[i]
		}
	}
	return ret, nil
}

// TODO(Mohit): Fix this code if we decide to still use BucketStore
// BucketStore instances are created per histogram - the list `indices` created is always a single element list
func BatchValue(
	ctx context.Context, tier tier.Tier,
	aggIds []ftypes.AggId, aggOptions []aggregate.Options, keys []value.Value, kwargs []value.Dict,
) ([]value.Value, error) {
	// Send a shadow request to nitrous if client has been initialized.
	if tier.NitrousClient.IsPresent() && unleash.IsEnabled("nitrous_shadow_requests") {
		// Copy the input arrays to allow the original to be returned to the
		// arena in case they were arena allocated without causing a race
		// condition.
		aggIdsDup := make([]ftypes.AggId, len(aggIds))
		copy(aggIdsDup, aggIds)
		keysDup := arena.Values.Alloc(len(keys), len(keys))
		copy(keysDup, keys)
		kwargsDup := arena.DictValues.Alloc(len(kwargs), len(kwargs))
		copy(kwargsDup, kwargs)
		go func(aggIds []ftypes.AggId, keys []value.Value, kwargs []value.Dict) {
			defer arena.Values.Free(keys)
			defer arena.DictValues.Free(kwargs)
			ctx := context.Background()
			_, err := NitrousBatchValue(ctx, tier, aggIds, keys, kwargs)
			if err != nil {
				tier.Logger.Warn("Nitrous read error", zap.Error(err))
			}
		}(aggIdsDup, keysDup, kwargsDup)
	}
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
		buckets := make([][]libcounter.Bucket, n)
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
			buckets[i] = h.BucketizeDuration(keys[index].String(), start, end)
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
		// Explicitly free the counter and bucket slices back to the arena.
		for i, v := range counts {
			arena.Values.Free(v)
			arena.Buckets.Free(buckets[i])
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

	if tier.NitrousClient.IsPresent() {
		// Forward updates to nitrous asynchronously. If it fails, log the error.
		// In the future, we will want to handle this error and in fact have
		// exactly-once processing of updates.
		go func() {
			var err error
			tier.NitrousClient.ForEach(func(client nitrous.NitrousClient) {
				err = client.Push(ctx, aggId, table)
			})
			if err != nil {
				tier.Logger.Info("Failed to push updates to nitrous", zap.Error(err))
			}
		}()
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
