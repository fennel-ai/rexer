package counter

import (
	"context"

	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/model/counter"
	"fennel/tier"
)

func Value(ctx context.Context, tier tier.Tier, key value.Value, histogram counter.Histogram) (value.Value, error) {
	end := ftypes.Timestamp(tier.Clock.Now())
	start := histogram.Start(end)
	buckets := histogram.BucketizeDuration(key.String(), start, end, histogram.Zero())
	counts, err := histogram.Get(ctx, tier, buckets, histogram.Zero())
	if err != nil {
		return nil, err
	}
	return histogram.Reduce(counts)
}

func Update(ctx context.Context, tier tier.Tier, table value.List, histogram counter.Histogram) error {
	buckets, err := counter.Bucketize(histogram, table)
	if err != nil {
		return err
	}
	buckets, err = counter.MergeBuckets(histogram, buckets)
	if err != nil {
		return err
	}
	return counter.Update(ctx, tier, buckets, histogram)
}
