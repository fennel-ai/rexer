package counter

import (
	"context"

	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/model/counter"
	"fennel/tier"
)

func Value(ctx context.Context, tier tier.Tier, aggname ftypes.AggName, key value.Value, histogram counter.Histogram) (value.Value, error) {
	end := ftypes.Timestamp(tier.Clock.Now())
	start := histogram.Start(end)
	buckets := counter.BucketizeDuration(key.String(), start, end, histogram.Windows(), histogram.Zero())
	counts, err := counter.GetMulti(ctx, tier, aggname, buckets, histogram)
	if err != nil {
		return nil, err
	}
	return histogram.Reduce(counts)
}

func Update(ctx context.Context, tier tier.Tier, aggname ftypes.AggName, table value.Table, histogram counter.Histogram) error {
	buckets, err := histogram.Bucketize(table)
	if err != nil {
		return err
	}
	buckets, err = counter.MergeBuckets(histogram, buckets)
	if err != nil {
		return err
	}
	return counter.Update(ctx, tier, aggname, buckets, histogram)
}
