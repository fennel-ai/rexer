package counter

import (
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/model/counter"
	"fennel/tier"
)

func Value(tier tier.Tier, agg aggregate.Aggregate, key value.Value, histogram counter.Histogram) (value.Value, error) {
	end := ftypes.Timestamp(tier.Clock.Now())
	start := histogram.Start(end)
	buckets := counter.BucketizeDuration(key.String(), start, end, histogram.Windows())
	counts, err := counter.GetMulti(tier, agg.Name, buckets)
	if err != nil {
		return value.List{}, err
	}
	return histogram.Reduce(counts)
}

func Update(tier tier.Tier, aggname ftypes.AggName, table value.Table, histogram counter.Histogram) error {
	buckets, err := histogram.Bucketize(table)
	if err != nil {
		return err
	}
	buckets = counter.MergeBuckets(histogram, buckets)
	return counter.Update(tier, aggname, buckets, histogram)
}
