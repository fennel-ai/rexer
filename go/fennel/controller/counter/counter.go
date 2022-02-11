package counter

import (
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/model/counter"
	"fennel/tier"
)

func Value(tier tier.Tier, aggname ftypes.AggName, key value.Value, histogram counter.Histogram) (value.Value, error) {
	end := ftypes.Timestamp(tier.Clock.Now())
	start := histogram.Start(end)
	buckets := counter.BucketizeDuration(key.String(), start, end, histogram.Windows(), histogram.Zero())
	counts, err := counter.GetMulti(tier, aggname, buckets, histogram)
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
	buckets, err = counter.MergeBuckets(histogram, buckets)
	if err != nil {
		return err
	}
	return counter.Update(tier, aggname, buckets, histogram)
}
