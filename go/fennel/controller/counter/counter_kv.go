//go:build badger

package counter

import (
	"context"

	"fennel/lib/aggregate"
	"fennel/lib/badger"
	libcounter "fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/lib/value"
	"fennel/model/counter"
	"fennel/model/counter/kv"
	"fennel/tier"

	db "github.com/dgraph-io/badger/v3"
)

func Value(
	ctx context.Context, tr tier.Tier,
	aggId ftypes.AggId, key value.Value, histogram counter.Histogram, kwargs value.Dict,
) (value.Value, error) {
	defer timer.Start(ctx, tr.ID, "controller.counter_kv.value").Stop()
	end := ftypes.Timestamp(tr.Clock.Now())
	start, err := histogram.Start(end, kwargs)
	if err != nil {
		return nil, err
	}
	buckets := histogram.BucketizeDuration(key.String(), start, end, histogram.Zero())
	var counts []value.Value
	tr.Badger.View(func(txn *db.Txn) error {
		kvstore := badger.NewTransactionalStore(tr, txn)
		v, err := kv.Get(ctx, tr, []ftypes.AggId{aggId}, [][]libcounter.Bucket{buckets}, []value.Value{histogram.Zero()}, kvstore)
		if err != nil {
			return err
		}
		counts = v[0]
		return err
	})
	if err != nil {
		return nil, err
	}
	return histogram.Reduce(counts)
}

func BatchValue(
	ctx context.Context, tr tier.Tier,
	aggIds []ftypes.AggId, keys []value.Value, histograms []counter.Histogram, kwargs []value.Dict,
) ([]value.Value, error) {
	defer timer.Start(ctx, tr.ID, "controller.counter_kv.batch_value").Stop()
	end := ftypes.Timestamp(tr.Clock.Now())
	buckets := make([][]libcounter.Bucket, len(aggIds))
	defaults_ := make([]value.Value, len(aggIds))
	for i := range aggIds {
		h := histograms[i]
		defaults_[i] = h.Zero()
		start, err := h.Start(end, kwargs[i])
		if err != nil {
			return nil, err
		}
		buckets[i] = append(buckets[i], h.BucketizeDuration(keys[i].String(), start, end, h.Zero())...)
	}
	var counts [][]value.Value
	err := tr.Badger.View(func(txn *db.Txn) error {
		kvstore := badger.NewTransactionalStore(tr, txn)
		var err error
		counts, err = kv.Get(ctx, tr, aggIds, buckets, defaults_, kvstore)
		return err
	})
	if err != nil {
		return nil, err
	}
	ret := make([]value.Value, len(aggIds))
	for i := range counts {
		ret[i], err = histograms[i].Reduce(counts[i])
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func Update(
	ctx context.Context, tr tier.Tier, agg aggregate.Aggregate, table value.List, histogram counter.Histogram,
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
	deltaProducer := tr.Producers[libcounter.AGGREGATE_DELTA_TOPIC_NAME]
	// do not write to the storage here; in the badger mode, the tailer will read the consumer and write to storage
	return deltaProducer.LogProto(ctx, &ad, nil)
}
