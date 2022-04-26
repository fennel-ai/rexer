package kv

import (
	"context"
	"fmt"

	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/kvstore"
	"fennel/lib/timer"
	"fennel/lib/value"
	modelCounter "fennel/model/counter"
	"fennel/model/counter/kv/codec"
	"fennel/model/counter/kv/codec/impls"
	"fennel/tier"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	// Use "current" codec only for testing. Production tiers should have a
	// codec that is fixed at initialization.
	LatestEncoder = impls.Current
)

const (
	tablet = kvstore.Aggregate
)

func Update(ctx context.Context, tr tier.Tier, aggIds []ftypes.AggId, histograms []modelCounter.Histogram, deltas [][]counter.Bucket, kv kvstore.ReaderWriter) error {
	// check that the inputs are of same size
	defer timer.Start(ctx, tr.ID, "counter_kv.update").Stop()
	if len(aggIds) != len(histograms) || len(aggIds) != len(deltas) {
		return fmt.Errorf("counter.kv.Update: aggIds, histogram and deltas must be of the same length")
	}

	defaults_ := make([]value.Value, len(aggIds))
	for i, h := range histograms {
		defaults_[i] = h.Zero()
	}

	vals, err := Get(ctx, tr, aggIds, deltas, defaults_, kv)
	if err != nil {
		return nil
	}
	for i := range deltas {
		h := histograms[i]
		for j := range deltas[i] {
			merged, err := h.Merge(vals[i][j], deltas[i][j].Value)
			if err != nil {
				return err
			}
			deltas[i][j].Value = merged
		}
	}
	return Set(ctx, tr, aggIds, deltas, kv)
}

func Set(ctx context.Context, tr tier.Tier, aggIds []ftypes.AggId, deltas [][]counter.Bucket, kv kvstore.ReaderWriter) error {
	defer timer.Start(ctx, tr.ID, "counter_kv.set").Stop()
	if len(aggIds) != len(deltas) {
		return fmt.Errorf("counter.kv.Set: aggIds, deltas must be of the same length")
	}
	if len(aggIds) == 0 {
		return nil
	}
	for i, aggId := range aggIds {
		for _, bucket := range deltas[i] {
			k, err := LatestEncoder.EncodeKey(aggId, bucket)
			if err != nil {
				return fmt.Errorf("failed to encode key: %v", err)
			}
			v, err := LatestEncoder.EncodeValue(bucket.Value)
			if err != nil {
				return err
			}
			if err = kv.Set(ctx, tablet, k, kvstore.SerializedValue{
				Codec: LatestEncoder.Identifier(),
				Raw:   v,
			}); err != nil {
				return fmt.Errorf("failed to set value in kv store: %v", err)
			}
		}
	}
	// no error so far, so transaction will be committed
	// add logging just before this
	for i, aggId := range aggIds {
		tr.Logger.Info("Updating badger keys for aggregate",
			zap.Int("aggregate", int(aggId)),
			zap.Int("num_keys", len(deltas[i])),
		)
	}
	return nil
}

func Get(ctx context.Context, tr tier.Tier, aggIds []ftypes.AggId, buckets [][]counter.Bucket, defaults_ []value.Value, kv kvstore.Reader) ([][]value.Value, error) {
	defer timer.Start(ctx, tr.ID, "counter_kv.get").Stop()
	if len(aggIds) != len(buckets) || len(aggIds) != len(defaults_) {
		return nil, fmt.Errorf("counter.kv.Get: names, buckets, and defaults must be the same length")
	}
	if len(aggIds) == 0 {
		return nil, nil
	}
	// TODO(mohit): For each aggrId, dedup Buckets to minimize roundtrips
	values := make([][]value.Value, len(aggIds))
	for i := range buckets {
		values[i] = make([]value.Value, len(buckets[i]))
	}
	errs, ctx := errgroup.WithContext(ctx)
	for i := range aggIds {
		for j := range buckets[i] {
			iIdx := i
			jIdx := j
			errs.Go(func() error {
				k, err := LatestEncoder.EncodeKey(aggIds[iIdx], buckets[iIdx][jIdx])
				if err != nil {
					return fmt.Errorf("failed to encode key: %v", err)
				}
				v, err := kv.Get(ctx, tablet, k)
				if err == kvstore.ErrKeyNotFound {
					values[iIdx][jIdx] = defaults_[iIdx]
				} else if err != nil {
					return fmt.Errorf("failed to get value: %v", err)
				} else {
					codec, err := codec.GetCodec(v.Codec)
					if err != nil {
						return fmt.Errorf("failed to get codec: %v", err)
					}
					values[iIdx][jIdx], err = codec.DecodeValue(v.Raw)
					if err != nil {
						return fmt.Errorf("failed to decode value: %v", err)
					}
				}
				return nil
			})
		}
	}
	return values, errs.Wait()
}
