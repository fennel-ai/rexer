package kv

import (
	"context"
	"fmt"

	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/kvstore"
	"fennel/lib/value"
	"fennel/model/counter/kv/codec"
	"fennel/model/counter/kv/codec/impls"
	"fennel/tier"

	"go.uber.org/zap"
)

var (
	// Use "current" codec only for testing. Production tiers should have a
	// codec that is fixed at initialization.
	LatestEncoder = impls.Current
)

const (
	tablet = kvstore.Aggregate
)

func Set(ctx context.Context, tr tier.Tier, aggIds []ftypes.AggId, deltas [][]counter.Bucket, kv kvstore.ReaderWriter) error {
	if len(aggIds) != len(deltas) {
		return fmt.Errorf("counter.kv.Set: aggIds, deltas must be the same length")
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
	if len(aggIds) != len(buckets) || len(aggIds) != len(defaults_) {
		return nil, fmt.Errorf("counter.kv.Get: names, buckets, and defaults must be the same length")
	}
	if len(aggIds) == 0 {
		return nil, nil
	}
	values := make([][]value.Value, len(aggIds))
	for i := range buckets {
		values[i] = make([]value.Value, len(buckets[i]))
	}
	for i := range aggIds {
		for j := range buckets[i] {
			k, err := LatestEncoder.EncodeKey(aggIds[i], buckets[i][j])
			if err != nil {
				return nil, fmt.Errorf("failed to encode key: %v", err)
			}
			v, err := kv.Get(ctx, tablet, k)
			if err == kvstore.ErrKeyNotFound {
				values[i][j] = defaults_[i]
			} else if err != nil {
				return nil, fmt.Errorf("failed to get value: %v", err)
			} else {
				codec, err := codec.GetCodec(v.Codec)
				if err != nil {
					return nil, fmt.Errorf("failed to get codec: %v", err)
				}
				values[i][j], err = codec.DecodeValue(v.Raw)
				if err != nil {
					return nil, fmt.Errorf("failed to decode value: %v", err)
				}
			}
		}
	}
	return values, nil
}
