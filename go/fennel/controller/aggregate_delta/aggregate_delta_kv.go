//go:build badger

package aggregate_delta

import (
	"context"
	"time"

	libkafka "fennel/kafka"
	"fennel/lib/badger"
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	modelCounter "fennel/model/counter"
	"fennel/model/counter/kv"
	"fennel/model/offsets"
	"fennel/tier"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	db "github.com/dgraph-io/badger/v3"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type bucketKey struct {
	AggId  ftypes.AggId
	Key    string
	Window ftypes.Window
	Width  uint64
	Index  uint64
}

func readBatch(ctx context.Context, consumer libkafka.FConsumer, count int, timeout time.Duration) ([]counter.AggregateDelta, error) {
	msgs, err := consumer.ReadBatch(ctx, count, timeout)
	if err != nil {
		return nil, err
	}
	ads := make([]counter.AggregateDelta, len(msgs))
	for i := range msgs {
		var p counter.ProtoAggregateDelta
		if err = proto.Unmarshal(msgs[i], &p); err != nil {
			return nil, err
		}
		if ads[i], err = counter.FromProtoAggregateDelta(&p); err != nil {
			return nil, err
		}
	}
	return ads, nil
}

func TransferAggrDeltasToDB(ctx context.Context, tr tier.Tier, consumer libkafka.FConsumer) error {
	// read from kafka
	ads, err := readBatch(ctx, consumer, 1000 /*count=*/, 1*time.Second /*timeout=*/)
	if err != nil {
		return err
	}

	// compute bucket values offline to avoid additional roundtrips
	aggrHist := make(map[ftypes.AggId]modelCounter.Histogram)
	bucketVals := make(map[bucketKey][]value.Value, len(ads))
	for _, ad := range ads {
		for _, b := range ad.Buckets {
			bk := bucketKey{
				AggId:  ad.AggId,
				Key:    b.Key,
				Window: b.Window,
				Width:  b.Width,
				Index:  b.Index,
			}
			bucketVals[bk] = append(bucketVals[bk], b.Value)
		}
		if _, ok := aggrHist[ad.AggId]; !ok {
			hist, err := modelCounter.ToHistogram(ad.Options)
			if err != nil {
				return err
			}
			aggrHist[ad.AggId] = hist
		}
	}

	// reduce the values
	aggrBuckets := make(map[ftypes.AggId][]counter.Bucket, len(aggrHist))
	for b, vals := range bucketVals {
		val, err := aggrHist[b.AggId].Reduce(vals)
		if err != nil {
			return err
		}
		aggrBuckets[b.AggId] = append(aggrBuckets[b.AggId], counter.Bucket{
			Key:    b.Key,
			Window: b.Window,
			Width:  b.Width,
			Index:  b.Index,
			Value:  val,
		})
	}

	aggIds := make([]ftypes.AggId, len(aggrBuckets))
	buckets := make([][]counter.Bucket, len(aggrBuckets))
	histograms := make([]modelCounter.Histogram, len(aggrBuckets))
	curr := 0
	for aggId, bs := range aggrBuckets {
		aggIds[curr] = aggId
		buckets[curr] = bs
		histograms[curr] = aggrHist[aggId]
		curr++
	}
	var partitions kafka.TopicPartitions
	err = tr.Badger.Update(func(txn *db.Txn) error {
		kvstore := badger.NewTransactionalStore(tr, txn)
		err = kv.Update(ctx, tr, aggIds, histograms, buckets, kvstore)
		if err != nil {
			return errors.Wrap(err, "failed to update counters")
		}
		partitions, err = consumer.Offsets()
		if err != nil {
			return errors.Wrap(err, "failed to read current kafka offsets")
		}
		tr.Logger.Debug("Committing offsets", zap.Any("partitions", partitions))
		err = offsets.Set(ctx, partitions, kvstore)
		if err != nil {
			return errors.Wrap(err, "failed to write offsets")
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "failed to write badger transaction to update counters")
	}
	// Sync contents to disk, just to be safe.
	err = tr.Badger.Sync()
	if err != nil {
		return errors.Wrap(err, "failed to sync badger to disk")
	}
	_, err = consumer.CommitOffsets(partitions)
	if err != nil {
		return errors.Wrap(err, "failed to commit offsets")
	}
	return nil
}
