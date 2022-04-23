//go:build badger

package aggregate_delta

import (
	"context"
	"fmt"
	"time"

	libkafka "fennel/kafka"
	"fennel/lib/badger"
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
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
	defer timer.Start(ctx, tr.ID, "aggregate_delta.TransferToDB").Stop()
	// read from kafka
	ads, err := readBatch(ctx, consumer, 1000 /*count=*/, 10*time.Second /*timeout=*/)
	if err != nil {
		return err
	}

	// merge buckets and send an accumulated update, this should help save few round trips
	aggHist := make(map[ftypes.AggId]modelCounter.Histogram, len(ads))
	aggBuckets := make(map[ftypes.AggId][]counter.Bucket, len(ads))
	for _, ad := range ads {
		aggBuckets[ad.AggId] = append(aggBuckets[ad.AggId], ad.Buckets...)
		if _, ok := aggHist[ad.AggId]; !ok {
			hist, err := modelCounter.ToHistogram(ad.Options)
			if err != nil {
				return err
			}
			aggHist[ad.AggId] = hist
		}
	}

	aggrBuckets := make(map[ftypes.AggId][]counter.Bucket, len(aggHist))
	for aggId, buckets := range aggBuckets {
		h, ok := aggHist[aggId]
		if !ok {
			return fmt.Errorf("histogram not found for aggId: %v", aggId)
		}
		aggrBuckets[aggId], err = modelCounter.MergeBuckets(h, buckets)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to merge buckets for aggId: %v", aggId))
		}
	}

	aggIds := make([]ftypes.AggId, len(aggrBuckets))
	buckets := make([][]counter.Bucket, len(aggrBuckets))
	histograms := make([]modelCounter.Histogram, len(aggrBuckets))
	curr := 0
	for aggId, bs := range aggrBuckets {
		aggIds[curr] = aggId
		buckets[curr] = bs
		histograms[curr] = aggHist[aggId]
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
