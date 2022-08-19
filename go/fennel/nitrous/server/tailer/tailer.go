package tailer

import (
	"context"
	"fmt"
	"time"

	"fennel/hangar"
	fkafka "fennel/kafka"
	"fennel/lib/timer"
	"fennel/nitrous"
	"fennel/nitrous/rpc"
	"fennel/resource"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	// The tailer_batch value is tuned to be large enough to process the binlog
	// quickly, but no so large that we get badger errors for transaction being
	// too large.
	tailer_batch         = 20_000
	default_poll_timeout = 10 * time.Second
)

type EventsProcessor func(ctx context.Context, ops []*rpc.NitrousOp, store hangar.Reader) (keys []hangar.Key, vgs []hangar.ValGroup, err error)

type Tailer struct {
	processor   EventsProcessor
	nitrous     nitrous.Nitrous
	binlog      fkafka.FConsumer
	stopCh      chan chan struct{}
	pollTimeout time.Duration
	running     *atomic.Bool
}

// Returns a new Tailer that can be used to tail the binlog.
func NewTailer(n nitrous.Nitrous, topic string, toppars kafka.TopicPartitions, processor EventsProcessor) (*Tailer, error) {
	// Given the topic partitions, decode what offsets to start reading from.
	toppars, err := decodeOffsets(toppars, n.Store)
	if err != nil {
		return nil, fmt.Errorf("failed to decode offsets: %w", err)
	}
	consumer, err := n.KafkaConsumerFactory(fkafka.ConsumerConfig{
		Scope:        resource.NewPlaneScope(n.PlaneID),
		Topic:        topic,
		GroupID:      n.Identity,
		OffsetPolicy: fkafka.DefaultOffsetPolicy,
		Partitions:   toppars,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize kafka consumer: %w", err)
	}
	t := Tailer{
		processor:   processor,
		nitrous:     n,
		binlog:      consumer,
		stopCh:      make(chan chan struct{}),
		pollTimeout: default_poll_timeout,
		running:     atomic.NewBool(false),
	}
	return &t, nil
}

// Returns the kafka offsets of this tailer. These can be used to initialize a
// new tailer.
func (t *Tailer) GetOffsets() (kafka.TopicPartitions, error) {
	return t.binlog.Offsets()
}

func (t *Tailer) SetPollTimeout(d time.Duration) {
	t.pollTimeout = d
}

func (t *Tailer) GetPollTimeout() time.Duration {
	return t.pollTimeout
}

func (t *Tailer) GetLag() (int, error) {
	return t.binlog.Backlog()
}

func (t *Tailer) Stop() {
	if !t.running.Load() {
		return
	}
	ack := make(chan struct{})
	t.stopCh <- ack
	<-ack
	t.running.Store(false)
}

func (t *Tailer) processBatch(rawops [][]byte) error {
	ctx, m := timer.Start(context.Background(), t.nitrous.PlaneID, "tailer.processBatch")
	defer m.Stop()
	zap.L().Debug("Got new messages from binlog", zap.Int("count", len(rawops)))
	ops := make([]*rpc.NitrousOp, len(rawops))
	for i, rawop := range rawops {
		var op rpc.NitrousOp
		err := proto.Unmarshal(rawop, &op)
		if err != nil {
			zap.L().Error("Failed to unmarshal op", zap.Error(err))
		}
		ops[i] = &op
	}
	keys, vgs, err := t.processor(ctx, ops, t.nitrous.Store)
	if err != nil {
		return fmt.Errorf("failed to proces: %w", err)
	}
	// Save kafka offsets in the same batch for exactly-once processing.
	offs, err := t.binlog.Offsets()
	if err != nil {
		return fmt.Errorf("failed to get offsets: %w", err)
	}
	offkeys, offvgs, err := encodeOffsets(offs)
	if err != nil {
		return fmt.Errorf("failed to encode offsets: %w", err)
	}
	keys = append(keys, offkeys...)
	vgs = append(vgs, offvgs...)
	// Finally, write the batch to the hangar.
	err = t.nitrous.Store.SetMany(context.Background(), keys, vgs)
	if err != nil {
		return fmt.Errorf("hangar write failed: %w", err)
	}
	// Commit the offsets to the kafka binlog.
	// This is not strictly required for correctly processing the binlog, but
	// needed to compute the lag.
	zap.L().Debug("Committing offsets to binlog", zap.Any("offsets", offs))
	_, err = t.binlog.CommitOffsets(offs)
	if err != nil {
		return fmt.Errorf("failed to commit binlog offsets to broker: %w", err)
	}
	return nil
}

// Start tailing the kafka binlog and forwarding events to processors.
// Note: This function blocks the caller and should be run in a goroutine. To
// stop the tailer, call Stop().
func (t *Tailer) Tail() {
	t.running.Store(true)
	for {
		select {
		case ack := <-t.stopCh:
			ack <- struct{}{}
			return
		default:
			ctx := context.Background()
			zap.L().Debug("Waiting for new messages from binlog...")
			rawops, err := t.binlog.ReadBatch(ctx, tailer_batch, t.pollTimeout)
			if kerr, ok := err.(kafka.Error); ok && (kerr.IsFatal() || kerr.Code() == kafka.ErrUnknownTopicOrPart) {
				zap.L().Fatal("Permanent error when reading from kafka", zap.Error(err))
			} else if err != nil {
				zap.L().Warn("Failed to read from binlog", zap.Error(err))
				// Insert a brief sleep to avoid busy loop.
				time.Sleep(t.pollTimeout)
				continue
			} else if len(rawops) == 0 {
				zap.L().Debug("No new messages from binlog")
				// Insert a brief sleep to avoid busy loop.
				time.Sleep(t.pollTimeout)
				continue
			}
			err = t.processBatch(rawops)
			if err != nil {
				zap.L().Error("Failed to process batch", zap.Error(err))
			}
		}
	}
}
