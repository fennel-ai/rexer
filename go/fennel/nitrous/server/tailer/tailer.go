package tailer

import (
	"context"
	"fennel/lib/utils/parallel"
	"fmt"
	"time"

	"fennel/hangar"
	fkafka "fennel/kafka"
	"fennel/lib/timer"
	"fennel/nitrous"
	"fennel/nitrous/rpc"
	"fennel/resource"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/samber/mo"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// The tailer_batch value is tuned to be large enough to process the binlog
	// quickly, but no so large that we get badger errors for transaction being
	// too large.
	DefaultTailerBatch = 20_000
	DefaultPollTimeout = 10 * time.Second
)

var (
	numProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "nitrous_tailer_num_processed",
		Help: "The number of messages processed by the tailer.",
	})
)

type EventsProcessor func(ctx context.Context, ops []*rpc.NitrousOp, store hangar.Reader) (keys []hangar.Key, vgs []hangar.ValGroup, err error)

type Tailer struct {
	topic       string
	processor   EventsProcessor
	nitrous     nitrous.Nitrous
	binlog      fkafka.FConsumer
	stopCh      chan chan struct{}
	pollTimeout time.Duration
	batchSize   int
	running     *atomic.Bool
	store       hangar.Hangar
	logger      *zap.Logger
}

// Returns a new Tailer that can be used to tail the binlog.
func NewTailer(n nitrous.Nitrous, topic string, toppar kafka.TopicPartition, store hangar.Hangar, processor EventsProcessor,
	pollTimeout time.Duration, batchSize int) (*Tailer, error) {
	logger := zap.L().Named(fmt.Sprintf("tailer-%s-%d", topic, toppar.Partition))
	// Given the topic partitions, decode what offsets to start reading from.
	consumer, err := n.KafkaConsumerFactory(fkafka.ConsumerConfig{
		Scope: resource.NewPlaneScope(n.PlaneID),
		Topic: topic,
		// Even though the nitrous instances which will come up, will be responsible for disjoint partition subset,
		// it is possible that the consumer group name is different for both, as the disjointed behavior is an eventual
		// property i.e. during the time when the new nitrous instance is catching up the binlog for the partitions
		// assigned to it, the requests for the data stored in corresponding partitions needs to be served and will
		// still be served from the previous nitrous instance. If they have the same consumer group name, one of them
		// may not have up-to date information.
		//
		// Similarly, for aggregate configurations, we want both the nitrous instances to see all the aggregate
		// configuration events
		GroupID:      n.Identity,
		OffsetPolicy: fkafka.DefaultOffsetPolicy,
		RebalanceCb: mo.Some(func(c *kafka.Consumer, e kafka.Event) error {
			logger.Info("Got kafka partition rebalance event: ", zap.String("topic", topic), zap.String("groupid", n.Identity), zap.String("consumer", c.String()), zap.String("event", e.String()))
			switch event := e.(type) {
			case kafka.AssignedPartitions:
				if len(event.Partitions) > 0 {
					// fetch the last committed offsets for the topic partitions assigned to the consumer
					var err error
					toppars, err := decodeOffsets([]kafka.TopicPartition{toppar}, store)
					if err != nil {
						logger.Fatal("Failed to fetch latest offsets", zap.String("consumer", c.String()), zap.Error(err))
					}
					logger.Info("Discarding broker assigned partitions and assigning partitions to self", zap.String("consumer", c.String()), zap.String("toppars", fmt.Sprintf("%v", toppars)))
					err = c.Assign(toppars)
					if err != nil {
						logger.Fatal("Failed to assign partitions", zap.Error(err))
					}
				}
			}
			return nil
		}),
		Configs: fkafka.ConsumerConfigs{
			// `max.partition.fetch.bytes` dictates the initial maximum number of bytes requested per
			// broker+partition.
			//
			// this could be restricted by `max.message.bytes` (topic) or `message.max.bytes` (broker) config
			"max.partition.fetch.bytes=2097164",
			// Maximum amount of data the broker shall return for a Fetch request.
			// Since this topic has consumers = partitions, this should preferably be
			// `max.partition.fetch.bytes x #partitions`
			"fetch.max.bytes=67109248",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize kafka consumer: %w", err)
	}
	t := Tailer{
		topic:       topic,
		processor:   processor,
		nitrous:     n,
		binlog:      consumer,
		stopCh:      make(chan chan struct{}),
		pollTimeout: pollTimeout,
		batchSize:   batchSize,
		running:     atomic.NewBool(false),
		store:       store,
		logger:      logger,
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

	// stop compaction on the underlying storage as well
	if err := t.store.StopCompaction(); err != nil {
		// this should never happen
		panic(fmt.Errorf("failed to stop compaction for the underlying hangar: %v", err))
	}
}

func (t *Tailer) processBatch(ctx context.Context, rawops [][]byte) error {
	parallel.Acquire(ctx, "nitrous", 2.2*parallel.OneCPU)
	// 2.2 is an empirical factor, means one call of the function takes 2.2 units of CPU, since it spawns multiple goroutines in the middle
	defer parallel.Release("nitrous", 2.2*parallel.OneCPU)
	defer numProcessed.Add(float64(len(rawops)))

	ctx, m := timer.Start(context.Background(), t.nitrous.PlaneID, "tailer.processBatch")
	defer m.Stop()
	t.logger.Debug("Got new messages from binlog", zap.Int("count", len(rawops)))
	ops := make([]*rpc.NitrousOp, len(rawops))
	for i, rawop := range rawops {
		op := rpc.NitrousOpFromVTPool()
		err := op.UnmarshalVT(rawop)
		if err != nil {
			return fmt.Errorf("failed to unmarshal rawop: %w", err)
		}
		ops[i] = op
	}
	ctx = hangar.NewWriteContext(ctx)
	keys, vgs, err := t.processor(ctx, ops, t.store)
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
	err = t.store.SetMany(context.Background(), keys, vgs)
	if err != nil {
		return fmt.Errorf("hangar write failed: %w", err)
	}
	// Commit the offsets to the kafka binlog.
	// This is not strictly required for correctly processing the binlog, but
	// needed to compute the lag.
	t.logger.Debug("Committing offsets to binlog", zap.Any("offsets", offs))
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
	if err := t.store.StartCompaction(); err != nil {
		// this should never happen
		panic(fmt.Errorf("failed to start compaction for underlying hangar: %v", err))
	}
	for {
		select {
		case ack := <-t.stopCh:
			ack <- struct{}{}
			return
		default:
			ctx := context.Background()
			t.logger.Debug("Waiting for new messages", zap.String("tailer", t.topic))
			rawops, err := t.binlog.ReadBatch(ctx, t.batchSize, t.pollTimeout)
			if kerr, ok := err.(kafka.Error); ok && (kerr.IsFatal() || kerr.Code() == kafka.ErrUnknownTopicOrPart) {
				t.logger.Fatal("Permanent error when reading from kafka", zap.Error(err))
			} else if err != nil {
				t.logger.Warn("Failed to read from binlog", zap.Error(err))
				// Insert a brief sleep to avoid busy loop.
				time.Sleep(t.pollTimeout)
				continue
			} else if len(rawops) == 0 {
				t.logger.Debug("No new messages from binlog")
				// Insert a brief sleep to avoid busy loop.
				time.Sleep(t.pollTimeout)
				continue
			}
			err = t.processBatch(ctx, rawops)
			if err != nil {
				t.logger.Error("Failed to process batch", zap.Error(err))
			}
		}
	}
}
