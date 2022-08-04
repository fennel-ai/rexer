package tailer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"fennel/hangar"
	fkafka "fennel/kafka"
	"fennel/lib/timer"
	"fennel/nitrous"
	"fennel/nitrous/rpc"
	"fennel/nitrous/server/offsets"
	"fennel/resource"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

const (
	tailer_batch         = 100_000
	default_poll_timeout = 10 * time.Second
)

var (
	backlog = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "nitrous_tailer_backlog",
		Help: "Backlog of tailer",
	})
)

type EventProcessor interface {
	Identity() string
	Process(ctx context.Context, ops []*rpc.NitrousOp, store hangar.Reader) (keys []hangar.Key, vgs []hangar.ValGroup, err error)
}

type Tailer struct {
	processors  []EventProcessor
	nitrous     nitrous.Nitrous
	binlog      fkafka.FConsumer
	offsetkey   []byte
	stopCh      chan chan struct{}
	pollTimeout time.Duration
	running     *atomic.Bool

	mu *sync.RWMutex
}

// Start a go-routine that periodically reports the tailer's backlog to prometheus.
func (t *Tailer) startReportingKafkaLag(consumer fkafka.FConsumer) {
	go func() {
		for range time.Tick(10 * time.Second) {
			b, err := consumer.Backlog()
			if err != nil {
				t.nitrous.Logger.Error("Failed to read kafka backlog", zap.String("Name", consumer.GroupID()), zap.Error(err))
			}
			backlog.Set(float64(b))
		}
	}()
}

// Returns a new Tailer that can be used to tail the binlog.
// 'offsets' denotes the kafka offsets at which the tailer should start tailing
// the log. 'offsetkey' denotes the keygroup under which the offsets should be
// checkpointed in the plane's hangar.
func NewTailer(n nitrous.Nitrous, topic string, offsets kafka.TopicPartitions, offsetkey []byte) (*Tailer, error) {
	stopCh := make(chan chan struct{})
	consumer, err := n.KafkaConsumerFactory(fkafka.ConsumerConfig{
		Scope:        resource.NewPlaneScope(n.PlaneID),
		Topic:        topic,
		GroupID:      n.Identity,
		OffsetPolicy: fkafka.DefaultOffsetPolicy,
		Partitions:   offsets,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize kafka consumer: %w", err)
	}
	t := Tailer{
		nil,
		n,
		consumer,
		offsetkey,
		stopCh,
		default_poll_timeout,
		atomic.NewBool(false),
		&sync.RWMutex{},
	}
	t.startReportingKafkaLag(consumer)
	return &t, nil
}

func (t *Tailer) Subscribe(p EventProcessor) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.processors = append(t.processors, p)
}

func (t *Tailer) Unsubscribe(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i, c := range t.processors {
		if c.Identity() == id {
			t.processors[i] = t.processors[len(t.processors)-1]
			t.processors = t.processors[:len(t.processors)-1]
			return
		}
	}
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
	t.nitrous.Logger.Debug("Got new messages from binlog", zap.Int("count", len(rawops)))
	ops := make([]*rpc.NitrousOp, len(rawops))
	for i, rawop := range rawops {
		var op rpc.NitrousOp
		err := proto.Unmarshal(rawop, &op)
		if err != nil {
			t.nitrous.Logger.Error("Failed to unmarshal op", zap.Error(err))
		}
		ops[i] = &op
	}
	type update struct {
		keys []hangar.Key
		vgs  []hangar.ValGroup
	}
	eg := &errgroup.Group{}
	t.mu.RLock()
	processors := t.processors
	t.mu.RUnlock()
	updates := make(chan update, len(processors))
	for i := range processors {
		p := processors[i]
		eg.Go(func() error {
			ks, vs, err := p.Process(ctx, ops, t.nitrous.Store)
			if err != nil {
				t.nitrous.Logger.Error("Failed to process ops", zap.String("processor", p.Identity()), zap.Error(err))
				return err
			}
			updates <- update{ks, vs}
			return nil
		})
	}
	// Wait for all processors to finish and then close updates channel.
	err := eg.Wait()
	close(updates)
	if err != nil {
		return fmt.Errorf("One or more op processors failed: %w", err)
	}
	// Consolidate all updates into one write batch.
	var keys []hangar.Key
	var vgs []hangar.ValGroup
	for update := range updates {
		keys = append(keys, update.keys...)
		vgs = append(vgs, update.vgs...)
	}
	// Save kafka offsets in the same batch for exactly-once processing.
	offs, err := t.binlog.Offsets()
	if err != nil {
		return fmt.Errorf("failed to get offsets: %w", err)
	}
	offvg, err := offsets.EncodeOffsets(offs)
	if err != nil {
		return fmt.Errorf("failed to encode offsets: %w", err)
	}
	keys = append(keys, hangar.Key{Data: t.offsetkey})
	vgs = append(vgs, offvg)
	// Finally, write the batch to the hangar.
	err = t.nitrous.Store.SetMany(context.Background(), keys, vgs)
	if err != nil {
		return fmt.Errorf("hangar write failed: %w", err)
	}
	// Commit the offsets to the kafka binlog.
	// This is not strictly necessary in prod, but useful in tests.
	t.nitrous.Logger.Debug("Committing offsets to binlog", zap.Any("offsets", offs))
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
			t.nitrous.Logger.Debug("Waiting for new messages from binlog...")
			rawops, err := t.binlog.ReadBatch(ctx, tailer_batch, t.pollTimeout)
			if kerr, ok := err.(kafka.Error); ok && (kerr.IsFatal() || kerr.Code() == kafka.ErrUnknownTopicOrPart) {
				t.nitrous.Logger.Fatal("Permanent error when reading from kafka", zap.Error(err))
			} else if err != nil {
				t.nitrous.Logger.Warn("Failed to read from binlog", zap.Error(err))
				// Insert a brief sleep to avoid busy loop.
				time.Sleep(t.pollTimeout)
				continue
			} else if len(rawops) == 0 {
				t.nitrous.Logger.Debug("No new messages from binlog")
				// Insert a brief sleep to avoid busy loop.
				time.Sleep(t.pollTimeout)
				continue
			}
			err = t.processBatch(rawops)
			if err != nil {
				t.nitrous.Logger.Error("Failed to process batch", zap.Error(err))
			}
		}
	}
}
