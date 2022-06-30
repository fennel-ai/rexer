package tailer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"fennel/hangar"
	fkafka "fennel/kafka"
	rpc "fennel/nitrous/rpc/v2"
	"fennel/nitrous/server/offsets"
	"fennel/plane"
	"fennel/resource"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	tailer_batch = 1000
)

type EventProcessor interface {
	Identity() string
	Process(ctx context.Context, ops []*rpc.NitrousOp) (keys []hangar.Key, vgs []hangar.ValGroup, err error)
}

type Tailer struct {
	processors  []EventProcessor
	plane       plane.Plane
	binlog      fkafka.FConsumer
	offsetkey   []byte
	stopCh      chan struct{}
	pollTimeout time.Duration

	mu *sync.RWMutex
}

// Returns a new Tailer that can be used to tail the binlog.
// 'offsets' denotes the kafka offsets at which the tailer should start tailing
// the log. 'offsetkey' denotes the keygroup under which the offsets should be
// checkpointed in the plane's hangar.
func NewTailer(plane plane.Plane, topic string, offsets kafka.TopicPartitions, offsetkey []byte) (*Tailer, error) {
	stopCh := make(chan struct{})
	consumer, err := plane.KafkaConsumerFactory(fkafka.ConsumerConfig{
		Scope:        resource.NewPlaneScope(plane.ID),
		Topic:        topic,
		GroupID:      "default-nitrous-tailer",
		OffsetPolicy: fkafka.DefaultOffsetPolicy,
		Partitions:   offsets,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize kafka consumer: %w", err)
	}
	return &Tailer{
		nil,
		plane,
		consumer,
		offsetkey,
		stopCh,
		10 * time.Second, // 10s as poll timeout
		&sync.RWMutex{},
	}, nil
}

func (t *Tailer) Subscribe(p EventProcessor) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.processors = append(t.processors, p)
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
	close(t.stopCh)
}

// Start tailing the kafka binlog and forwarding events to processors.
// Note: This function blocks the caller and should be run in a goroutine. To
// stop the tailer, call Stop().
func (t *Tailer) Tail() {
	for {
		select {
		case <-t.stopCh:
			return
		default:
			ctx := context.Background()
			t.plane.Logger.Info("Waiting for new messages from binlog...")
			rawops, err := t.binlog.ReadBatch(ctx, tailer_batch, t.pollTimeout)
			if kerr, ok := err.(kafka.Error); ok && (kerr.IsFatal() || kerr.Code() == kafka.ErrUnknownTopicOrPart) {
				t.plane.Logger.Fatal("permanent error when reading from kafka", zap.Error(err))
			} else if err != nil {
				t.plane.Logger.Warn("failed to read from binlog", zap.Error(err))
				// Insert a brief sleep to avoid busy loop.
				time.Sleep(t.pollTimeout)
				continue
			} else if len(rawops) == 0 {
				t.plane.Logger.Debug("no new messages from binlog")
				// Insert a brief sleep to avoid busy loop.
				time.Sleep(t.pollTimeout)
				continue
			}
			t.plane.Logger.Debug("Got new messages from binlog", zap.Int("count", len(rawops)))
			ops := make([]*rpc.NitrousOp, len(rawops))
			for i, rawop := range rawops {
				var op rpc.NitrousOp
				err := proto.Unmarshal(rawop, &op)
				if err != nil {
					t.plane.Logger.Error("failed to unmarshal op", zap.Error(err))
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
					ks, vs, err := p.Process(ctx, ops)
					if err != nil {
						t.plane.Logger.Error("failed to process ops", zap.String("processor", p.Identity()), zap.Error(err))
						return err
					}
					updates <- update{ks, vs}
					return nil
				})
			}
			// Wait for all processors to finish and then close updates channel.
			err = eg.Wait()
			close(updates)
			if err != nil {
				t.plane.Logger.Error("one or more op processors failed", zap.Error(err))
				continue
			}
			// Consolidate all updates into one write batch.
			var keys []hangar.Key
			var vgs []hangar.ValGroup
			for up := range updates {
				keys = append(keys, up.keys...)
				vgs = append(vgs, up.vgs...)
			}
			// Save kafka offsets in the same batch for exactly-once processing.
			offs, err := t.binlog.Offsets()
			if err != nil {
				t.plane.Logger.Error("failed to get offsets", zap.Error(err))
			}
			offvg, err := offsets.EncodeOffsets(offs)
			if err != nil {
				t.plane.Logger.Error("failed to save offsets", zap.Error(err))
			}
			keys = append(keys, hangar.Key{Data: t.offsetkey})
			vgs = append(vgs, offvg)
			// Finally, write the batch to the hangar.
			err = t.plane.Store.SetMany(keys, vgs)
			if err != nil {
				t.plane.Logger.Error("failed to update store", zap.Error(err))
			}
			// Commit the offsets to the kafka binlog.
			_, err = t.binlog.CommitOffsets(offs)
			if err != nil {
				t.plane.Logger.Error("failed to commit offsets", zap.Error(err))
			}
		}
	}
}
