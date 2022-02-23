package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"fennel/lib/timer"
	"fennel/resource"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
)

type RemoteConsumer struct {
	scope resource.Scope
	*kafka.Consumer
	topic   string
	groupid string
	conf    resource.Config
}

func (k RemoteConsumer) GroupID() string {
	return k.groupid
}

var _ FConsumer = RemoteConsumer{}

var _ resource.Resource = RemoteConsumer{}

func (k RemoteConsumer) Close() error {
	k.Consumer.Close()
	return nil
}

func (k RemoteConsumer) Type() resource.Type {
	return resource.KafkaConsumer
}

func (k RemoteConsumer) ReadProto(ctx context.Context, pmsg proto.Message, timeout time.Duration) error {
	defer timer.Start(k.scope.GetTierID(), "kafka.read_proto").ObserveDuration()
	ch := make(chan error)
	go func() {
		kmsg, err := k.ReadMessage(timeout)
		if err != nil {
			ch <- fmt.Errorf("failed to read msg from kafka: %v", err)
			return
		}
		err = proto.Unmarshal(kmsg.Value, pmsg)
		if err != nil {
			ch <- fmt.Errorf("failed to parse msg from kafka to action: %v", err)
			return
		}
		ch <- nil
	}()
	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled before reading")
	case err := <-ch:
		return err
	}
}

// ReadBatch polls over Kafka and keeps reading messages until either it has read 'upto' messages
// or timeout time has elapsed
func (k RemoteConsumer) ReadBatch(ctx context.Context, upto int, timeout time.Duration) ([][]byte, error) {
	if timeout < 0 {
		return nil, fmt.Errorf("read batch timeout can not be negative")
	}
	timer := time.Tick(timeout)
	ret := make([][]byte, 0)
	start := time.Now()
	for len(ret) < upto {
		select {
		case <-timer:
			return ret, nil
		case <-ctx.Done():
			return ret, nil
		default:
			t := timeout - time.Since(start)
			// while this is unlikely to happen, still being cautious
			if t < 0 {
				return ret, nil
			}

			msg, err := k.ReadMessage(t)
			if err == nil {
				ret = append(ret, msg.Value)
			} else if kerr, ok := err.(kafka.Error); ok && kerr.Code() != kafka.ErrTimedOut {
				return nil, err
			}
		}
	}
	return ret, nil
}

// Commit commits the offsets (in a blocking manner)
func (k RemoteConsumer) Commit() error {
	_, err := k.Consumer.Commit()
	return err
}

// AsyncCommit commits the offsets but does so in an async manner without blocking
// and returns a channel of errors which the caller can use to check the error status
func (k RemoteConsumer) AsyncCommit() chan error {
	ret := make(chan error)
	go func() {
		defer close(ret)
		ret <- k.Commit()
	}()
	return ret
}

// Backlog returns the combined total of "lag" all topic partitions have that
// this consumer consumes from. For example, if this consumer is consuming from
// topic "foo" and is assigned to partitions 0, 2, and 3, then the backlog will
// be the log-end offset minus the current offset for all three partitions,
// added together.
// Original: https://github.com/confluentinc/confluent-kafka-go/issues/201#issue-330947997
// An alternate implementation is sketched here:
// https://github.com/confluentinc/confluent-kafka-go/issues/690#issuecomment-932810037.
func (k RemoteConsumer) Backlog() (int, error) {
	defer timer.Start(k.scope.GetTierID(), "kafka.backlog").ObserveDuration()
	var n int

	// Get the current assigned topic partitions.
	toppars, err := k.Assignment()
	if err != nil {
		return n, err
	}

	// Get the current offset for each partition, assigned to this consumer group.
	toppars, err = k.Committed(toppars, 5000 /* timeoutMs */)
	if err != nil {
		return n, err
	}

	// Loop over the topic partitions, get the high watermark for each toppar, and
	// subtract the current offset from that number, to get the total "lag". We
	// combine this value for each toppar to get the final backlog integer.
	var l, h int64
	for i := range toppars {
		l, h, err = k.QueryWatermarkOffsets(*toppars[i].Topic, toppars[i].Partition, 5000 /* timeoutMs */)
		if err != nil {
			return n, err
		}

		o := int64(toppars[i].Offset)
		if toppars[i].Offset == kafka.OffsetInvalid {
			o = l
		}

		n = n + int(h-o)
	}

	return n, nil
}

type RemoteConsumerConfig struct {
	BootstrapServer string
	Username        string
	Password        string
	GroupID         string
	OffsetPolicy    string
	Topic           string
}

func (conf RemoteConsumerConfig) Materialize(scope resource.Scope) (resource.Resource, error) {
	configmap := ConfigMap(conf.BootstrapServer, conf.Username, conf.Password)

	if err := configmap.SetKey("group.id", conf.GroupID); err != nil {
		return nil, err
	}
	if err := configmap.SetKey("auto.offset.reset", conf.OffsetPolicy); err != nil {
		return nil, err
	}
	// disable auto committing so we can have tighter control over it
	if err := configmap.SetKey("enable.auto.commit", false); err != nil {
		return nil, err
	}
	consumer, err := kafka.NewConsumer(configmap)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %v", err)
	}
	rebalanceCb := func(_ *kafka.Consumer, e kafka.Event) error {
		log.Printf("Got kafka partition rebalance event: %v", e.String())
		return nil
	}
	err = consumer.Subscribe(conf.Topic, rebalanceCb)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to topic [%s]: %v", conf.Topic, err)
	}
	return RemoteConsumer{scope, consumer, conf.Topic, conf.GroupID, conf}, nil
}

var _ resource.Config = RemoteConsumerConfig{}
