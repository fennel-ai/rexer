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
	*kafka.Consumer
	resource.Scope
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

func (k RemoteConsumer) Read(ctx context.Context, timeout time.Duration) ([]byte, error) {
	defer timer.Start(ctx, k.ID(), "kafka.read").Stop()
	errCh := make(chan error)
	msgCh := make(chan []byte)
	go func() {
		kmsg, err := k.ReadMessage(timeout)
		if err != nil {
			errCh <- fmt.Errorf("failed to read msg from kafka: %v", err)
			return
		}
		msgCh <- kmsg.Value
	}()
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("context cancelled before reading")
	case err := <-errCh:
		return nil, err
	case msg := <-msgCh:
		return msg, nil
	}
}

func (k RemoteConsumer) ReadProto(ctx context.Context, pmsg proto.Message, timeout time.Duration) error {
	defer timer.Start(ctx, k.ID(), "kafka.read_proto").Stop()
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
	timer := time.NewTimer(timeout).C
	var ret [][]byte
	start := time.Now()
	// `seen`` map is used to deduplicate messages read from the same partition.
	//
	// following scenario is possible:
	// 	1. c1 gets assigned par1(with say offset=0)
	//  2. c2 reads 5 messages from par1 and adds it to `ret`
	//  3. broker initiates partition rebalance event
	// 	4. c1 now gets assigned par1, par2
	//  5. c1 will now start reading the partitions from their earliest offset - which for par1 is 0
	//  6. c1 will buffer the first 5 messages from par1 again
	//
	// TODO: Fix the possibility that a partition which was partially read by a consumer gets assigned to another
	// consumer. This might lead to multiple consumers reading the same subset of data from a particular partition.
	seen := make(map[string]struct{})
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
				toppar := msg.TopicPartition.String()
				if _, ok := seen[toppar]; !ok {
					seen[toppar] = struct{}{}
					ret = append(ret, msg.Value)
				}
			} else if kerr, ok := err.(kafka.Error); ok && kerr.Code() != kafka.ErrTimedOut {
				return nil, err
			}
		}
	}
	return ret, nil
}

// Commit commits the offsets (in a blocking manner)
func (k RemoteConsumer) Commit() (kafka.TopicPartitions, error) {
	return k.Consumer.Commit()
}

// Commit commits the given offsets (in a blocking manner)
func (k RemoteConsumer) CommitOffsets(offsets kafka.TopicPartitions) (kafka.TopicPartitions, error) {
	return k.Consumer.CommitOffsets(offsets)
}

func (k RemoteConsumer) Offsets() (kafka.TopicPartitions, error) {
	toppars, err := k.Consumer.Assignment()
	if err != nil {
		return nil, fmt.Errorf("failed to get topic assignment: %v", err)
	}
	return k.Consumer.Position(toppars)
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

type ConsumerConfig struct {
	GroupID      string
	OffsetPolicy string
	Topic        string
	// List of topic partitions to consume from. If empty, consume from broker
	// assigned partitions.
	Partitions kafka.TopicPartitions
}

type RemoteConsumerConfig struct {
	ConsumerConfig
	Scope           resource.Scope
	BootstrapServer string
	Username        string
	Password        string
}

func (conf RemoteConsumerConfig) Materialize() (resource.Resource, error) {
	configmap := ConfigMap(conf.BootstrapServer, conf.Username, conf.Password)

	topic := conf.Scope.PrefixedName(conf.Topic)

	if err := configmap.SetKey("group.id", conf.GroupID); err != nil {
		return nil, err
	}
	if err := configmap.SetKey("auto.offset.reset", conf.OffsetPolicy); err != nil {
		return nil, err
	}

	// Disable auto committing so we can have tighter control over it
	if err := configmap.SetKey("enable.auto.commit", false); err != nil {
		return nil, err
	}

	// Enable application to receive rebalance event notifications.
	// This is required for the consumer to be able to assign specific topic
	// partitions to itself instead of the ones assigned by the broker(s).
	if err := configmap.SetKey("go.application.rebalance.enable", true); err != nil {
		return nil, err
	}

	consumer, err := kafka.NewConsumer(configmap)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %v", err)
	}
	rebalanceCb := func(c *kafka.Consumer, e kafka.Event) error {
		log.Printf("[%s:%s:%s]Got kafka partition rebalance event: %v", topic, conf.GroupID, c.String(), e.String())
		switch event := e.(type) {
		case kafka.AssignedPartitions:
			if len(conf.Partitions) > 0 && len(event.Partitions) > 0 {
				log.Printf("Assigning partitions to self[%s]: %v", c.String(), conf.Partitions)
				err := c.Assign(conf.Partitions)
				if err != nil {
					log.Fatalf("Failed to assign partitions: %v", err)
				}
			}
		}
		return nil
	}
	err = consumer.Subscribe(topic, rebalanceCb)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to topic [%s]: %v", topic, err)
	}
	return RemoteConsumer{consumer, conf.Scope, topic, conf.GroupID, nil}, nil
}

var _ resource.Config = RemoteConsumerConfig{}
