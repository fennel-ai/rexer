package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/samber/mo"

	"fennel/lib/timer"
	"fennel/lib/utils/ptr"
	"fennel/resource"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
)

var (
	ErrNoPartition = errors.New("no partition assigned")
)

type RemoteConsumer struct {
	*kafka.Consumer
	resource.Scope
	topic   string
	groupid string
	conf    resource.Config
}

func (k RemoteConsumer) Name() string {
	return k.Consumer.String()
}

func (k RemoteConsumer) GroupID() string {
	return k.groupid
}

var _ FConsumer = RemoteConsumer{}

var _ resource.Resource = RemoteConsumer{}

func (k RemoteConsumer) Close() error {
	return k.Consumer.Close()
}

func (k RemoteConsumer) Type() resource.Type {
	return resource.KafkaConsumer
}

// Returns all the partitions for the consumer's topic.
func (k RemoteConsumer) GetPartitions() (kafka.TopicPartitions, error) {
	topic := k.topic
	metadata, err := k.Consumer.GetMetadata(ptr.To(topic), false, 1000 /* timeout_ms */)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic metadata: %w", err)
	}
	topicInfo, ok := metadata.Topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic %s not found", topic)
	}
	toppars := make(kafka.TopicPartitions, len(topicInfo.Partitions))
	for i := 0; i < len(topicInfo.Partitions); i++ {
		toppars[i] = kafka.TopicPartition{
			Topic:     ptr.To(topic),
			Partition: int32(i),
		}
	}
	return toppars, nil
}

func (k RemoteConsumer) Read(ctx context.Context, timeout time.Duration) ([]byte, error) {
	ctx, t := timer.Start(ctx, k.ID(), "kafka.read")
	defer t.Stop()
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
	ctx, t := timer.Start(ctx, k.ID(), "kafka.read_proto")
	defer t.Stop()
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
	seen := make(map[uint64]struct{})
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
				msgID := uint64(msg.TopicPartition.Partition<<32) + uint64(msg.TopicPartition.Offset&((1<<32)-1))
				if _, ok := seen[msgID]; !ok {
					seen[msgID] = struct{}{}
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

	if len(toppars) == 0 {
		return 0, ErrNoPartition
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

type ConsumerConfigs []string

type ConsumerConfig struct {
	Scope        resource.Scope
	GroupID      string
	OffsetPolicy string
	Topic        string
	RebalanceCb  mo.Option[func(c *kafka.Consumer, e kafka.Event) error]
	Configs      ConsumerConfigs
}

type RemoteConsumerConfig struct {
	ConsumerConfig
	BootstrapServer string
	Username        string
	Password        string
	SaslMechanism   string
	// TODO(mohit): Consider making this a required option, with every consumer setting this to the AZ ID
	// once topics are migrated to MSK cluster
	AzId mo.Option[string]
}

func (conf RemoteConsumerConfig) Materialize() (resource.Resource, error) {
	configmap := ConfigMap(conf.BootstrapServer, conf.Username, conf.Password, conf.SaslMechanism)

	topic := conf.Scope.PrefixedName(conf.Topic)
	log.Printf("Creating remote consumer for topic %s", topic)

	if err := configmap.SetKey("group.id", conf.GroupID); err != nil {
		return nil, err
	}
	if err := configmap.SetKey("auto.offset.reset", conf.OffsetPolicy); err != nil {
		return nil, err
	}

	if conf.AzId.IsPresent() {
		// Set client rack id on the consumer configuration so that the closest broker (=> broker in the same AZ)
		// is assigned to this consumer - this helps to avoid cross-AZ traffic
		//
		// see - https://aws.amazon.com/blogs/big-data/reduce-network-traffic-costs-of-your-amazon-msk-consumers-with-rack-awareness/
		//
		// NOTE: This is okay for confluent based topics as well since brokers may not have `broker.rack` configured.
		// Even if it is not, this should be a safe change since the broker assignment fallbacks to any available broker.
		// This behavior is helpful for MSK cluster as well since this helps if broker in the same AZ is down
		if err := configmap.SetKey("client.rack", conf.AzId.MustGet()); err != nil {
			return nil, err
		}
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

	// set additional consumer configurations
	for _, c := range conf.Configs {
		if err := configmap.Set(c); err != nil {
			return nil, err
		}
	}

	consumer, err := kafka.NewConsumer(configmap)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %v", err)
	}
	if conf.RebalanceCb.IsPresent() {
		if err = consumer.Subscribe(topic, conf.RebalanceCb.MustGet()); err != nil {
			return nil, fmt.Errorf("failed to subscribe to topic [%s]: %v", topic, err)
		}
	} else {
		if err = consumer.Subscribe(topic, nil); err != nil {
			return nil, fmt.Errorf("failed to subscripe to topic [%s]: %v", topic, err)
		}
	}
	return RemoteConsumer{consumer, conf.Scope, topic, conf.GroupID, nil}, nil
}

var _ resource.Config = RemoteConsumerConfig{}
