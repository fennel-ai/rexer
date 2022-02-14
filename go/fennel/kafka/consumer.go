package kafka

import (
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/resource"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
)

type RemoteConsumer struct {
	tierID ftypes.TierID
	*kafka.Consumer
	topic string
	conf  resource.Config
}

var _ FConsumer = RemoteConsumer{}

func (k RemoteConsumer) TierID() ftypes.TierID {
	return k.tierID
}

var _ resource.Resource = RemoteConsumer{}

func (k RemoteConsumer) Close() error {
	return nil
}

func (k RemoteConsumer) Type() resource.Type {
	return resource.KafkaConsumer
}

func (k RemoteConsumer) Read(pmsg proto.Message) error {
	defer timer.Start(k.tierID, "kafka.read").ObserveDuration()
	kmsg, err := k.ReadMessage(-1)
	if err != nil {
		return fmt.Errorf("failed to read msg from kafka: %v", err)
	}
	err = proto.Unmarshal(kmsg.Value, pmsg)
	if err != nil {
		return fmt.Errorf("failed to parse msg from kafka to action: %v", err)
	}
	return nil
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
	defer timer.Start(k.tierID, "kafka.backlog").ObserveDuration()
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

func (conf RemoteConsumerConfig) Materialize(tierID ftypes.TierID) (resource.Resource, error) {
	if tierID == 0 {
		return nil, fmt.Errorf("tier ID not initialized")
	}
	conf.Topic = resource.TieredName(tierID, conf.Topic)
	configmap := ConfigMap(conf.BootstrapServer, conf.Username, conf.Password)

	if err := configmap.SetKey("group.id", conf.GroupID); err != nil {
		return nil, err
	}
	if err := configmap.SetKey("auto.offset.reset", conf.OffsetPolicy); err != nil {
		return nil, err
	}
	consumer, err := kafka.NewConsumer(configmap)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %v", err)
	}
	err = consumer.Subscribe(conf.Topic, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to Topic [%s]: %v", conf.Topic, err)
	}
	return RemoteConsumer{tierID, consumer, conf.Topic, conf}, nil
}

var _ resource.Config = RemoteConsumerConfig{}
