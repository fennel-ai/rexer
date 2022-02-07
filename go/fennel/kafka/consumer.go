package kafka

import (
	"fennel/lib/ftypes"
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
	conf.Topic = TieredName(tierID, conf.Topic)
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
