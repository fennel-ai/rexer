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
	TierID          ftypes.TierID
	BootstrapServer string
	Username        string
	Password        string
	GroupID         string
	OffsetPolicy    string
	Topic           string
}

func (conf RemoteConsumerConfig) genConfigMap() *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers": conf.BootstrapServer,
		"sasl.username":     conf.Username,
		"sasl.password":     conf.Password,
		"security.protocol": SecurityProtocol,
		"sasl.mechanisms":   SaslMechanism,
		"group.id":          conf.GroupID,
		"auto.offset.reset": conf.OffsetPolicy,
	}
}

func (conf RemoteConsumerConfig) Materialize() (resource.Resource, error) {
	if conf.TierID == 0 {
		return nil, fmt.Errorf("tier ID not initialized")
	}
	conf.Topic = fmt.Sprintf("t_%d_%s", conf.TierID, conf.Topic)
	configmap := conf.genConfigMap()
	consumer, err := kafka.NewConsumer(configmap)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %v", err)
	}
	err = consumer.Subscribe(conf.Topic, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to Topic [%s]: %v", conf.Topic, err)
	}
	return RemoteConsumer{conf.TierID, consumer, conf.Topic, conf}, nil
}

var _ resource.Config = RemoteConsumerConfig{}
