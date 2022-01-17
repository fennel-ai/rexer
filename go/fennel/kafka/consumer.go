package kafka

import (
	"fennel/resource"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
)

type RemoteConsumer struct {
	*kafka.Consumer
	topic string
	conf  resource.Config
}

var _ resource.Resource = RemoteConsumer{}

func (k RemoteConsumer) Close() error {
	return nil
}

func (k RemoteConsumer) Teardown() error {
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
	groupID         string
	offsetPolicy    string
	topic           string
}

func (conf RemoteConsumerConfig) genConfigMap() *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers": conf.BootstrapServer,
		"sasl.username":     conf.Username,
		"sasl.password":     conf.Password,
		"security.protocol": securityProtocol,
		"sasl.mechanisms":   saslMechanism,
		"group.id":          conf.groupID,
		"auto.offset.reset": conf.offsetPolicy,
	}
}

func (conf RemoteConsumerConfig) Materialize() (resource.Resource, error) {
	configmap := conf.genConfigMap()
	consumer, err := kafka.NewConsumer(configmap)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %v", err)
	}
	err = consumer.Subscribe(conf.topic, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to topic [%s]: %v", conf.topic, err)
	}
	return RemoteConsumer{consumer, conf.topic, conf}, nil
}

var _ resource.Config = RemoteConsumerConfig{}

type LocalConsumer struct {
	topic string
	ch    <-chan []byte
}

func (l LocalConsumer) Read(message proto.Message) error {
	ser := <-l.ch
	err := proto.Unmarshal(ser, message)
	return err
}

func (l LocalConsumer) Close() error {
	return nil
}

func (l LocalConsumer) Teardown() error {
	return nil
}

func (l LocalConsumer) Type() resource.Type {
	return resource.KafkaConsumer
}

var _ FConsumer = LocalConsumer{}

type LocalConsumerConfig struct {
	ch    chan []byte
	topic string
}

func (l LocalConsumerConfig) Materialize() (resource.Resource, error) {
	return LocalConsumer{l.topic, l.ch}, nil
}

var _ resource.Config = LocalConsumerConfig{}
