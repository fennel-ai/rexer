package kafka

import (
	"fennel/instance"
	"fennel/resource"
	"fmt"
	"google.golang.org/protobuf/proto"
)

const (
	bootstrapServer  = "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092"
	username         = "DXGQTONRSCJ7YC2W"
	password         = "s1TAmKoJ7WnbJusVMPlgvKbYszD6lE50789bM1Y6aTlJNtRjThhhPR8+LeaY6Mqi"
	securityProtocol = "SASL_SSL"
	saslMechanism    = "PLAIN"
)

type FConsumer interface {
	resource.Resource
	Read(message proto.Message) error
}

type FProducer interface {
	resource.Resource
	Log(message proto.Message) error
}

func DefaultProducerConsumer(topic string) (FProducer, FConsumer, error) {
	switch instance.Current() {
	case instance.PROD:
		return prodDefault(topic)
	case instance.TEST:
		return testDefault(topic)
	default:
		return nil, nil, fmt.Errorf("invalid instance")
	}
}

func prodDefault(topic string) (FProducer, FConsumer, error) {
	producerConf := RemoteProducerConfig{
		topic:           topic,
		recreateTopic:   false,
		BootstrapServer: bootstrapServer,
		Username:        username,
		Password:        password,
	}
	producer, err := producerConf.Materialize()
	if err != nil {
		return nil, nil, err
	}
	consumerConfig := RemoteConsumerConfig{
		topic:           topic,
		BootstrapServer: bootstrapServer,
		Username:        username,
		Password:        password,
		groupID:         "mygroup",
		offsetPolicy:    "earliest",
	}
	consumer, err := consumerConfig.Materialize()
	if err != nil {
		return nil, nil, err
	}
	return producer.(FProducer), consumer.(FConsumer), nil
}

func testDefault(topic string) (FProducer, FConsumer, error) {
	ch := make(chan []byte, 1000)
	producer, err := LocalProducerConfig{ch: ch, topic: topic}.Materialize()
	if err != nil {
		return nil, nil, err
	}
	consumer, err := LocalConsumerConfig{ch: ch, topic: topic}.Materialize()
	if err != nil {
		return nil, nil, err
	}
	return producer.(FProducer), consumer.(FConsumer), nil
}
