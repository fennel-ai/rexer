package test

import (
	"fennel/kafka"
	"fennel/resource"
	"google.golang.org/protobuf/proto"
)

func DefaultProducerConsumer(topic string) (kafka.FProducer, kafka.FConsumer, error) {
	ch := make(chan []byte, 1000)
	producer, err := localProducerConfig{ch: ch, topic: topic}.Materialize()
	if err != nil {
		return nil, nil, err
	}
	consumer, err := localConsumerConfig{Channel: ch, Topic: topic}.Materialize()
	if err != nil {
		return nil, nil, err
	}
	return producer.(kafka.FProducer), consumer.(kafka.FConsumer), nil
}

//=================================
// Local consumer(for tests)
//=================================

type localConsumer struct {
	Topic   string
	Channel <-chan []byte
}

func (l localConsumer) Read(message proto.Message) error {
	ser := <-l.Channel
	err := proto.Unmarshal(ser, message)
	return err
}

func (l localConsumer) Close() error {
	return nil
}

func (l localConsumer) Teardown() error {
	return nil
}

func (l localConsumer) Type() resource.Type {
	return resource.KafkaConsumer
}

var _ kafka.FConsumer = localConsumer{}

//=================================
// Config for localConsumer
//=================================

type localConsumerConfig struct {
	Channel chan []byte
	Topic   string
}

func (l localConsumerConfig) Materialize() (resource.Resource, error) {
	return localConsumer{l.Topic, l.Channel}, nil
}

var _ resource.Config = localConsumerConfig{}

//=================================
// Local producer(for tests)
//=================================

type localProducer struct {
	topic string
	ch    chan<- []byte
}

func (l localProducer) Close() error {
	close(l.ch)
	return nil
}

func (l localProducer) Teardown() error {
	return nil
}

func (l localProducer) Type() resource.Type {
	return resource.KafkaProducer
}

func (l localProducer) Log(protoMsg proto.Message) error {
	ser, err := proto.Marshal(protoMsg)
	if err != nil {
		return err
	}
	l.ch <- ser
	return nil
}

var _ kafka.FProducer = localProducer{}

//=================================
// Config for localProducer
//=================================

type localProducerConfig struct {
	ch    chan []byte
	topic string
}

func (conf localProducerConfig) Materialize() (resource.Resource, error) {
	return localProducer{conf.topic, conf.ch}, nil
}

var _ resource.Config = localProducerConfig{}
