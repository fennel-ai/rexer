package test

import (
	"fennel/kafka"
	"fennel/lib/ftypes"
	"fennel/resource"
	"fmt"
	"google.golang.org/protobuf/proto"
)

func defaultProducerConsumer(tierID ftypes.TierID, topic string) (kafka.FProducer, kafka.FConsumer, error) {
	ch := make(chan []byte, 1000)
	producer, err := localProducerConfig{tierID: tierID, ch: ch, topic: topic}.Materialize()
	if err != nil {
		return nil, nil, err
	}
	consumer, err := localConsumerConfig{tierID: tierID, Channel: ch, Topic: topic}.Materialize()
	if err != nil {
		return nil, nil, err
	}
	return producer.(kafka.FProducer), consumer.(kafka.FConsumer), nil
}

//=================================
// Local consumer(for tests)
//=================================

type localConsumer struct {
	tierID  ftypes.TierID
	Topic   string
	Channel <-chan []byte
}

func (l localConsumer) TierID() ftypes.TierID {
	return l.tierID
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
	tierID  ftypes.TierID
	Channel chan []byte
	Topic   string
}

func (l localConsumerConfig) Materialize() (resource.Resource, error) {
	if l.tierID == 0 {
		return nil, fmt.Errorf("tier ID not initialized")
	}
	return localConsumer{l.tierID, l.Topic, l.Channel}, nil
}

var _ resource.Config = localConsumerConfig{}

//=================================
// Local producer(for tests)
//=================================

type localProducer struct {
	tierID ftypes.TierID
	topic  string
	ch     chan<- []byte
}

func (l localProducer) TierID() ftypes.TierID {
	return l.tierID
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
	tierID ftypes.TierID
	ch     chan []byte
	topic  string
}

func (conf localProducerConfig) Materialize() (resource.Resource, error) {
	if conf.tierID == 0 {
		return nil, fmt.Errorf("tier ID not initialized")
	}
	return localProducer{conf.tierID, conf.topic, conf.ch}, nil
}

var _ resource.Config = localProducerConfig{}
