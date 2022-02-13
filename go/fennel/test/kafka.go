package test

import (
	"context"
	fkafka "fennel/kafka"
	"fennel/lib/ftypes"
	"fennel/resource"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
)

func createMockKafka(tierID ftypes.TierID) (map[string]fkafka.FProducer, map[string]fkafka.FConsumer, error) {
	producers := make(map[string]fkafka.FProducer, 0)
	consumers := make(map[string]fkafka.FConsumer, 0)
	for _, topic := range fkafka.ALL_TOPICS {
		name := resource.TieredName(tierID, topic)
		kProducer, kConsumer, err := mockProducerConsumer(tierID, name)
		if err != nil {
			return nil, nil, err
		}
		producers[topic] = kProducer
		consumers[topic] = kConsumer
	}
	return producers, consumers, nil
}

func mockProducerConsumer(tierID ftypes.TierID, topic string) (fkafka.FProducer, fkafka.FConsumer, error) {
	ch := make(chan []byte, 1000)
	producer, err := localProducerConfig{ch: ch, topic: topic}.Materialize(tierID)
	if err != nil {
		return nil, nil, err
	}
	consumer, err := localConsumerConfig{Channel: ch, Topic: topic}.Materialize(tierID)
	if err != nil {
		return nil, nil, err
	}
	return producer.(fkafka.FProducer), consumer.(fkafka.FConsumer), nil
}

func setupKafkaTopics(tierID ftypes.TierID, host, username, password string, topics []string) error {
	names := make([]string, len(topics))
	for i, topic := range topics {
		names[i] = resource.TieredName(tierID, topic)
	}
	// Create admin client
	c, err := kafka.NewAdminClient(fkafka.ConfigMap(host, username, password))
	if err != nil {
		return fmt.Errorf("failed to create admin client: %v", err)
	}
	defer c.Close()

	// now create the topics
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	specs := make([]kafka.TopicSpecification, len(names))
	for i, name := range names {
		specs[i] = kafka.TopicSpecification{
			Topic:             name,
			NumPartitions:     1,
			ReplicationFactor: 0,
		}
	}
	results, err := c.CreateTopics(ctx, specs)
	if err != nil {
		return fmt.Errorf("failed to create topics: %v", err)
	}
	for _, tr := range results {
		if tr.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf(tr.Error.Error())
		}
	}
	return nil
}

func teardownKafkaTopics(tierID ftypes.TierID, host, username, password string, topics []string) error {
	names := make([]string, len(topics))
	for i, topic := range topics {
		names[i] = resource.TieredName(tierID, topic)
	}
	// Create admin client.
	c, err := kafka.NewAdminClient(fkafka.ConfigMap(host, username, password))
	if err != nil {
		return fmt.Errorf("failed to create admin client: %v", err)
	}
	defer c.Close()

	// delete any existing topics of these names
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = c.DeleteTopics(ctx, names)
	return err
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

func (l localConsumer) Type() resource.Type {
	return resource.KafkaConsumer
}

func (l localConsumer) Backlog() (int, error) {
	return len(l.Channel), nil
}

var _ fkafka.FConsumer = localConsumer{}

//=================================
// Config for localConsumer
//=================================

type localConsumerConfig struct {
	Channel chan []byte
	Topic   string
}

func (l localConsumerConfig) Materialize(tierID ftypes.TierID) (resource.Resource, error) {
	if tierID == 0 {
		return nil, fmt.Errorf("tier ID not initialized")
	}
	return localConsumer{tierID, l.Topic, l.Channel}, nil
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

var _ fkafka.FProducer = localProducer{}

//=================================
// Config for localProducer
//=================================

type localProducerConfig struct {
	ch    chan []byte
	topic string
}

func (conf localProducerConfig) Materialize(tierID ftypes.TierID) (resource.Resource, error) {
	if tierID == 0 {
		return nil, fmt.Errorf("tier ID not initialized")
	}
	return localProducer{tierID, conf.topic, conf.ch}, nil
}

var _ resource.Config = localProducerConfig{}
