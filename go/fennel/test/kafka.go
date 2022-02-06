package test

import (
	"context"
	fkafka "fennel/kafka"
	"fennel/lib/ftypes"
	"fennel/resource"
	"fennel/tier"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
	"time"
)

const (
	test_kafka_servers = "pkc-pgq85.us-west-2.aws.confluent.cloud:9092"
	kafka_username     = "PQESAHSX5EUQJPIV"
	kafka_password     = "EDjraEtpIjYQBv9WQ2QINnZZcExKUtm6boweLCsQ5gv3arWSk+VHyD1kfjJ+p382"
)

func createKafka(tierID ftypes.TierID, integration bool) (map[string]fkafka.FProducer, map[string]fkafka.FConsumer, error) {
	producers := make(map[string]fkafka.FProducer, 0)
	consumers := make(map[string]fkafka.FConsumer, 0)
	var err error
	if integration {
		if err := setupKafkaTopics(tierID, fkafka.ALL_TOPICS); err != nil {
			return nil, nil, err
		}
		producers, consumers, err = tier.CreateKafka(tierID, test_kafka_servers, kafka_username, kafka_password)
	} else {
		for _, topic := range fkafka.ALL_TOPICS {
			name := fkafka.TieredName(tierID, topic)
			kProducer, kConsumer, err := mockProducerConsumer(tierID, name)
			if err != nil {
				return nil, nil, err
			}
			producers[topic] = kProducer
			consumers[topic] = kConsumer
		}
	}
	return producers, consumers, err
}

func mockProducerConsumer(tierID ftypes.TierID, topic string) (fkafka.FProducer, fkafka.FConsumer, error) {
	fmt.Printf("coming to mock kafka creator\n")
	ch := make(chan []byte, 1000)
	producer, err := localProducerConfig{tierID: tierID, ch: ch, topic: topic}.Materialize()
	if err != nil {
		return nil, nil, err
	}
	consumer, err := localConsumerConfig{tierID: tierID, Channel: ch, Topic: topic}.Materialize()
	if err != nil {
		return nil, nil, err
	}
	return producer.(fkafka.FProducer), consumer.(fkafka.FConsumer), nil
}

func setupKafkaTopics(tierID ftypes.TierID, topics []string) error {
	fmt.Printf("going to setup kafka topics\n")
	names := make([]string, len(topics))
	for i, topic := range topics {
		names[i] = fkafka.TieredName(tierID, topic)
	}
	// Create admin client
	c, err := kafka.NewAdminClient(fkafka.ConfigMap(test_kafka_servers, kafka_username, kafka_password))
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

func teardownKafkaTopics(tierID ftypes.TierID, topics []string) error {
	fmt.Printf("going to tear down kafka topics\n")
	names := make([]string, len(topics))
	for i, topic := range topics {
		names[i] = fkafka.TieredName(tierID, topic)
	}
	// Create admin client.
	c, err := kafka.NewAdminClient(fkafka.ConfigMap(test_kafka_servers, kafka_username, kafka_password))
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

var _ fkafka.FConsumer = localConsumer{}

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
