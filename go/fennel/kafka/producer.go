package kafka

import (
	"context"
	"fennel/resource"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
	"log"
	"time"
)

//=================================
// Kafka producer
//=================================

type remoteProducer struct {
	topic string
	*kafka.Producer
}

func (k remoteProducer) Close() error {
	return k.Close()
}

func (k remoteProducer) Teardown() error {
	return nil
}

func (k remoteProducer) Type() resource.Type {
	return resource.KafkaProducer
}

func (k remoteProducer) Log(protoMsg proto.Message) error {
	value, err := proto.Marshal(protoMsg)
	if err != nil {
		return fmt.Errorf("failed to serialize protoMsg to proto: %v", err)
	}
	kafkaMsg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &k.topic, Partition: kafka.PartitionAny},
		Value:          value,
	}
	return k.Produce(&kafkaMsg, nil)
}

var _ FProducer = remoteProducer{}

//=================================
// Config for remoteProducer
//=================================

type RemoteProducerConfig struct {
	topic           string
	recreateTopic   bool
	BootstrapServer string
	Username        string
	Password        string
}

func (conf RemoteProducerConfig) Materialize() (resource.Resource, error) {
	configmap := conf.genConfigMap()
	producer, err := kafka.NewProducer(configmap)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize kafka producer for topic [%s]: %v", conf.topic, err)
	}
	if conf.recreateTopic {
		conf.recreate()
	}
	// Delivery report handler for produced messages
	// This starts a go-routine that goes through all "delivery reports" for sends
	// as they arrive and logs if any of those are failing
	go func() {
		for e := range producer.Events() {
			if m, ok := e.(*kafka.Message); ok && m.TopicPartition.Error != nil {
				log.Printf("[ERROR] Kafka send failed. Event: %v", e.String())
			}
		}
	}()
	return remoteProducer{conf.topic, producer}, err
}

func (conf RemoteProducerConfig) recreate() error {
	// Create admin client.
	c, err := kafka.NewAdminClient(conf.genConfigMap())
	if err != nil {
		return fmt.Errorf("failed to create admin client: %v", err)
	}
	defer c.Close()

	// First, delete any existing topics of this name
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// we ignore results/errors because sometimes the topic may not exist
	_, _ = c.DeleteTopics(ctx, []string{conf.topic})

	// now recreate the topic
	results, err := c.CreateTopics(ctx, []kafka.TopicSpecification{
		{Topic: conf.topic, NumPartitions: 1},
	})
	if err != nil {
		return fmt.Errorf("failed to create topic [%s]: %v", conf.topic, err)
	}
	for _, tr := range results {
		if tr.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf(tr.Error.Error())
		}
	}
	return nil
}

func (conf RemoteProducerConfig) genConfigMap() *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers": conf.BootstrapServer,
		"sasl.username":     conf.Username,
		"sasl.password":     conf.Password,
		"security.protocol": securityProtocol,
		"sasl.mechanisms":   saslMechanism,
	}
}

var _ resource.Config = RemoteProducerConfig{}

//=================================
// Local producer/config (for tests)
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

var _ FProducer = localProducer{}

//=================================
// Config for localProducer
//=================================

type LocalProducerConfig struct {
	ch    chan []byte
	topic string
}

func (conf LocalProducerConfig) Materialize() (resource.Resource, error) {
	return localProducer{conf.topic, conf.ch}, nil
}

var _ resource.Config = LocalProducerConfig{}
