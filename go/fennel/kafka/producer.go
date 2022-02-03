package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"fennel/resource"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
)

//=================================
// Kafka producer
//=================================

type RemoteProducer struct {
	topic string
	*kafka.Producer
}

func (k RemoteProducer) Close() error {
	return k.Close()
}

func (k RemoteProducer) Teardown() error {
	return nil
}

func (k RemoteProducer) Type() resource.Type {
	return resource.KafkaProducer
}

func (k RemoteProducer) Log(protoMsg proto.Message) error {
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

var _ FProducer = RemoteProducer{}

//=================================
// Config for remoteProducer
//=================================

type RemoteProducerConfig struct {
	Topic           string
	RecreateTopic   bool
	BootstrapServer string
	Username        string
	Password        string
}

func (conf RemoteProducerConfig) Materialize() (resource.Resource, error) {
	configmap := conf.genConfigMap()
	producer, err := kafka.NewProducer(configmap)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize kafka producer for Topic [%s]: %v", conf.Topic, err)
	}
	if conf.RecreateTopic {
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
	return RemoteProducer{conf.Topic, producer}, err
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
	// we ignore results/errors because sometimes the Topic may not exist
	_, _ = c.DeleteTopics(ctx, []string{conf.Topic})

	// now recreate the Topic
	results, err := c.CreateTopics(ctx, []kafka.TopicSpecification{
		{Topic: conf.Topic, NumPartitions: 1},
	})
	if err != nil {
		return fmt.Errorf("failed to create Topic [%s]: %v", conf.Topic, err)
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
		"security.protocol": SecurityProtocol,
		"sasl.mechanisms":   SaslMechanism,
	}
}

var _ resource.Config = RemoteProducerConfig{}
