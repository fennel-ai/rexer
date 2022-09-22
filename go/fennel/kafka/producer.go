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
	resource.Scope
}

func (k RemoteProducer) LogProtoToPartition(ctx context.Context, message proto.Message, partition int32, partitionKey []byte) error {
	raw, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to serialize protoMsg to proto: %v", err)
	}
	return k.LogToPartition(ctx, raw, partition, partitionKey)
}

func (k RemoteProducer) LogToPartition(ctx context.Context, message []byte, partition int32, partitionKey []byte) error {
	kafkaMsg := kafka.Message{
		Key: partitionKey,
		TopicPartition: kafka.TopicPartition{Topic: &k.topic, Partition: partition},
		Value: message,
	}
	return k.Produce(&kafkaMsg, nil)
}

func (k RemoteProducer) Log(ctx context.Context, message []byte, partitionKey []byte) error {
	return k.LogToPartition(ctx, message, kafka.PartitionAny, partitionKey)
}

func (k RemoteProducer) LogProto(ctx context.Context, protoMsg proto.Message, partitionKey []byte) error {
	return k.LogProtoToPartition(ctx, protoMsg, kafka.PartitionAny, partitionKey)
}

func (k RemoteProducer) Flush(timeout time.Duration) error {
	if left := k.Producer.Flush(int(timeout.Milliseconds())); left > 0 {
		return fmt.Errorf("could not flush all messages, %d left unflushed", left)
	}
	return nil
}

func (k RemoteProducer) Close() error {
	if err := k.Flush(time.Second * 10); err != nil {
		return err
	}
	k.Producer.Close()
	return nil
}

func (k RemoteProducer) Type() resource.Type {
	return resource.KafkaProducer
}

var _ FProducer = RemoteProducer{}

//=================================
// Config for remoteProducer
//=================================

type ProducerConfigs []string

type RemoteProducerConfig struct {
	Topic           string
	BootstrapServer string
	Username        string
	Password        string
	SaslMechanism   string
	Scope           resource.Scope
	Configs         ProducerConfigs
}

func (conf RemoteProducerConfig) Materialize() (resource.Resource, error) {
	topic := conf.Scope.PrefixedName(conf.Topic)
	configmap := ConfigMap(conf.BootstrapServer, conf.Username, conf.Password, conf.SaslMechanism)
	for _, config := range conf.Configs {
		if err := configmap.Set(config); err != nil {
			return nil, err
		}
	}
	log.Printf("Creating remote producer for topic %s", topic)
	producer, err := kafka.NewProducer(configmap)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize kafka producer for Topic [%s]: %v", topic, err)
	}
	// record events
	go RecordEvents(producer.Events())
	return RemoteProducer{topic, producer, conf.Scope}, err
}

var _ resource.Config = RemoteProducerConfig{}
