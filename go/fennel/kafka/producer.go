package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"fennel/lib/timer"
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

func (k RemoteProducer) Log(_ context.Context, message []byte, partitionKey []byte) error {
	kafkaMsg := kafka.Message{
		Key:            partitionKey,
		TopicPartition: kafka.TopicPartition{Topic: &k.topic, Partition: kafka.PartitionAny},
		Value:          message,
	}
	return k.Produce(&kafkaMsg, nil)
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

func (k RemoteProducer) LogProto(ctx context.Context, protoMsg proto.Message, partitionKey []byte) error {
	defer timer.Start("kafka.log_proto").Stop()
	raw, err := proto.Marshal(protoMsg)
	if err != nil {
		return fmt.Errorf("failed to serialize protoMsg to proto: %v", err)
	}
	return k.Log(ctx, raw, partitionKey)
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
	Scope           resource.Scope
	Configs         ProducerConfigs
}

func (conf RemoteProducerConfig) Materialize() (resource.Resource, error) {
	topic := conf.Scope.PrefixedName(conf.Topic)
	configmap := ConfigMap(conf.BootstrapServer, conf.Username, conf.Password)
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
