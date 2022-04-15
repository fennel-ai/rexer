package kafka

import (
	"context"
	"fmt"
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
		TopicPartition: kafka.TopicPartition{Topic: &k.topic},
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
	defer timer.Start(ctx, k.ID(), "kafka.log_proto").Stop()
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

type RemoteProducerConfig struct {
	Topic           string
	BootstrapServer string
	Username        string
	Password        string
	Scope           resource.Scope
}

func (conf RemoteProducerConfig) Materialize() (resource.Resource, error) {
	configmap := ConfigMap(conf.BootstrapServer, conf.Username, conf.Password)
	producer, err := kafka.NewProducer(configmap)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize kafka producer for Topic [%s]: %v", conf.Topic, err)
	}
	// record events
	go RecordEvents(producer.Events())
	return RemoteProducer{conf.Topic, producer, conf.Scope}, err
}

var _ resource.Config = RemoteProducerConfig{}
