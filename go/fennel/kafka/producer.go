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
	scope resource.Scope
	topic string
	*kafka.Producer
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
	defer timer.Start(ctx, k.scope.GetTierID(), "kafka.log_proto").Stop()
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
}

func (conf RemoteProducerConfig) Materialize(scope resource.Scope) (resource.Resource, error) {
	configmap := ConfigMap(conf.BootstrapServer, conf.Username, conf.Password)
	producer, err := kafka.NewProducer(configmap)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize kafka producer for Topic [%s]: %v", conf.Topic, err)
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
	return RemoteProducer{scope, conf.Topic, producer}, err
}

var _ resource.Config = RemoteProducerConfig{}
