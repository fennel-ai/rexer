package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/resource"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
)

//=================================
// Kafka producer
//=================================

type RemoteProducer struct {
	tierID ftypes.TierID
	topic  string
	*kafka.Producer
}

func (k RemoteProducer) Log(ctx context.Context, message []byte, partitionKey []byte) error {
	kafkaMsg := kafka.Message{
		Key:            partitionKey,
		TopicPartition: kafka.TopicPartition{Topic: &k.topic},
		Value:          message,
	}
	ch := make(chan error, 1)
	go func() {
		ch <- k.Produce(&kafkaMsg, nil)
	}()
	select {
	case <-ctx.Done():
		return fmt.Errorf("context timed out before logging")
	case err := <-ch:
		return err
	}
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

func (k RemoteProducer) TierID() ftypes.TierID {
	return k.tierID
}

func (k RemoteProducer) Type() resource.Type {
	return resource.KafkaProducer
}

func (k RemoteProducer) LogProto(ctx context.Context, protoMsg proto.Message, partitionKey []byte) error {
	defer timer.Start(k.tierID, "kafka.log_proto").ObserveDuration()
	value, err := proto.Marshal(protoMsg)
	if err != nil {
		return fmt.Errorf("failed to serialize protoMsg to proto: %v", err)
	}
	kafkaMsg := kafka.Message{
		Key:            partitionKey,
		TopicPartition: kafka.TopicPartition{Topic: &k.topic},
		Value:          value,
	}
	ch := make(chan error, 1)
	go func() {
		ch <- k.Produce(&kafkaMsg, nil)
	}()
	select {
	case <-ctx.Done():
		return fmt.Errorf("context timed out before logging")
	case err := <-ch:
		return err
	}
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

func (conf RemoteProducerConfig) Materialize(tierID ftypes.TierID) (resource.Resource, error) {
	if tierID == 0 {
		return nil, fmt.Errorf("tier ID not initialized")
	}
	conf.Topic = resource.TieredName(tierID, conf.Topic)

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
	return RemoteProducer{tierID, conf.Topic, producer}, err
}

var _ resource.Config = RemoteProducerConfig{}
