package kafka

import (
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/resource"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/protobuf/proto"
	"log"
)

//=================================
// Kafka producer
//=================================

type RemoteProducer struct {
	tierID ftypes.TierID
	topic  string
	*kafka.Producer
}

func (k RemoteProducer) Close() error {
	return k.Close()
}

func (k RemoteProducer) TierID() ftypes.TierID {
	return k.tierID
}

func (k RemoteProducer) Type() resource.Type {
	return resource.KafkaProducer
}

func (k RemoteProducer) Log(protoMsg proto.Message) error {
	defer timer.Start(k.tierID, "kafka.log").ObserveDuration()
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
