package client

import (
	"fennel/data/lib"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// TODO: move this to client object and create a client.Close() function
var producer *kafka.Producer

func init() {
	var err error
	producer, err = kafka.NewProducer(lib.KafkaConfigMap())
	if err != nil {
		panic(err)
	}
	// Delivery report handler for produced messages
	// This starts a go-routine that goes through all "delivery reports" for sends
	// as they arrive and logs if any of those are failing
	go func() {
		for e := range producer.Events() {
			if m, ok := e.(*kafka.Message); ok && m.TopicPartition.Error != nil {
				log.Printf("[ERROR] Kafka send failed: %v", m.TopicPartition)
			}
		}
	}()
}

func closeKafka() {
	// Wait for message deliveries before shutting down
	producer.Flush(15 * 1000)
	producer.Close()
}

func send(value []byte) error {
	topic := lib.KafkaActionTopic()
	message := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          value,
	}
	return producer.Produce(&message, nil)
}
