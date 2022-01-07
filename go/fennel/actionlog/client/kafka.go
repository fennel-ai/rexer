package client

import (
	"fennel/actionlog/lib"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

// TODO: move this to client object and create a client.Close() function
var producer *kafka.Producer

func init() {
	var err error
	producer, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": lib.KAFKA_BOOTSTRAP_SERVER})
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

func send(message []byte) {
	topic := lib.KAFKA_TOPIC
	producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          message,
	}, nil)
}
