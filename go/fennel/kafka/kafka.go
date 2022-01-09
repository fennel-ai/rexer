package kafka

import (
	"context"
	"fennel/instance"
	"fennel/utils"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"time"
)

func init() {
	instance.Register(instance.Kafka, setupTopic)
	instance.Register(instance.Kafka, setupProducer)
	instance.Register(instance.Kafka, setupConsumer)
}

func LogAction(value []byte) error {
	message := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          value,
	}
	return producer.Produce(&message, nil)
}

func ReadActionMessage() (*kafka.Message, error) {
	return consumer.ReadMessage(-1)
}

const (
	cluster_id        = "lkc-yzwjp"
	bootstrap_server  = "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092"
	rest_endpoint     = "https://pkc-l7pr2.ap-south-1.aws.confluent.cloud/"
	security_protocol = "SASL_SSL"
	sasl_mechanism    = "PLAIN"
	username          = "DXGQTONRSCJ7YC2W"
	password          = "s1TAmKoJ7WnbJusVMPlgvKbYszD6lE50789bM1Y6aTlJNtRjThhhPR8+LeaY6Mqi"
)

var topic string = ""
var consumer *kafka.Consumer = nil
var producer *kafka.Producer = nil

func kafkaConfigMap() *kafka.ConfigMap {
	return &kafka.ConfigMap{
		// connection configs.
		"bootstrap.servers": bootstrap_server,
		"security.protocol": security_protocol,
		"sasl.mechanisms":   sasl_mechanism,
		"sasl.username":     username,
		"sasl.password":     password,
	}
}

func setupTopic() error {
	log.Println("Setting up kafka...")
	if instance.Current() == instance.TEST {
		topicname := "test_actionlog"
		c, err := kafka.NewAdminClient(kafkaConfigMap())
		defer c.Close()
		if err != nil {
			return err
		}
		// first delete any existing topics of this name
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// we ignore results/errors because sometimes the topic may not exist
		_, _ = c.DeleteTopics(ctx, []string{topicname})
		// now recreate the topic
		results, err := c.CreateTopics(ctx, []kafka.TopicSpecification{
			{
				Topic:         topicname,
				NumPartitions: 1,
			},
		})
		if err != nil {
			return err
		}
		for _, tr := range results {
			if tr.Error.Code() != kafka.ErrNoError {
				return fmt.Errorf(tr.Error.Error())
			}
		}
		topic = topicname
	} else {
		topic = "data"
	}
	log.Printf("Done setting up kafka\n")
	return nil
}

func setupConsumer() error {
	group := "actionlog_tailer"
	// in test, randomizing the consumer group so there is no confusion
	// regarding offsets consumed for any given consumer
	if instance.Current() == instance.TEST {
		group = fmt.Sprintf("test_actionlog_tailer_%s", utils.RandString(5))
	}
	var err error
	consumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		// connection configs.
		"bootstrap.servers": bootstrap_server,
		"security.protocol": security_protocol,
		"sasl.mechanisms":   sasl_mechanism,
		"sasl.username":     username,
		"sasl.password":     password,

		// consumer specific configs.
		"group.id":          group,
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}
	consumer.SubscribeTopics([]string{topic}, nil)
	return nil
}

func setupProducer() error {
	var err error
	producer, err = kafka.NewProducer(kafkaConfigMap())
	if err != nil {
		return err
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
	return nil
}

func tearDown() {
	// Wait for message deliveries before shutting down
	producer.Flush(15 * 1000)
	producer.Close()
}
