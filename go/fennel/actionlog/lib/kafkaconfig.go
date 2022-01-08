package lib

import (
	"context"
	"fennel/instance"
	"fennel/utils"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"time"
)

const (
	KAFKA_CLUSTER_ID        = "lkc-yzwjp"
	KAFKA_BOOTSTRAP_SERVER  = "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092"
	KAFKA_REST_ENDPOINT     = "https://pkc-l7pr2.ap-south-1.aws.confluent.cloud/"
	KAFKA_SECURITY_PROTOCOL = "SASL_SSL"
	KAFKA_SASL_MECHANISM    = "PLAIN"
	KAFKA_USERNAME          = "DXGQTONRSCJ7YC2W"
	KAFKA_PASSWORD          = "s1TAmKoJ7WnbJusVMPlgvKbYszD6lE50789bM1Y6aTlJNtRjThhhPR8+LeaY6Mqi"
)

var topic string = ""
var kConsumer *kafka.Consumer = nil

func KafkaConfigMap() *kafka.ConfigMap {
	return &kafka.ConfigMap{
		// connection configs.
		"bootstrap.servers": KAFKA_BOOTSTRAP_SERVER,
		"security.protocol": KAFKA_SECURITY_PROTOCOL,
		"sasl.mechanisms":   KAFKA_SASL_MECHANISM,
		"sasl.username":     KAFKA_USERNAME,
		"sasl.password":     KAFKA_PASSWORD,
	}
}

func init() {
	instance.Register(instance.Kafka, setupTopic)
	instance.Register(instance.Kafka, setupConsumer)
}

func KafkaActionTopic() string {
	return topic
}

func KafkaActionConsumer() *kafka.Consumer {
	return kConsumer
}

func setupTopic() error {
	log.Println("Setting up kafka...")
	if instance.Current() == instance.TEST {
		topicname := "test_actionlog"
		c, err := kafka.NewAdminClient(KafkaConfigMap())
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
		topic = "actionlog"
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
	kConsumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		// connection configs.
		"bootstrap.servers": KAFKA_BOOTSTRAP_SERVER,
		"security.protocol": KAFKA_SECURITY_PROTOCOL,
		"sasl.mechanisms":   KAFKA_SASL_MECHANISM,
		"sasl.username":     KAFKA_USERNAME,
		"sasl.password":     KAFKA_PASSWORD,

		// consumer specific configs.
		"group.id":          group,
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}
	kConsumer.SubscribeTopics([]string{KafkaActionTopic()}, nil)
	return nil
}
