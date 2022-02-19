//go:build integration

package kafka

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
	"fennel/resource"
)

const (
	test_kafka_servers = "pkc-pgq85.us-west-2.aws.confluent.cloud:9092"
	kafka_username     = "PQESAHSX5EUQJPIV"
	kafka_password     = "EDjraEtpIjYQBv9WQ2QINnZZcExKUtm6boweLCsQ5gv3arWSk+VHyD1kfjJ+p382"
)

func TestIntegration(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	t.Run("integration_producer_consumer", func(t *testing.T) {
		t.Parallel()
		tierID := ftypes.TierID(rand.Uint32())
		producer, consumer := integrationProducerConsumer(t, tierID, "topic", "group", "earliest")
		defer teardownKafkaTopics(tierID, "topic")
		testProducerConsumer(t, producer, consumer)
	})
	t.Run("integration_read_batch", func(t *testing.T) {
		t.Parallel()
		tierID := ftypes.TierID(rand.Uint32())
		producer, consumer := integrationProducerConsumer(t, tierID, "topic", "group", "earliest")
		defer teardownKafkaTopics(tierID, "topic")
		testReadBatch(t, producer, consumer)
	})
	t.Run("integration_flush_commit_backlog", func(t *testing.T) {
		t.Parallel()
		tierID := ftypes.TierID(rand.Uint32())
		producer, consumer := integrationProducerConsumer(t, tierID, "topic", "group", "earliest")
		defer teardownKafkaTopics(tierID, "topic")
		testBacklog(t, producer, consumer)
	})
}

func setupKafkaTopics(tierID ftypes.TierID, topic string) error {
	name := resource.TieredName(tierID, topic)
	// Create admin client
	c, err := kafka.NewAdminClient(ConfigMap(test_kafka_servers, kafka_username, kafka_password))
	if err != nil {
		return fmt.Errorf("failed to create admin client: %v", err)
	}
	defer c.Close()

	// now create the Topic
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results, err := c.CreateTopics(ctx, []kafka.TopicSpecification{{Topic: name, NumPartitions: 1}})
	if err != nil {
		return fmt.Errorf("failed to create topics: %v", err)
	}
	for _, tr := range results {
		if tr.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf(tr.Error.Error())
		}
	}
	return nil
}

func teardownKafkaTopics(tierID ftypes.TierID, topic string) error {
	name := resource.TieredName(tierID, topic)
	// Create admin client.
	c, err := kafka.NewAdminClient(ConfigMap(test_kafka_servers, kafka_username, kafka_password))
	if err != nil {
		return fmt.Errorf("failed to create admin client: %v", err)
	}
	defer c.Close()

	// delete the Topic
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = c.DeleteTopics(ctx, []string{name})
	return err
}

func integrationProducerConsumer(t *testing.T, tierID ftypes.TierID, topic, groupid, offsetpolicy string) (FProducer, FConsumer) {
	// first create the topics
	assert.NoError(t, setupKafkaTopics(tierID, topic))

	// then create producer/consumer
	resource, err := RemoteProducerConfig{
		Topic:           topic,
		BootstrapServer: test_kafka_servers,
		Username:        kafka_username,
		Password:        kafka_password,
	}.Materialize(tierID)
	assert.NoError(t, err)
	producer := resource.(FProducer)

	resource, err = RemoteConsumerConfig{
		Topic:           topic,
		BootstrapServer: test_kafka_servers,
		Username:        kafka_username,
		Password:        kafka_password,
		GroupID:         groupid,
		OffsetPolicy:    offsetpolicy,
	}.Materialize(tierID)
	assert.NoError(t, err)
	consumer := resource.(FConsumer)
	return producer, consumer
}
