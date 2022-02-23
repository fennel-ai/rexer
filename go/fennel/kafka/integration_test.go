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
	topic := "testtopic"
	t.Run("integration_producer_consumer", func(t *testing.T) {
		t.Parallel()
		tierID := ftypes.TierID(rand.Uint32())
		producer := integrationProducer(t, tierID, topic)
		consumer := integrationConsumer(t, tierID, topic, "group", "earliest")
		defer teardownKafkaTopics(tierID, "topic")
		testProducerConsumer(t, producer, consumer)
	})
	t.Run("integration_read_batch", func(t *testing.T) {
		t.Parallel()
		tierID := ftypes.TierID(rand.Uint32())
		producer := integrationProducer(t, tierID, topic)
		consumer := integrationConsumer(t, tierID, topic, "group", "earliest")
		defer teardownKafkaTopics(tierID, "topic")
		testReadBatch(t, producer, consumer)
	})
	t.Run("integration_flush_commit_backlog", func(t *testing.T) {
		t.Parallel()
		tierID := ftypes.TierID(rand.Uint32())
		producer := integrationProducer(t, tierID, topic)
		consumer := integrationConsumer(t, tierID, topic, "group", "earliest")
		defer teardownKafkaTopics(tierID, "topic")
		testBacklog(t, producer, consumer)
	})
	t.Run("integration_different_consumer_groups", func(t *testing.T) {
		t.Parallel()
		tierID := ftypes.TierID(rand.Uint32())
		producer := integrationProducer(t, tierID, topic)
		consumer1 := integrationConsumer(t, tierID, topic, "group1", "earliest")
		consumer2 := integrationConsumer(t, tierID, topic, "group2", "earliest")
		defer teardownKafkaTopics(tierID, "topic")
		testDifferentConsumerGroups(t, producer, consumer1, consumer2)
	})
	t.Run("integration_same_consumer_groups", func(t *testing.T) {
		t.Parallel()
		tierID := ftypes.TierID(rand.Uint32())
		producer := integrationProducer(t, tierID, topic)
		consumer1 := integrationConsumer(t, tierID, topic, "group", "earliest")
		consumer2 := integrationConsumer(t, tierID, topic, "group", "earliest")
		defer teardownKafkaTopics(tierID, "topic")
		testSameConsumerGroup(t, producer, consumer1, consumer2)
	})
	t.Run("integration_no_auto_commit", func(t *testing.T) {
		t.Parallel()
		tierID := ftypes.TierID(rand.Uint32())
		producer := integrationProducer(t, tierID, topic)
		consumer1 := integrationConsumer(t, tierID, topic, "group", "earliest")
		consumer2 := integrationConsumer(t, tierID, topic, "group", "earliest")
		defer teardownKafkaTopics(tierID, "topic")
		testNoAutoCommit(t, producer, consumer1, consumer2)
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

	results, err := c.CreateTopics(ctx, []kafka.TopicSpecification{{Topic: name, NumPartitions: 2}})
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

func integrationProducer(t *testing.T, tierID ftypes.TierID, topic string) FProducer {
	// first create the topics
	assert.NoError(t, setupKafkaTopics(tierID, topic))

	// then create producer
	resource, err := RemoteProducerConfig{
		Topic:           resource.TieredName(tierID, topic),
		BootstrapServer: test_kafka_servers,
		Username:        kafka_username,
		Password:        kafka_password,
	}.Materialize(resource.GetTierScope(tierID))
	assert.NoError(t, err)
	producer := resource.(FProducer)
	return producer
}

func integrationConsumer(t *testing.T, tierID ftypes.TierID, topic, groupid, offsetpolicy string) FConsumer {
	resource, err := RemoteConsumerConfig{
		Topic:           resource.TieredName(tierID, topic),
		BootstrapServer: test_kafka_servers,
		Username:        kafka_username,
		Password:        kafka_password,
		GroupID:         groupid,
		OffsetPolicy:    offsetpolicy,
	}.Materialize(resource.GetTierScope(tierID))
	assert.NoError(t, err)
	consumer := resource.(FConsumer)
	return consumer
}
