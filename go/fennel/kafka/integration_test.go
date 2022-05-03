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
	"fennel/lib/utils"
	"fennel/resource"
)

const (
	test_kafka_servers = "pkc-pgq85.us-west-2.aws.confluent.cloud:9092"
	kafka_username     = "HWGB3CSLWYNXWNA3"
	kafka_password     = "t7SYuJa4OsQI600/c4x8IBppm6zvPHevjWNC0klU501UViMydaeW0BqsEt+xFSxw"
)

func TestIntegration(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	topic := "testtopic"
	t.Run("integration_producer_consumer", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		producer := integrationProducer(t, scope, topic)
		consumer := integrationConsumer(t, scope, topic, utils.RandString(5), DefaultOffsetPolicy)
		defer teardownKafkaTopics(scope, topic)
		testProducerConsumer(t, producer, consumer)
	})
	t.Run("integration_producer_consumer_proto", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		producer := integrationProducer(t, scope, topic)
		consumer := integrationConsumer(t, scope, topic, utils.RandString(5), DefaultOffsetPolicy)
		defer teardownKafkaTopics(scope, topic)
		testProducerConsumerProto(t, producer, consumer)
	})
	t.Run("integration_read_batch", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		producer := integrationProducer(t, scope, topic)
		consumer := integrationConsumer(t, scope, topic, utils.RandString(5), DefaultOffsetPolicy)
		defer teardownKafkaTopics(scope, topic)
		testReadBatch(t, producer, consumer)
	})
	t.Run("integration_flush_commit_backlog", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		producer := integrationProducer(t, scope, topic)
		consumer := integrationConsumer(t, scope, topic, utils.RandString(5), DefaultOffsetPolicy)
		defer teardownKafkaTopics(scope, topic)
		testBacklog(t, producer, consumer)
	})
	t.Run("integration_different_consumer_groups", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		producer := integrationProducer(t, scope, topic)
		consumer1 := integrationConsumer(t, scope, topic, utils.RandString(5), DefaultOffsetPolicy)
		consumer2 := integrationConsumer(t, scope, topic, utils.RandString(5), DefaultOffsetPolicy)
		defer teardownKafkaTopics(scope, topic)
		testDifferentConsumerGroups(t, producer, consumer1, consumer2)
	})
	t.Run("integration_same_consumer_groups", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		group := utils.RandString(5)
		producer := integrationProducer(t, scope, topic)
		consumer1 := integrationConsumer(t, scope, topic, group, DefaultOffsetPolicy)
		consumer2 := integrationConsumer(t, scope, topic, group, DefaultOffsetPolicy)
		defer teardownKafkaTopics(scope, topic)
		testSameConsumerGroup(t, producer, consumer1, consumer2)
	})
	t.Run("integration_no_auto_commit", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		group := utils.RandString(5)
		producer := integrationProducer(t, scope, topic)
		consumer1 := integrationConsumer(t, scope, topic, group, DefaultOffsetPolicy)
		consumer2 := integrationConsumer(t, scope, topic, group, DefaultOffsetPolicy)
		defer teardownKafkaTopics(scope, topic)
		testNoAutoCommit(t, producer, consumer1, consumer2)
	})
}

func setupKafkaTopics(scope resource.Scope, topic string) error {
	name := scope.PrefixedName(topic)
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

func teardownKafkaTopics(scope resource.Scope, topic string) error {
	name := scope.PrefixedName(topic)
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

func integrationProducer(t *testing.T, scope resource.Scope, topic string) FProducer {
	// first create the topics
	assert.NoError(t, setupKafkaTopics(scope, topic))

	// then create producer
	resource, err := RemoteProducerConfig{
		Topic:           scope.PrefixedName(topic),
		BootstrapServer: test_kafka_servers,
		Username:        kafka_username,
		Password:        kafka_password,
		Scope:           scope,
	}.Materialize()
	assert.NoError(t, err)
	producer := resource.(FProducer)
	return producer
}

func integrationConsumer(t *testing.T, scope resource.Scope, topic, groupid, offsetpolicy string) FConsumer {
	resource, err := RemoteConsumerConfig{
		BootstrapServer: test_kafka_servers,
		Username:        kafka_username,
		Password:        kafka_password,
		Scope:           scope,
		ConsumerConfig: ConsumerConfig{
			Topic:        topic,
			GroupID:      groupid,
			OffsetPolicy: offsetpolicy,
		},
	}.Materialize()
	assert.NoError(t, err)
	consumer := resource.(FConsumer)
	return consumer
}
