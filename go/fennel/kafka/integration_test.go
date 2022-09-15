//go:build integration

package kafka

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"

	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/lib/value"
	"fennel/resource"
)

const (
	test_kafka_servers = "b-2.p2kafkacluster.yo55qq.c10.kafka.us-west-2.amazonaws.com:9096,b-1.p2kafkacluster.yo55qq.c10.kafka.us-west-2.amazonaws.com:9096"
	kafka_username     = "p-2-username"
	kafka_password     = "p-2-password"
	kafka_sasl_mechanism = "SCRAM-SHA-512"
)

func TestIntegration(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	topic := "testtopic"
	t.Run("integration_producer_consumer", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		producer := singlePartitionProducer(t, scope, topic)
		consumer := integrationConsumer(t, scope, topic, utils.RandString(5), DefaultOffsetPolicy)
		defer teardownKafkaTopic(t, scope, topic)
		testProducerConsumer(t, producer, consumer, true /*ordered=*/)
	})
	t.Run("integration_producer_consumer_proto", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		producer := singlePartitionProducer(t, scope, topic)
		consumer := integrationConsumer(t, scope, topic, utils.RandString(5), DefaultOffsetPolicy)
		defer teardownKafkaTopic(t, scope, topic)
		testProducerConsumerProto(t, producer, consumer, true /*ordered=*/)
	})
	t.Run("integration_read_batch", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		producer := singlePartitionProducer(t, scope, topic)
		consumer := integrationConsumer(t, scope, topic, utils.RandString(5), DefaultOffsetPolicy)
		defer teardownKafkaTopic(t, scope, topic)
		testReadBatch(t, producer, consumer, true /*ordered=*/)
	})
	t.Run("integration_flush_commit_backlog", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		producer := singlePartitionProducer(t, scope, topic)
		consumer := integrationConsumer(t, scope, topic, utils.RandString(5), DefaultOffsetPolicy)
		defer teardownKafkaTopic(t, scope, topic)
		testBacklog(t, producer, consumer)
	})
	t.Run("integration_different_consumer_groups", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		producer := singlePartitionProducer(t, scope, topic)
		consumer1 := integrationConsumer(t, scope, topic, utils.RandString(5), DefaultOffsetPolicy)
		consumer2 := integrationConsumer(t, scope, topic, utils.RandString(5), DefaultOffsetPolicy)
		defer teardownKafkaTopic(t, scope, topic)
		testDifferentConsumerGroups(t, producer, consumer1, consumer2, true /*ordered=*/)
	})
	t.Run("integration_same_consumer_groups", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		group := utils.RandString(5)
		producer := singlePartitionProducer(t, scope, topic)
		consumer1 := integrationConsumer(t, scope, topic, group, DefaultOffsetPolicy)
		consumer2 := integrationConsumer(t, scope, topic, group, DefaultOffsetPolicy)
		defer teardownKafkaTopic(t, scope, topic)
		testSameConsumerGroup(t, producer, consumer1, consumer2)
	})
	t.Run("integration_no_auto_commit", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		group := utils.RandString(5)
		producer := singlePartitionProducer(t, scope, topic)
		consumer1 := integrationConsumer(t, scope, topic, group, DefaultOffsetPolicy)
		consumer2 := integrationConsumer(t, scope, topic, group, DefaultOffsetPolicy)
		defer teardownKafkaTopic(t, scope, topic)
		testNoAutoCommit(t, producer, consumer1, consumer2, true /*ordered=*/)
	})
	t.Run("integration_same_key_read_by_same_consumer", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		group := utils.RandString(5)
		producer := multiPartitionProducer(t, scope, topic)
		consumer1 := integrationConsumer(t, scope, topic, group, DefaultOffsetPolicy)
		consumer2 := integrationConsumer(t, scope, topic, group, DefaultOffsetPolicy)
		defer teardownKafkaTopic(t, scope, topic)
		testSameKeyReadBySameConsumer(t, producer, consumer1, consumer2, false /*ordered=*/)
	})
}

func TestIntegrationMultiplePartitions(t *testing.T) {
	// here, there is a guarantee on the ordering of the messages in the partition, but no guarantees on ordering across partitions
	rand.Seed(time.Now().UnixNano())
	topic := "testtopic-multipars"
	t.Run("integration_producer_consumer", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		producer := multiPartitionProducer(t, scope, topic)
		consumer := integrationConsumer(t, scope, topic, utils.RandString(5), DefaultOffsetPolicy)
		defer teardownKafkaTopic(t, scope, topic)
		testProducerConsumer(t, producer, consumer, false /*ordered=*/)
	})
	t.Run("integration_producer_consumer_proto", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		producer := multiPartitionProducer(t, scope, topic)
		consumer := integrationConsumer(t, scope, topic, utils.RandString(5), DefaultOffsetPolicy)
		defer teardownKafkaTopic(t, scope, topic)
		testProducerConsumerProto(t, producer, consumer, false /*ordered=*/)
	})
	t.Run("integration_read_batch", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		producer := multiPartitionProducer(t, scope, topic)
		consumer := integrationConsumer(t, scope, topic, utils.RandString(5), DefaultOffsetPolicy)
		defer teardownKafkaTopic(t, scope, topic)
		testReadBatch(t, producer, consumer, false /*ordered=*/)
	})
	t.Run("integration_flush_commit_backlog", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		producer := multiPartitionProducer(t, scope, topic)
		consumer := integrationConsumer(t, scope, topic, utils.RandString(5), DefaultOffsetPolicy)
		defer teardownKafkaTopic(t, scope, topic)
		testBacklog(t, producer, consumer)
	})
	t.Run("integration_different_consumer_groups", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		producer := multiPartitionProducer(t, scope, topic)
		consumer1 := integrationConsumer(t, scope, topic, utils.RandString(5), DefaultOffsetPolicy)
		consumer2 := integrationConsumer(t, scope, topic, utils.RandString(5), DefaultOffsetPolicy)
		defer teardownKafkaTopic(t, scope, topic)
		testDifferentConsumerGroups(t, producer, consumer1, consumer2, false /*ordered=*/)
	})
	t.Run("integration_same_consumer_groups", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		group := utils.RandString(5)
		producer := multiPartitionProducer(t, scope, topic)
		consumer1 := integrationConsumer(t, scope, topic, group, DefaultOffsetPolicy)
		consumer2 := integrationConsumer(t, scope, topic, group, DefaultOffsetPolicy)
		defer teardownKafkaTopic(t, scope, topic)
		testSameConsumerGroup(t, producer, consumer1, consumer2)
	})
	t.Run("integration_no_auto_commit", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		group := utils.RandString(5)
		producer := multiPartitionProducer(t, scope, topic)
		consumer1 := integrationConsumer(t, scope, topic, group, DefaultOffsetPolicy)
		consumer2 := integrationConsumer(t, scope, topic, group, DefaultOffsetPolicy)
		defer teardownKafkaTopic(t, scope, topic)
		testNoAutoCommit(t, producer, consumer1, consumer2, false /*ordered=*/)
	})
	t.Run("integration_same_key_read_by_same_consumer", func(t *testing.T) {
		tierID := ftypes.RealmID(rand.Uint32())
		scope := resource.NewTierScope(tierID)
		t.Parallel()
		group := utils.RandString(5)
		producer := multiPartitionProducer(t, scope, topic)
		consumer1 := integrationConsumer(t, scope, topic, group, DefaultOffsetPolicy)
		consumer2 := integrationConsumer(t, scope, topic, group, DefaultOffsetPolicy)
		defer teardownKafkaTopic(t, scope, topic)
		testSameKeyReadBySameConsumer(t, producer, consumer1, consumer2, false /*ordered=*/)
	})
}

func setupKafkaTopics(scope resource.Scope, topic string, partitions int) error {
	name := scope.PrefixedName(topic)
	// Create admin client
	c, err := kafka.NewAdminClient(ConfigMap(test_kafka_servers, kafka_username, kafka_password, kafka_sasl_mechanism))
	if err != nil {
		return fmt.Errorf("failed to create admin client: %v", err)
	}
	defer c.Close()

	// now create the Topic
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	results, err := c.CreateTopics(ctx, []kafka.TopicSpecification{{Topic: name, NumPartitions: partitions}})
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

func teardownKafkaTopic(t *testing.T, scope resource.Scope, topic string) {
	name := scope.PrefixedName(topic)
	// Create admin client.
	c, err := kafka.NewAdminClient(ConfigMap(test_kafka_servers, kafka_username, kafka_password, kafka_sasl_mechanism))
	assert.NoError(t, err)
	defer c.Close()

	// delete the Topic
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = c.DeleteTopics(ctx, []string{name})
	assert.NoError(t, err)
}

func singlePartitionProducer(t *testing.T, scope resource.Scope, topic string) FProducer {
	return integrationProducer(t, scope, topic, 1)
}

func multiPartitionProducer(t *testing.T, scope resource.Scope, topic string) FProducer {
	return integrationProducer(t, scope, topic, 2)
}

func integrationProducer(t *testing.T, scope resource.Scope, topic string, partitions int) FProducer {
	// first create the topics
	assert.NoError(t, setupKafkaTopics(scope, topic, partitions))

	// then create producer
	resource, err := RemoteProducerConfig{
		Topic:           topic,
		BootstrapServer: test_kafka_servers,
		Username:        kafka_username,
		Password:        kafka_password,
		SaslMechanism: 	 SaslScramSha512Mechanism,
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
		SaslMechanism:   SaslScramSha512Mechanism,
		ConsumerConfig: ConsumerConfig{
			Scope:        scope,
			Topic:        topic,
			GroupID:      groupid,
			OffsetPolicy: offsetpolicy,
		},
	}.Materialize()
	assert.NoError(t, err)
	consumer := resource.(FConsumer)
	return consumer
}

func testSameKeyReadBySameConsumer(t *testing.T, producer FProducer, consumer1, consumer2 FConsumer, ordered bool) {
	wg := sync.WaitGroup{}
	wg.Add(3)
	ctx := context.Background()
	found1 := make([][]byte, 0)
	found2 := make([][]byte, 0)
	go func() {
		defer wg.Done()
		defer producer.Close()
		for i := 0; i < 10; i++ {
			msg, e := value.Int(i).MarshalJSON()
			assert.NoError(t, e)
			// same key for two consecutive messages
			key := []byte(fmt.Sprintf("%d", (i/2)*10))
			assert.NoError(t, producer.Log(ctx, msg, key))
		}
	}()
	go func() {
		defer wg.Done()
		defer consumer1.Close()
		var err error
		found1, err = consumer1.ReadBatch(ctx, 10, time.Second*10)
		assert.NoError(t, err)
		_, err = consumer1.Commit()
		assert.NoError(t, err)
		// it is possible that a consumer has nothing to commit in case of a multi-partition setup
	}()
	go func() {
		defer wg.Done()
		defer consumer2.Close()
		var err error
		found2, err = consumer2.ReadBatch(ctx, 10, time.Second*10)
		assert.NoError(t, err)
		_, err = consumer2.Commit()
		assert.NoError(t, err)
		// it is possible that a consumer has nothing to commit in case of a multi-partition setup
	}()
	wg.Wait()
	// in the multi-partition setup, one consumer would have been assigned to one partition, hence should read
	// messages in even number (we have the same key for two messages); and since each consumer will read them in order,
	// every next message should have +1 value than itself
	for i := 0; i < len(found1)/2; i++ {
		v1, e := value.FromJSON(found1[i*2])
		assert.NoError(t, e)
		v2, e := value.FromJSON(found1[i*2+1])
		assert.NoError(t, e)
		assert.Equal(t, int64(v1.(value.Int))+1, int64(v2.(value.Int)))
	}
	for i := 0; i < len(found2)/2; i++ {
		v1, e := value.FromJSON(found2[i*2])
		assert.NoError(t, e)
		v2, e := value.FromJSON(found2[i*2+1])
		assert.NoError(t, e)
		assert.Equal(t, int64(v1.(value.Int))+1, int64(v2.(value.Int)))
	}
}

func testNoAutoCommit(t *testing.T, producer FProducer, consumer1, consumer2 FConsumer, ordered bool) {
	// verify that if a consumer closes before committing, its messages
	// get assigned to another consumer
	// NOTE: current local / mock kafka implementation doesn't support commits so this
	// only applies to the remote kafka
	ctx := context.Background()
	expected := make([][]byte, 0)
	for i := 0; i < 10; i++ {
		expected = append(expected, []byte(fmt.Sprintf("%d", i)))
	}
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer producer.Close()
		for _, msg := range expected {
			assert.NoError(t, producer.Log(ctx, msg, nil))
		}
	}()
	go func() {
		// consumer 1 reads some messages but then closes before doing commit
		defer wg.Done()
		defer consumer1.Close()
		_, err := consumer1.ReadBatch(ctx, 5, time.Second*30)
		assert.NoError(t, err)
	}()
	wg.Wait()
	// now consumer 2 is kicked off, which should be able to read all messages
	defer consumer2.Close()
	found, err := consumer2.ReadBatch(ctx, 20, time.Second*10)
	assert.NoError(t, err)
	// it is possible that a consumer has nothing to commit in case of a multi-partition setup
	_, err = consumer2.Commit()
	assert.NoError(t, err)
	if ordered {
		assert.Equal(t, found, expected)
	} else {
		assert.ElementsMatch(t, found, expected)
	}
}
