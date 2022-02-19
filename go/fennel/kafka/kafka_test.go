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
	"google.golang.org/protobuf/proto"

	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/resource"
)

const (
	test_kafka_servers = "pkc-pgq85.us-west-2.aws.confluent.cloud:9092"
	kafka_username     = "PQESAHSX5EUQJPIV"
	kafka_password     = "EDjraEtpIjYQBv9WQ2QINnZZcExKUtm6boweLCsQ5gv3arWSk+VHyD1kfjJ+p382"
)

func TestProducerConsumer(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.TierID(rand.Uint32())

	// first create the topics
	topic := "kafka_test_topic"
	assert.NoError(t, setupKafkaTopics(tierID, topic))
	defer teardownKafkaTopics(tierID, topic)

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
		GroupID:         "test_group",
		OffsetPolicy:    "earliest",
	}.Materialize(tierID)
	assert.NoError(t, err)
	consumer := resource.(FConsumer)
	// spin up two goroutines that produce/consume 10 messages each asyncrhonously
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		for i := 0; i < 10; i++ {
			v := value.Int(i)
			msg, err := value.ToProtoValue(v)
			assert.NoError(t, err)
			assert.NoError(t, producer.LogProto(&msg, nil))
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < 10; i++ {
			v := value.Int(i)
			expected, err := value.ToProtoValue(v)
			assert.NoError(t, err)
			var found value.PValue
			err = consumer.ReadProto(&found, -1)
			assert.NoError(t, err)
			assert.True(t, proto.Equal(&expected, &found))
		}
		wg.Done()
	}()
	wg.Wait()
}

func setupKafkaTopics(tierID ftypes.TierID, topic string) error {
	name := resource.TieredName(tierID, topic)
	// Create admin client
	c, err := kafka.NewAdminClient(ConfigMap(test_kafka_servers, kafka_username, kafka_password))
	if err != nil {
		return fmt.Errorf("failed to create admin client: %v", err)
	}
	defer c.Close()

	// now create the topic
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

	// delete the topic
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = c.DeleteTopics(ctx, []string{name})
	return err
}
