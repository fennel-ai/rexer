package kafka

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/resource"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func testProducerConsumer(t *testing.T, producer FProducer, consumer FConsumer) {
	// spin up two goroutines that produce/consume 10 messages each asyncrhonously
	wg := sync.WaitGroup{}
	wg.Add(2)
	ctx := context.Background()

	go func() {
		defer wg.Done()
		defer producer.Close()
		for i := 0; i < 10; i++ {
			v := value.Int(i)
			msg := value.ToJSON(v)
			assert.NoError(t, producer.Log(ctx, msg, nil))
		}
		assert.NoError(t, producer.Flush(time.Second*5))
	}()
	go func() {
		defer wg.Done()
		defer consumer.Close()
		for i := 0; i < 10; i++ {
			v := value.Int(i)
			expected := value.ToJSON(v)
			found, err := consumer.Read(ctx, time.Second*30)
			assert.NoError(t, err)
			assert.Equal(t, expected, found)
		}
	}()
	wg.Wait()
}

func testProducerConsumerProto(t *testing.T, producer FProducer, consumer FConsumer) {
	// spin up two goroutines that produce/consume 10 messages each asyncrhonously
	wg := sync.WaitGroup{}
	wg.Add(2)
	ctx := context.Background()

	go func() {
		defer wg.Done()
		defer producer.Close()
		for i := 0; i < 10; i++ {
			aggname := strconv.Itoa(i)
			msg := aggregate.AggRequest{AggName: aggname}
			assert.NoError(t, producer.LogProto(ctx, &msg, nil))
		}
		assert.NoError(t, producer.Flush(time.Second*5))
	}()
	go func() {
		defer wg.Done()
		defer consumer.Close()
		for i := 0; i < 10; i++ {
			aggname := strconv.Itoa(i)
			expected := aggregate.AggRequest{AggName: aggname}
			var found aggregate.AggRequest
			err := consumer.ReadProto(ctx, &found, time.Second*30)
			assert.NoError(t, err)
			assert.True(t, proto.Equal(&expected, &found))
		}
	}()
	wg.Wait()
}

func testReadBatch(t *testing.T, producer FProducer, consumer FConsumer) {
	expected := make([][]byte, 0)
	for i := 0; i < 10; i++ {
		msg := []byte(fmt.Sprintf("%d", i))
		expected = append(expected, msg)
	}
	wg := sync.WaitGroup{}
	wg.Add(2)
	ctx := context.Background()
	go func() {
		defer wg.Done()
		defer producer.Close()
		for _, msg := range expected {
			assert.NoError(t, producer.Log(ctx, msg, nil))
		}
	}()
	go func() {
		defer wg.Done()
		defer consumer.Close()
		// read in a batch of 4, 4, 2
		batch1, err := consumer.ReadBatch(ctx, 4, time.Second*30)
		assert.NoError(t, err)
		assert.Len(t, batch1, 4)

		batch2, err := consumer.ReadBatch(ctx, 4, time.Second*30)
		assert.NoError(t, err)
		assert.Len(t, batch2, 4)

		batch3, err := consumer.ReadBatch(ctx, 2, time.Second*30)
		assert.NoError(t, err)
		assert.Len(t, batch3, 2)
		found := append(batch1, batch2...)
		found = append(found, batch3...)

		assert.ElementsMatch(t, expected, found)
	}()
	wg.Wait()
}

func getMockProducer(t *testing.T, scope resource.Scope, topic string, broker *MockBroker) FProducer {
	producer, err := MockProducerConfig{
		Broker: broker,
		Topic:  scope.PrefixedName(topic),
	}.Materialize()
	assert.NoError(t, err)
	return producer.(FProducer)
}

func getMockConsumer(t *testing.T, scope resource.Scope, topic, groupID string, broker *MockBroker) FConsumer {
	consumer, err := MockConsumerConfig{
		Broker:  broker,
		Topic:   scope.PrefixedName(topic),
		GroupID: groupID,
		Scope:   scope,
	}.Materialize()
	assert.NoError(t, err)
	return consumer.(FConsumer)
}

func testBacklog(t *testing.T, producer FProducer, consumer FConsumer) {
	wg := sync.WaitGroup{}
	wg.Add(2)
	ctx := context.Background()
	message := []byte(fmt.Sprintf("hello"))
	go func() {
		defer wg.Done()
		defer producer.Close()
		for i := 0; i < 10; i++ {
			err := producer.Log(ctx, message, nil)
			assert.NoError(t, err)
		}
		assert.NoError(t, producer.Flush(time.Second*5))
	}()

	// Read 2 messages. This is required to actually have the Broker assign a
	// partition to the consumer.
	go func() {
		defer wg.Done()
		defer consumer.Close()
		found, err := consumer.ReadBatch(ctx, 2, time.Second*30)
		assert.NoError(t, err)
		assert.ElementsMatch(t, [][]byte{message, message}, found)
		// Commit the read offset.
		err = consumer.Commit()
		assert.NoError(t, err)
		// Now calculate the backlog.
		backlog, err := consumer.Backlog()
		assert.NoError(t, err)
		assert.Equal(t, 8, backlog)
	}()
	wg.Wait()
}

func testDifferentConsumerGroups(t *testing.T, producer FProducer, consumer1, consumer2 FConsumer) {
	// this test verifies that consumers of different groups are independent and dont' affect
	// each other's commits/messages
	ctx := context.Background()
	expected := make([][]byte, 0)
	for i := 0; i < 5; i++ {
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
		defer wg.Done()
		defer consumer1.Close()
		defer consumer2.Close()
		found, err := consumer1.ReadBatch(ctx, 5, time.Second*30)
		assert.NoError(t, err)
		assert.Equal(t, expected, found)
		consumer1.Commit()
		found, err = consumer2.ReadBatch(ctx, 5, time.Second*30)
		assert.NoError(t, err)
		assert.Equal(t, expected, found)
	}()
	wg.Wait()
}

func testSameConsumerGroup(t *testing.T, producer FProducer, consumer1, consumer2 FConsumer) {
	// this test verifies that consumers of same group don't duplicate read messages
	ctx := context.Background()
	expected := make([][]byte, 0)
	found1 := make([][]byte, 0)
	found2 := make([][]byte, 0)
	for i := 0; i < 10; i++ {
		expected = append(expected, []byte(fmt.Sprintf("%d", i)))
	}
	wg := sync.WaitGroup{}
	wg.Add(3)
	go func() {
		defer wg.Done()
		defer producer.Close()
		for _, msg := range expected {
			assert.NoError(t, producer.Log(ctx, msg, nil))
		}
	}()
	go func() {
		defer wg.Done()
		defer consumer1.Close()
		var err error
		found1, err = consumer1.ReadBatch(ctx, 5, time.Second*30)
		assert.NoError(t, err)
		assert.NoError(t, consumer1.Commit())
	}()
	go func() {
		defer wg.Done()
		defer consumer2.Close()
		var err error
		found2, err = consumer2.ReadBatch(ctx, 5, time.Second*30)
		assert.NoError(t, err)
		assert.NoError(t, consumer2.Commit())
	}()
	wg.Wait()
	found := append(found1, found2...)
	assert.ElementsMatch(t, expected, found)
}

func testNoAutoCommit(t *testing.T, producer FProducer, consumer1, consumer2 FConsumer) {
	// verify that if a consumer closes before committing, its messages
	// get assigned to another consumer
	// NOTE: current local / mock kafka implementation doesn't support commits so this
	// only applies to the remote kafka
	ctx := context.Background()
	expected := make([][]byte, 0)
	found := make([][]byte, 0)
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
	found, err := consumer2.ReadBatch(ctx, 10, time.Second*30)
	assert.NoError(t, err)
	assert.NoError(t, consumer2.Commit())
	assert.ElementsMatch(t, found, expected)
}

func TestLocal(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	topic := "topic"
	tierID := ftypes.RealmID(rand.Uint32())
	scope := resource.NewTierScope(tierID)

	t.Run("local_producer_consumer", func(t *testing.T) {
		broker := NewMockTopicBroker()
		producer := getMockProducer(t, scope, topic, &broker)
		consumer := getMockConsumer(t, scope, topic, "group", &broker)
		testProducerConsumer(t, producer, consumer)
	})
	t.Run("local_producer_consumer_proto", func(t *testing.T) {
		broker := NewMockTopicBroker()
		producer := getMockProducer(t, scope, topic, &broker)
		consumer := getMockConsumer(t, scope, topic, "group", &broker)
		testProducerConsumerProto(t, producer, consumer)
	})
	t.Run("local_read_batch", func(t *testing.T) {
		broker := NewMockTopicBroker()
		producer := getMockProducer(t, scope, topic, &broker)
		consumer := getMockConsumer(t, scope, topic, "group", &broker)
		testReadBatch(t, producer, consumer)
	})
	t.Run("local_flush_commit_backlog", func(t *testing.T) {
		broker := NewMockTopicBroker()
		producer := getMockProducer(t, scope, topic, &broker)
		consumer := getMockConsumer(t, scope, topic, "group", &broker)
		testBacklog(t, producer, consumer)
	})
	t.Run("local_different_consumer_groups", func(t *testing.T) {
		broker := NewMockTopicBroker()
		producer := getMockProducer(t, scope, topic, &broker)
		consumer1 := getMockConsumer(t, scope, topic, "group1", &broker)
		consumer2 := getMockConsumer(t, scope, topic, "group2", &broker)
		testDifferentConsumerGroups(t, producer, consumer1, consumer2)
	})
	t.Run("local_same_consumer_group", func(t *testing.T) {
		broker := NewMockTopicBroker()
		producer := getMockProducer(t, scope, topic, &broker)
		consumer1 := getMockConsumer(t, scope, topic, "group", &broker)
		consumer2 := getMockConsumer(t, scope, topic, "group", &broker)
		testSameConsumerGroup(t, producer, consumer1, consumer2)
	})
}
