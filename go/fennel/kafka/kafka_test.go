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

func testProducerConsumer(t *testing.T, producer FProducer, consumer FConsumer, ordered bool) {
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
		expected := make([][]byte, 10)
		actual := make([][]byte, 10)
		for i := 0; i < 10; i++ {
			v := value.Int(i)
			expected[i] = value.ToJSON(v)
			var err error
			actual[i], err = consumer.Read(ctx, time.Second*30)
			assert.NoError(t, err)
		}
		if ordered {
			assert.Equal(t, expected, actual)
		} else {
			assert.ElementsMatch(t, expected, actual)
		}
	}()
	wg.Wait()
}

func testProducerConsumerProto(t *testing.T, producer FProducer, consumer FConsumer, ordered bool) {
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
		expected := make([]aggregate.AggRequest, 10)
		actual := make([]aggregate.AggRequest, 10)
		for i := 0; i < 10; i++ {
			aggname := strconv.Itoa(i)
			expected[i] = aggregate.AggRequest{AggName: aggname}
			err := consumer.ReadProto(ctx, &actual[i], time.Second*30)
			assert.NoError(t, err)
		}
		if ordered {
			for i := 0; i < 10; i++ {
				assert.True(t, proto.Equal(&expected[i], &actual[i]))
			}
		} else {
			// if they are unordered, it is difficult to match the equality across protos,
			// so for now, we will match the agg names
			e := make([]string, 10)
			a := make([]string, 10)
			for i := 0; i < 10; i++ {
				e[i], a[i] = expected[i].AggName, actual[i].AggName
			}
			assert.ElementsMatch(t, e, a)
		}
	}()
	wg.Wait()
}

func testReadBatch(t *testing.T, producer FProducer, consumer FConsumer, ordered bool) {
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

		if ordered {
			assert.Equal(t, expected, found)
		} else {
			assert.ElementsMatch(t, expected, found)
		}
	}()
	wg.Wait()
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
		consumer1.Commit()
		// it is possible that a consumer has nothing to commit in case of a multi-partition setup
	}()
	go func() {
		defer wg.Done()
		defer consumer2.Close()
		var err error
		found2, err = consumer2.ReadBatch(ctx, 10, time.Second*10)
		assert.NoError(t, err)
		consumer2.Commit()
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

func getMockProducer(t *testing.T, scope resource.Scope, topic string, broker *MockBroker) FProducer {
	producer, err := MockProducerConfig{
		Broker: broker,
		Topic:  topic,
		Scope:  scope,
	}.Materialize()
	assert.NoError(t, err)
	return producer.(FProducer)
}

func getMockConsumer(t *testing.T, scope resource.Scope, topic, groupID string, broker *MockBroker) FConsumer {
	consumer, err := MockConsumerConfig{
		Broker:  broker,
		Topic:   topic,
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
		_, err = consumer.Commit()
		assert.NoError(t, err)
		// Now calculate the backlog.
		backlog, err := consumer.Backlog()
		assert.NoError(t, err)
		assert.Equal(t, 8, backlog)
	}()
	wg.Wait()
}

func testDifferentConsumerGroups(t *testing.T, producer FProducer, consumer1, consumer2 FConsumer, ordered bool) {
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
		if ordered {
			assert.Equal(t, expected, found)
		} else {
			assert.ElementsMatch(t, expected, found)
		}
		consumer1.Commit()
		found, err = consumer2.ReadBatch(ctx, 5, time.Second*30)
		assert.NoError(t, err)
		if ordered {
			assert.Equal(t, expected, found)
		} else {
			assert.ElementsMatch(t, expected, found)
		}
		consumer2.Commit()
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
	// in the scenario where topic has multiple partitions, it is not required that messages are
	// equally distributed across the partitions. Try reading 10 elements from each with a timeout.
	go func() {
		defer wg.Done()
		defer consumer1.Close()
		var err error
		found1, err = consumer1.ReadBatch(ctx, 10, time.Second*10)
		assert.NoError(t, err)
		consumer1.Commit()
		// it is possible that a consumer has nothing to commit in case of a multi-partition setup
	}()
	go func() {
		defer wg.Done()
		defer consumer2.Close()
		var err error
		found2, err = consumer2.ReadBatch(ctx, 10, time.Second*10)
		assert.NoError(t, err)
		consumer2.Commit()
		// it is possible that a consumer has nothing to commit in case of a multi-partition setup
	}()
	wg.Wait()
	found := append(found1, found2...)
	assert.ElementsMatch(t, expected, found)
}

func testNoAutoCommit(t *testing.T, producer FProducer, consumer1, consumer2 FConsumer, ordered bool) {
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
	found, err := consumer2.ReadBatch(ctx, 20, time.Second*10)
	assert.NoError(t, err)
	// it is possible that a consumer has nothing to commit in case of a multi-partition setup
	consumer2.Commit()
	if ordered {
		assert.Equal(t, found, expected)
	} else {
		assert.ElementsMatch(t, found, expected)
	}
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
		testProducerConsumer(t, producer, consumer, true /*ordered=*/)
	})
	t.Run("local_producer_consumer_proto", func(t *testing.T) {
		broker := NewMockTopicBroker()
		producer := getMockProducer(t, scope, topic, &broker)
		consumer := getMockConsumer(t, scope, topic, "group", &broker)
		testProducerConsumerProto(t, producer, consumer, true /*ordered=*/)
	})
	t.Run("local_read_batch", func(t *testing.T) {
		broker := NewMockTopicBroker()
		producer := getMockProducer(t, scope, topic, &broker)
		consumer := getMockConsumer(t, scope, topic, "group", &broker)
		testReadBatch(t, producer, consumer, true /*ordered=*/)
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
		testDifferentConsumerGroups(t, producer, consumer1, consumer2, true /*ordered=*/)
	})
	t.Run("local_same_consumer_group", func(t *testing.T) {
		broker := NewMockTopicBroker()
		producer := getMockProducer(t, scope, topic, &broker)
		consumer1 := getMockConsumer(t, scope, topic, "group", &broker)
		consumer2 := getMockConsumer(t, scope, topic, "group", &broker)
		testSameConsumerGroup(t, producer, consumer1, consumer2)
	})
}
