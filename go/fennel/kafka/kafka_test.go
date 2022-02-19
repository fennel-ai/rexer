package kafka

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"fennel/lib/ftypes"
	"fennel/lib/value"
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
			msg, err := value.ToProtoValue(v)
			assert.NoError(t, err)
			assert.NoError(t, producer.LogProto(ctx, &msg, nil))
		}
		assert.NoError(t, producer.Flush(time.Second*5))
	}()
	go func() {
		defer wg.Done()
		defer consumer.Close()
		for i := 0; i < 10; i++ {
			v := value.Int(i)
			expected, err := value.ToProtoValue(v)
			assert.NoError(t, err)
			var found value.PValue
			err = consumer.ReadProto(ctx, &found, time.Second*30)
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

func mockProducerConsumer(t *testing.T, tierID ftypes.TierID) (FProducer, FConsumer) {
	broker := NewMockTopicBroker()
	producer, err := MockProducerConfig{
		Broker: &broker,
		Topic:  "sometopic",
	}.Materialize(tierID)
	assert.NoError(t, err)
	consumer, err := MockConsumerConfig{
		Broker:  &broker,
		Topic:   "sometopic",
		GroupID: "somegroup",
	}.Materialize(tierID)
	assert.NoError(t, err)
	return producer.(FProducer), consumer.(FConsumer)
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

	// Read 1 message. This is required to actually have the Broker assign a
	// partition to the consumer.
	go func() {
		defer wg.Done()
		defer consumer.Close()
		found, err := consumer.ReadBatch(ctx, 2, time.Second*5)
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

func TestLocal(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.TierID(rand.Uint32())

	t.Run("local_producer_consumer", func(t *testing.T) {
		producer, consumer := mockProducerConsumer(t, tierID)
		testProducerConsumer(t, producer, consumer)
	})
	t.Run("local_read_batch", func(t *testing.T) {
		producer, consumer := mockProducerConsumer(t, tierID)
		testReadBatch(t, producer, consumer)
	})
	t.Run("local_flush_commit_backlog", func(t *testing.T) {
		producer, consumer := mockProducerConsumer(t, tierID)
		testBacklog(t, producer, consumer)
	})
}
