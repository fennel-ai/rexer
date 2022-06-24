package kafka

import (
	"context"
	"testing"
	"time"

	"fennel/lib/ftypes"
	"fennel/resource"

	"github.com/stretchr/testify/assert"
)

func TestOffsets(t *testing.T) {
	broker := NewMockTopicBroker()
	topic := "my-topic"
	tierId := ftypes.RealmID(1)
	consumerCfg := MockConsumerConfig{
		Broker:  &broker,
		Topic:   topic,
		GroupID: "my-group",
		Scope:   resource.NewTierScope(tierId),
	}
	consumer, err := consumerCfg.Materialize()
	assert.NoError(t, err)
	c, ok := consumer.(mockConsumer)
	assert.True(t, ok)

	// Intially the consumer offsets should be empty.
	toppars, err := c.Offsets()
	assert.NoError(t, err)
	assert.Equal(t, 0, len(toppars))

	// Log a message and confirm that now there is a backlog
	// of 1 message.
	producerCfg := MockProducerConfig{
		Broker: &broker,
		Topic:  topic,
		Scope:  resource.NewTierScope(tierId),
	}
	producer, err := producerCfg.Materialize()
	assert.NoError(t, err)
	p := producer.(mockProducer)
	err = p.Log(context.Background(), []byte("hello"), nil)
	assert.NoError(t, err)
	backlog, err := c.Backlog()
	assert.NoError(t, err)
	assert.EqualValues(t, 1, backlog)

	// Consume the message from the broker. The backlog
	// should now be 0 and the offset should be 1.
	_, err = c.ReadBatch(context.Background(), 100, time.Millisecond*10)
	assert.NoError(t, err)
	backlog, err = c.Backlog()
	assert.NoError(t, err)
	assert.EqualValues(t, 0, backlog)
	toppars, err = c.Offsets()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(toppars))
	assert.Equal(t, topic, *toppars[0].Topic)
	assert.EqualValues(t, 1, toppars[0].Offset)
}
