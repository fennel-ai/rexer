package kafka_test

import (
	"testing"

	"fennel/kafka"
	"fennel/lib/action"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func TestBacklog(t *testing.T) {
	tier, err := test.Tier()
	defer test.Teardown(tier)

	assert.NoError(t, err)
	topicId := action.ACTIONLOG_KAFKA_TOPIC

	// Produce some messages.
	producer := tier.Producers[topicId]
	message := &action.ProtoAction{}
	for i := 0; i < 10; i++ {
		err = producer.Log(message)
		assert.NoError(t, err)
	}
	if rp, ok := producer.(kafka.RemoteProducer); ok {
		remaining := rp.Flush(1000 /* timeoutMs */)
		assert.Equal(t, 0, remaining)
	}

	consumer, err := tier.NewKafkaConsumer(topicId, "somegroup", "earliest")
	assert.NoError(t, err)
	// Read 1 message. This is required to actually have the broker assign a
	// partition to the consumer.
	err = consumer.Read(message)
	assert.NoError(t, err)
	// Commit the read offset.
	if rc, ok := consumer.(kafka.RemoteConsumer); ok {
		_, err = rc.Commit()
		assert.NoError(t, err)
	}
	// Now calculate the backlog.
	backlog, err := consumer.Backlog()
	assert.NoError(t, err)
	assert.Equal(t, 9, backlog)
}
