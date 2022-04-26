package offsets

import (
	"bytes"
	"context"
	"fennel/lib/badger"
	"fennel/test"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/require"
)

func TestKeyPrefix(t *testing.T) {
	topic := "test"
	keyPrefix, err := getTopicPrefix(topic)
	require.NoError(t, err)
	encodedKey, err := encodeKey(topic, int32(0))
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(encodedKey, keyPrefix))
}

func TestKeyEncoding(t *testing.T) {
	topic := "test"
	partition := int32(96)
	key, err := encodeKey(topic, partition)
	require.NoError(t, err)
	topic2, partition2, err := decodeKey(key)
	require.NoError(t, err)
	require.Equal(t, topic, topic2)
	require.Equal(t, partition, partition2)
}

func TestValueEncoding(t *testing.T) {
	offset := int64(42)
	value, err := encodeValue(offset)
	require.NoError(t, err)
	offset2, err := decodeValue(value)
	require.NoError(t, err)
	require.Equal(t, offset, offset2)
}

func getPtr(s string) *string {
	return &s
}

func TestPartitionSetGet(t *testing.T) {
	tier, err := test.Tier()
	require.NoError(t, err)
	defer test.Teardown(tier)

	txn := tier.Badger.NewTransaction(true)
	store := badger.NewTransactionalStore(tier, txn)

	p1 := kafka.TopicPartition{
		Topic:     getPtr("topic1"),
		Partition: int32(29),
		Offset:    kafka.Offset(42),
	}
	p2 := kafka.TopicPartition{
		Topic:     getPtr("topic1"),
		Partition: int32(75),
		Offset:    kafka.Offset(1294),
	}
	err = Set(context.Background(), tier.Logger, []kafka.TopicPartition{p1, p2}, store)
	require.NoError(t, err)
	got, err := Get(context.Background(), tier.Logger, "topic1", store)
	require.NoError(t, err)
	require.ElementsMatch(t, []kafka.TopicPartition{p1, p2}, got)
}
