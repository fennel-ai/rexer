package offsets

import (
	"bytes"
	"testing"

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
