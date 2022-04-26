package offsets

import (
	"context"
	"fmt"

	"fennel/lib/kvstore"
	"fennel/lib/utils/binary"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
)

const (
	tablet       = kvstore.Offset
	codec  uint8 = 1
)

// This package implements a key-value model for storing kafka
// topic offsets. These are encoded as:
// key=<topic><partition>, value=<offset>.

func Set(ctx context.Context, logger *zap.Logger, toppars kafka.TopicPartitions, kv kvstore.Writer) error {
	for _, toppar := range toppars {
		topic := *toppar.Topic
		partition := toppar.Partition
		k, err := encodeKey(topic, partition)
		if err != nil {
			return fmt.Errorf("failed to encode topic/partition key for {topic: \"%s\", partition: %d}: %v", topic, partition, err)
		}
		v, err := encodeValue(int64(toppar.Offset))
		if err != nil {
			return fmt.Errorf("failed to encode offset [%d]: %v", toppar.Offset, err)
		}
		logger.Info(fmt.Sprintf("[offset.set] topic: %v, partition: %v, offset: %v", topic, partition, int64(toppar.Offset)))
		err = kv.Set(ctx, tablet, k, kvstore.SerializedValue{
			Codec: codec,
			Raw:   v,
		})
		if err != nil {
			return fmt.Errorf("failed to set offset in kvstore: %v", err)
		}
	}
	return nil
}

func Get(ctx context.Context, logger *zap.Logger, topic string, kv kvstore.Reader) (kafka.TopicPartitions, error) {
	keyPrefix, err := getTopicPrefix(topic)
	if err != nil {
		return nil, err
	}
	ks, vs, err := kv.GetAll(ctx, tablet, keyPrefix)
	if err != nil {
		return nil, err
	}
	toppars := make([]kafka.TopicPartition, len(ks))
	for i, k := range ks {
		topic, partition, err := decodeKey(k)
		if err != nil {
			return nil, fmt.Errorf("failed to decode key: %v", err)
		}
		offset, err := decodeValue(vs[i].Raw)
		if err != nil {
			return nil, fmt.Errorf("failed to decode value: %v", err)
		}
		logger.Info(fmt.Sprintf("[offset.get] topic: %v, partition: %v, offset: %v", topic, partition, offset))
		tp := kafka.TopicPartition{
			Topic:     &topic,
			Partition: partition,
			Offset:    kafka.Offset(offset),
		}
		toppars[i] = tp
	}
	return toppars, nil
}

func encodeKey(topic string, partition int32) ([]byte, error) {
	buf := make([]byte, 8+len(topic)+8)

	curr := 0

	if n, err := binary.PutString(buf[curr:], topic); err != nil {
		return nil, err
	} else {
		curr += n
	}

	if n, err := binary.PutVarint(buf[curr:], int64(partition)); err != nil {
		return nil, err
	} else {
		curr += n
	}

	return buf[:curr], nil
}

func getTopicPrefix(topic string) ([]byte, error) {
	buf := make([]byte, 8+len(topic))

	curr := 0

	if n, err := binary.PutString(buf[curr:], topic); err != nil {
		return nil, err
	} else {
		curr += n
	}
	return buf[:curr], nil
}

func decodeKey(key []byte) (string, int32, error) {
	curr := 0

	topic, n, err := binary.ReadString(key[curr:])
	if err != nil {
		return "", 0, err
	} else {
		curr += n
	}

	partition, n, err := binary.ReadVarint(key[curr:])
	if err != nil {
		return "", 0, err
	} else {
		curr += n // nolint
	}

	return topic, int32(partition), nil
}

func encodeValue(offset int64) ([]byte, error) {
	buf := make([]byte, 8)
	if n, err := binary.PutVarint(buf, offset); err != nil {
		return nil, err
	} else {
		return buf[:n], nil
	}
}

func decodeValue(value []byte) (int64, error) {
	offset, _, err := binary.ReadVarint(value)
	return offset, err
}
