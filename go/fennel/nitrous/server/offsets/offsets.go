package offsets

import (
	"fmt"

	"fennel/hangar"
	"fennel/lib/utils/binary"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func RestoreBinlogOffset(store hangar.Hangar, offsetkey []byte) (kafka.TopicPartitions, error) {
	vgs, err := store.GetMany([]hangar.KeyGroup{{Prefix: hangar.Key{Data: offsetkey}}})
	if err != nil {
		return nil, fmt.Errorf("failed to get binlog offsets: %w", err)
	}
	if len(vgs) == 0 {
		return nil, nil
	}
	toppars := make([]kafka.TopicPartition, len(vgs[0].Fields))
	for i, f := range vgs[0].Fields {
		topic, partition, err := decodeField(f)
		if err != nil {
			return nil, fmt.Errorf("failed to decode binlog offset field [%s]: %w", string(f), err)
		}
		offset, err := decodeValue(vgs[0].Values[i])
		if err != nil {
			return nil, fmt.Errorf("failed to decode binlog offset value [%s]: %w", string(vgs[0].Values[i]), err)
		}
		tp := kafka.TopicPartition{
			Topic:     &topic,
			Partition: partition,
			Offset:    kafka.Offset(offset),
		}
		toppars[i] = tp
	}
	return toppars, nil
}

func SaveBinlogOffsets(toppars []kafka.TopicPartition, offsetkey []byte) ([]hangar.Key, []hangar.ValGroup, error) {
	fields := make([][]byte, len(toppars))
	values := make([][]byte, len(toppars))
	for i, toppar := range toppars {
		topic := *toppar.Topic
		partition := toppar.Partition
		f, err := encodeField(topic, partition)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to encode topic/partition key for {topic: \"%s\", partition: %d}: %v", topic, partition, err)
		}
		fields[i] = f
		v, err := encodeValue(int64(toppar.Offset))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to encode offset [%d]: %v", toppar.Offset, err)
		}
		values[i] = v
	}
	return []hangar.Key{{Data: offsetkey}}, []hangar.ValGroup{{Fields: fields, Values: values, Expiry: 0}}, nil
}

func encodeField(topic string, partition int32) ([]byte, error) {
	buf := make([]byte, 10+len(topic)+10)
	curr := 0
	n, err := binary.PutString(buf[curr:], topic)
	if err != nil {
		return nil, err
	}
	curr += n
	n, err = binary.PutVarint(buf[curr:], int64(partition))
	if err != nil {
		return nil, err
	}
	curr += n
	return buf[:curr], nil
}

func decodeField(key []byte) (string, int32, error) {
	curr := 0
	topic, n, err := binary.ReadString(key[curr:])
	if err != nil {
		return "", 0, err
	}
	curr += n
	partition, n, err := binary.ReadVarint(key[curr:])
	if err != nil {
		return "", 0, err
	}
	curr += n // nolint
	return topic, int32(partition), nil
}

func encodeValue(offset int64) ([]byte, error) {
	buf := make([]byte, 10)
	n, err := binary.PutVarint(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func decodeValue(value []byte) (int64, error) {
	offset, _, err := binary.ReadVarint(value)
	return offset, err
}
