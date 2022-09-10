package tailer

import (
	"bytes"
	"context"
	"fmt"

	"fennel/hangar"
	"fennel/lib/utils/binary"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
)

const (
	OFFSET_KEY_PREFIX = "offsets_"
	DEFAULT_OFFSET    = kafka.OffsetBeginning
)

var (
	OFFSET_FIELD = []byte("tailer_offset")
)

// Given a list of topic partitions, returns them with Offset field set to the
// last committed offset.
func decodeOffsets(toppars kafka.TopicPartitions, store hangar.Hangar) (kafka.TopicPartitions, error) {
	if len(toppars) == 0 {
		return toppars, nil
	}
	kgs := make([]hangar.KeyGroup, len(toppars))
	for i, toppar := range toppars {
		key, err := encodeKey(toppar)
		if err != nil {
			return nil, err
		}
		kgs[i].Prefix = hangar.Key{Data: key}
	}
	// Call in write mode to get consistent results.
	ctx := context.Background()
	ctx = hangar.NewWriteContext(ctx)
	vgs, err := store.GetMany(ctx, kgs)
	if err != nil {
		return nil, fmt.Errorf("failed to get offsets: %w", err)
	}
	for i, vg := range vgs {
		if len(vg.Fields) == 0 {
			zap.L().Info("Offset not found for partition. Using default value.",
				zap.String("topic", *toppars[i].Topic), zap.Int32("partition", toppars[i].Partition))
			toppars[i].Offset = DEFAULT_OFFSET
		} else if !bytes.Equal(vg.Fields[0], OFFSET_FIELD) {
			zap.L().Error("Invalid offset field. Using default value.",
				zap.String("topic", *toppars[i].Topic), zap.Int32("partition", toppars[i].Partition), zap.Binary("field", vg.Fields[0]))
			toppars[i].Offset = DEFAULT_OFFSET
		} else {
			off, err := decodeValue(vg.Values[0])
			if err != nil {
				return nil, fmt.Errorf("failed to decode field: %w", err)
			}
			zap.L().Info("Restored offset for partition",
				zap.String("topic", *toppars[i].Topic), zap.Int32("partition", toppars[i].Partition),
				zap.Int64("offset", off))
			err = toppars[i].Offset.Set(off)
			if err != nil {
				return nil, fmt.Errorf("failed to set offset: %w", err)
			}
		}
	}
	return toppars, nil
}

func encodeOffsets(toppars []kafka.TopicPartition) ([]hangar.Key, []hangar.ValGroup, error) {
	keys := make([]hangar.Key, len(toppars))
	vgs := make([]hangar.ValGroup, len(toppars))
	for i, toppar := range toppars {
		topic := *toppar.Topic
		partition := toppar.Partition
		zap.L().Debug("Encoding offset for partition",
			zap.String("topic", *toppars[i].Topic), zap.Int32("partition", toppars[i].Partition),
			zap.Int64("offset", int64(toppars[i].Offset)))
		key, err := encodeKey(toppar)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to encode binlog offset key: %w", err)
		}
		keys[i].Data = key
		v, err := encodeValue(int64(toppar.Offset))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to encode offset for {topic: \"%s\", partition: %d}: %v", topic, partition, err)
		}
		vgs[i].Fields = append(vgs[i].Fields, OFFSET_FIELD)
		vgs[i].Values = append(vgs[i].Values, v)
	}
	return keys, vgs, nil
}

func encodeKey(toppar kafka.TopicPartition) ([]byte, error) {
	buf := make([]byte, 10+len(OFFSET_KEY_PREFIX)+10+len(*toppar.Topic)+10)
	curr := 0
	n, err := binary.PutString(buf[curr:], OFFSET_KEY_PREFIX)
	if err != nil {
		return nil, err
	}
	curr += n
	n, err = binary.PutString(buf[curr:], *toppar.Topic)
	if err != nil {
		return nil, err
	}
	curr += n
	n, err = binary.PutVarint(buf[curr:], int64(toppar.Partition))
	if err != nil {
		return nil, err
	}
	curr += n
	return buf[:curr], nil
}

func encodeValue(offset int64) ([]byte, error) {
	buf := make([]byte, 10)
	curr := 0
	n, err := binary.PutVarint(buf[curr:], offset)
	if err != nil {
		return nil, err
	}
	curr += n
	return buf[:curr], nil
}

func decodeValue(buf []byte) (int64, error) {
	offset, n, err := binary.ReadVarint(buf)
	if err != nil {
		return 0, err
	}
	if n != len(buf) {
		return 0, fmt.Errorf("invalid field length: %d != %d", n, len(buf))
	}
	return offset, nil
}
