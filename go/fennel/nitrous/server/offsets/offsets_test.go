package offsets

import (
	"fennel/hangar"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

func String(v string) *string {
	return &v
}

func TestOffsetSaveRestore(t *testing.T) {
	toppars := []kafka.TopicPartition{
		{
			Topic:     String("topicA"),
			Partition: int32(0),
			Offset:    kafka.Offset(1004),
		},
		{
			Topic:     String("topicB"),
			Partition: int32(1),
			Offset:    kafka.Offset(295),
		},
	}
	vg, err := EncodeOffsets(toppars)
	assert.NoError(t, err)

	got, err := DecodeOffsets(vg)
	assert.NoError(t, err)
	assert.ElementsMatch(t, toppars, got)
}

func TestRestoreEmpty(t *testing.T) {
	got, err := DecodeOffsets(hangar.ValGroup{})
	assert.NoError(t, err)
	assert.Empty(t, got)
}
