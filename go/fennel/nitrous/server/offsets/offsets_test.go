package offsets

import (
	"testing"

	"fennel/plane"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

func String(v string) *string {
	return &v
}

func TestOffsetSaveRestore(t *testing.T) {
	tp := plane.NewTestPlane(t)
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
	ks, vgs, err := SaveBinlogOffsets(toppars, []byte("offsetkey"))
	assert.NoError(t, err)
	err = tp.Store.SetMany(ks, vgs)
	assert.NoError(t, err)

	got, err := RestoreBinlogOffset(tp.Store, []byte("offsetkey"))
	assert.NoError(t, err)
	assert.ElementsMatch(t, toppars, got)
}

func TestRestoreEmpty(t *testing.T) {
	tp := plane.NewTestPlane(t)
	got, err := RestoreBinlogOffset(tp.Store, []byte("offsetkey"))
	assert.NoError(t, err)
	assert.Empty(t, got)
}
