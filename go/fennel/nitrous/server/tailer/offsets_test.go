package tailer

import (
	"context"
	"fennel/hangar/test"
	"fennel/lib/ftypes"
	"fennel/lib/utils/ptr"
	"math/rand"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

func TestOffsetSaveRestore(t *testing.T) {
	planeId := ftypes.RealmID(rand.Uint32())
	db := test.NewInMemoryHangar(planeId)
	toppars := []kafka.TopicPartition{
		{
			Topic:     ptr.To("topicA"),
			Partition: int32(0),
			Offset:    kafka.Offset(38412838),
		},
		{
			Topic:     ptr.To("topicB"),
			Partition: int32(1),
			Offset:    kafka.Offset(295),
		},
	}
	keys, vgs, err := encodeOffsets(toppars)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(keys))
	assert.Equal(t, 2, len(vgs))
	err = db.SetMany(context.Background(), keys, vgs)
	assert.NoError(t, err)

	topparsNoOffset := []kafka.TopicPartition{
		{
			Topic:     ptr.To("topicA"),
			Partition: int32(0),
		},
		{
			Topic:     ptr.To("topicB"),
			Partition: int32(1),
		},
	}
	got, err := decodeOffsets(topparsNoOffset, db)
	assert.NoError(t, err)
	assert.ElementsMatch(t, toppars, got)
}

func TestRestoreEmpty(t *testing.T) {
	planeId := ftypes.RealmID(rand.Uint32())
	db := test.NewInMemoryHangar(planeId)
	got, err := decodeOffsets(kafka.TopicPartitions{}, db)
	assert.NoError(t, err)
	assert.Empty(t, got)
}
