package offsets

import (
	"testing"

	"fennel/hangar/db"
	"fennel/hangar/encoders"
	"fennel/lib/ftypes"

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
	ks, vgs, err := SaveBinlogOffsets(toppars, []byte("offsetkg"))
	assert.NoError(t, err)

	planeId := ftypes.RealmID(5)
	db, err := db.NewHangar(planeId, t.TempDir(), 1<<10, encoders.Default())
	assert.NoError(t, err)
	err = db.SetMany(ks, vgs)
	assert.NoError(t, err)

	got, err := RestoreBinlogOffset(db, []byte("offsetkg"))
	assert.NoError(t, err)
	assert.ElementsMatch(t, toppars, got)
}

func TestRestoreEmpty(t *testing.T) {
	planeId := ftypes.RealmID(5)
	db, err := db.NewHangar(planeId, t.TempDir(), 1<<10, encoders.Default())
	assert.NoError(t, err)

	got, err := RestoreBinlogOffset(db, []byte("offsetkg"))
	assert.NoError(t, err)
	assert.Empty(t, got)
}
