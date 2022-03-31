package feature

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/kafka"
	"fennel/lib/feature"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/test"
)

func TestLogMulti_Kafka(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	rows := make([]feature.Row, 0)
	for i := 0; i < 10; i++ {
		rows = append(rows, feature.Row{
			ContextOType:    "user",
			ContextOid:      ftypes.OidType(i),
			CandidateOType:  "video",
			CandidateOid:    ftypes.OidType(i + 1),
			Features:        value.NewDict(map[string]value.Value{"x": value.Int(i)}),
			Workflow:        "something",
			RequestID:       12,
			Timestamp:       12312,
			ModelName:       "some model",
			ModelVersion:    "0.1.0",
			ModelPrediction: 0.1323,
		})
	}
	err = LogMulti(ctx, tier, rows)
	assert.NoError(t, err)
	consumer, err := tier.NewKafkaConsumer(feature.KAFKA_TOPIC_NAME, "somegroup", "earliest")
	for i := 0; i < 10; i++ {
		var pmsg feature.ProtoRow
		err = consumer.ReadProto(ctx, &pmsg, -1)
		assert.NoError(t, err)
		msg, err := feature.FromProtoRow(&pmsg)
		assert.NoError(t, err)
		assert.Equal(t, rows[i], *msg)
	}
}

func TestLog_Read(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	row := feature.Row{ContextOType: "user",
		ContextOid:      1,
		CandidateOType:  "video",
		CandidateOid:    2,
		Features:        value.NewDict(map[string]value.Value{"x": value.Int(3)}),
		Workflow:        "something",
		RequestID:       12,
		Timestamp:       12312,
		ModelName:       "some model",
		ModelVersion:    "0.1.0",
		ModelPrediction: 0.1323,
	}

	err = Log(ctx, tier, row)
	assert.NoError(t, err)
	consumer, err := tier.NewKafkaConsumer(feature.KAFKA_TOPIC_NAME, "testgroup", kafka.DefaultOffsetPolicy)
	assert.NoError(t, err)
	defer consumer.Close()
	rowptr, err := Read(ctx, tier, consumer)
	assert.NoError(t, err)
	assert.Equal(t, row, *rowptr)
}
